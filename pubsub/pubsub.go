// pubsub/pubsub.go
package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/d3ltmeshQ/evb/pkg/logger"
	"github.com/joho/godotenv"
)

// Client defines the Pub/Sub interface
// Subscribe returns the next message or an error
// Publish sends a message to the first configured topic
// Close shuts down the consumer group
type Client interface {
	Subscribe(ctx context.Context) (*sarama.ConsumerMessage, error)
	Publish(key, value []byte) error
	Close() error
	Ping() error
}

// KafkaClient implements Client using Sarama ConsumerGroup
// and a background goroutine feeding into msgChan
type KafkaClient struct {
	consumer sarama.ConsumerGroup
	topics   []string
	msgChan  chan *sarama.ConsumerMessage
	once     sync.Once
	closeMu  sync.Mutex
	closed   bool
	producer sarama.SyncProducer

	// stored to support Ping/HealthCheck-like behavior
	brokers          []string
	config           *sarama.Config
	accessibleBroker string
}

type brokersErr struct{}

func (b brokersErr) Error() string { return "no accessible brokers" }

// NewClient reads configuration from environment and constructs a KafkaClient
// Required env vars:
//
//	KAFKA_HOSTS - comma-separated list of broker addresses
//	KAFKA_TOPIC - comma-separated list of topics (first is used for Publish)
//	KAFKA_CONSUMER_GROUP_ID - consumer group ID
//
// Optional:
//
//	KAFKA_SASL_USER, KAFKA_SASL_PASS
//	KAFKA_MAX_RETRY (default 3)
//	KAFKA_RETRY_FREQUENCY (ms, default 500)
func NewClient() (Client, error) {
	// Parse brokers and topics
	_, b, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(b) // e.g. /path/to/yourapp

	envPath := filepath.Join(projectRoot, ".env")
	err := godotenv.Load(envPath)
	if err != nil {
		logger.Printf("error loading .env, %v", err)
	}

	hosts := os.Getenv("KAFKA_HOSTS")
	if hosts == "" {
		return nil, errors.New("KAFKA_HOSTS env required")
	}
	brokers := strings.Split(hosts, ",")

	topicsEnv := os.Getenv("KAFKA_TOPIC")
	if topicsEnv == "" {
		return nil, errors.New("KAFKA_TOPIC env required")
	}
	topics := strings.Split(topicsEnv, ",")

	groupID := os.Getenv("KAFKA_CONSUMER_GROUP_ID")
	if groupID == "" {
		return nil, errors.New("KAFKA_CONSUMER_GROUP_ID env required")
	}

	maxRetry := 3
	if v := os.Getenv("KAFKA_MAX_RETRY"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			maxRetry = i
		}
	}
	retryFreq := 500 * time.Millisecond
	if v := os.Getenv("KAFKA_RETRY_FREQUENCY"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			retryFreq = time.Duration(i) * time.Millisecond
		}
	}

	// Build Sarama config
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Producer settings
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = maxRetry
	config.Producer.Retry.Backoff = retryFreq

	// Optional SASL
	user := os.Getenv("KAFKA_SASL_USER")
	pass := os.Getenv("KAFKA_SASL_PASS")
	if user != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = user
		config.Net.SASL.Password = pass
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	// Create SyncProducer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	// Create ConsumerGroup
	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		producer.Close()
		return nil, err
	}

	kc := &KafkaClient{
		consumer: consumer,
		topics:   topics,
		msgChan:  make(chan *sarama.ConsumerMessage, 256),
		producer: producer,

		// store brokers/config for Ping/HealthCheck
		brokers: brokers,
		config:  config,
	}
	return kc, nil
}

// Subscribe returns the next message or an error, blocking until one is available
func (k *KafkaClient) Subscribe(ctx context.Context) (*sarama.ConsumerMessage, error) {
	k.once.Do(func() {
		go k.runConsumerLoop()
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-k.msgChan:
		if !ok {
			return nil, errors.New("message channel closed")
		}
		return msg, nil
	}
}

// runConsumerLoop continually consumes from Kafka and pushes into msgChan
func (k *KafkaClient) runConsumerLoop() {
	handler := consumerGroupHandler{msgChan: k.msgChan}
	for {
		if err := k.consumer.Consume(context.Background(), k.topics, handler); err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
	}
}

// Publish sends a message to the first configured topic
func (k *KafkaClient) Publish(key, value []byte) error {
	topic := k.topics[0]
	msg := &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)}
	_, _, err := k.producer.SendMessage(msg)
	return err
}

// Ping checks connectivity to the Kafka cluster by refreshing metadata for configured topics,
// updating the accessible broker, and returning an error when no accessible broker is present.
func (k *KafkaClient) Ping() error {
	// create a short-lived client to refresh metadata and inspect brokers
	client, err := sarama.NewClient(k.brokers, k.config)
	if err != nil {
		return err
	}
	defer client.Close()

	// Refresh metadata for topics
	if err := client.RefreshMetadata(k.topics...); err != nil {
		return err
	}

	lastFetchedBrokers := k.accessibleBroker

	// Determine an accessible broker from metadata (pick the first broker address if present)
	bs := client.Brokers()
	if len(bs) == 0 {
		logger.Printf("No Accessible broker Present")
		k.accessibleBroker = ""
		return brokersErr{}
	}

	// Use the first broker's address as the accessible broker (consistent with simple health-check)
	firstAddr := bs[0].Addr()
	k.accessibleBroker = firstAddr

	if k.accessibleBroker == "" {
		logger.Printf("No Accessible broker Present")
		return brokersErr{}
	}

	if lastFetchedBrokers != k.accessibleBroker {
		logger.Printf("Previous accessible kafka brokers were: %s Current accessible kafka brokers are: %s", lastFetchedBrokers, k.accessibleBroker)
	}

	return nil
}

func (k *KafkaClient) Bind(message []byte, target interface{}) error {
	return json.Unmarshal(message, target)
}

// Close shuts down consumer and producer
func (k *KafkaClient) Close() error {
	k.closeMu.Lock()
	defer k.closeMu.Unlock()
	if k.closed {
		return nil
	}
	k.closed = true
	close(k.msgChan)
	k.producer.Close()
	return k.consumer.Close()
}

// consumerGroupHandler forwards messages to msgChan
type consumerGroupHandler struct {
	msgChan chan<- *sarama.ConsumerMessage
}

func (h consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.msgChan <- msg
		sess.MarkMessage(msg, "")
	}
	return nil
}
