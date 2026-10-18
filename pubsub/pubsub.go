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
	"github.com/joho/godotenv"

	"github.com/Amisha0612/evb/pkg/logger"
)

// Client defines the Pub/Sub interface
// Subscribe returns the next message or an error
// Publish sends a message to the first configured topic
// Close shuts down the consumer group
//
// Ack tells the client that a message returned by Subscribe has been fully handled
// (relayed, or deliberately dropped). Only then is its offset eligible to be
// committed, so a message that was still in flight when the process stopped is read
// again on restart instead of being lost.
type Client interface {
	Subscribe(ctx context.Context) (*sarama.ConsumerMessage, error)
	Ack(msg *sarama.ConsumerMessage)
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
	producer sarama.SyncProducer

	closeMu sync.Mutex
	closed  bool
	done    chan struct{} // closed by Close; unblocks everything waiting on msgChan
	cancel  context.CancelFunc
	ctx     context.Context

	tracker *offsetTracker

	sessMu  sync.Mutex
	session sarama.ConsumerGroupSession // current consumer group session, nil between sessions

	// stored to support Ping/HealthCheck-like behavior
	brokers          []string
	config           *sarama.Config
	accessibleBroker string
}

func newKafkaClient(consumer sarama.ConsumerGroup, producer sarama.SyncProducer, topics, brokers []string, cfg *sarama.Config, buffer int) *KafkaClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &KafkaClient{
		consumer: consumer,
		producer: producer,
		topics:   topics,
		msgChan:  make(chan *sarama.ConsumerMessage, buffer),
		done:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
		tracker:  newOffsetTracker(),
		brokers:  brokers,
		config:   cfg,
	}
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
	// Offsets are marked by Ack, after a message was handled; the periodic auto-commit
	// then flushes whatever has been marked.
	config.Consumer.Offsets.AutoCommit.Enable = true

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
		_ = producer.Close()
		return nil, err
	}

	return newKafkaClient(consumer, producer, topics, brokers, config, 256), nil
}

// Subscribe returns the next message or an error, blocking until one is available
func (k *KafkaClient) Subscribe(ctx context.Context) (*sarama.ConsumerMessage, error) {
	k.once.Do(func() {
		go k.runConsumerLoop()
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-k.done:
		return nil, ErrClosed
	case msg := <-k.msgChan:
		return msg, nil
	}
}

// ErrClosed is returned by Subscribe once the client has been closed.
var ErrClosed = errors.New("pubsub client is closed")

// Ack marks the message's offset as handled. The committed position only moves
// past an offset once every earlier message of the same partition is handled too.
func (k *KafkaClient) Ack(msg *sarama.ConsumerMessage) {
	if msg == nil {
		return
	}

	next, advanced := k.tracker.Done(msg.Topic, msg.Partition, msg.Offset)
	if !advanced {
		return
	}

	k.sessMu.Lock()
	defer k.sessMu.Unlock()

	// After a rebalance the partition belongs to another consumer, which resumes from
	// the last committed offset; marking from a dead session would be meaningless.
	if k.session != nil {
		k.session.MarkOffset(msg.Topic, msg.Partition, next, "")
	}
}

// runConsumerLoop keeps joining the consumer group until the client is closed.
func (k *KafkaClient) runConsumerLoop() {
	handler := &consumerGroupHandler{client: k}

	for k.ctx.Err() == nil {
		if err := k.consumer.Consume(k.ctx, k.topics, handler); err != nil {
			if k.ctx.Err() != nil || errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}

			logger.Errorf("kafka consume error: %v", err)

			select {
			case <-k.ctx.Done():
				return
			case <-time.After(time.Second):
			}
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
	defer func() { _ = client.Close() }()

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

	// Signal first: msgChan is never closed, because a claim may still be sending on
	// it and a send on a closed channel panics. done and the context release them.
	close(k.done)
	k.cancel()

	producerErr := k.producer.Close()

	return errors.Join(producerErr, k.consumer.Close())
}

// consumerGroupHandler forwards messages to the client's channel. It does not mark an
// offset itself; the client does that when the message is acknowledged.
type consumerGroupHandler struct {
	client *KafkaClient
}

func (h *consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	h.client.sessMu.Lock()
	h.client.session = sess
	h.client.sessMu.Unlock()

	return nil
}

func (h *consumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	h.client.sessMu.Lock()
	h.client.session = nil
	h.client.sessMu.Unlock()

	// Offsets of messages still in flight belong to the old assignment.
	h.client.tracker.Reset()

	// Flush what was acknowledged so far before the partitions move on.
	sess.Commit()

	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-sess.Context().Done():
			return nil
		case <-h.client.done:
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			h.client.tracker.Deliver(msg.Topic, msg.Partition, msg.Offset)

			select {
			case h.client.msgChan <- msg:
			case <-sess.Context().Done():
				return nil
			case <-h.client.done:
				return nil
			}
		}
	}
}
