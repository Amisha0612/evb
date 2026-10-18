package relay

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"github.com/Amisha0612/evb/pkg/logger"
)

type kafka struct {
	id             string
	producer       sarama.SyncProducer
	Topics         []string  `yaml:"topics"`
	Hosts          []string  `yaml:"hosts"`
	Mode           string    `yaml:"mode"`
	HeaderList     []headers `yaml:"headers"` // raw headers
	Username       string    `yaml:"username"`
	Password       string    `yaml:"password"`
	MaxRetries     int       `yaml:"maxRetry"`
	RetryFrequency int       `yaml:"retryFrequency"`
	Backend        string    `yaml:"pubsubBackend"`
}

type headers struct {
	Key   string
	Value string
}

type kafkaController interface {
	GetID() string
	Transport(context.Context, any) (any, error)
}

func newKafkaProducer(obj *kafka) kafkaController {
	if obj == nil {
		return nil
	}

	k := &kafka{
		id:             uuid.New().String(),
		Topics:         obj.Topics,
		Hosts:          obj.Hosts,
		Mode:           obj.Mode,
		HeaderList:     obj.HeaderList,
		Username:       obj.Username,
		Password:       obj.Password,
		MaxRetries:     obj.MaxRetries,
		RetryFrequency: obj.RetryFrequency,
		Backend:        obj.Backend,
	}

	if len(k.Hosts) == 0 || len(k.Topics) == 0 {
		logger.Infof("kafka sink not configured (hosts and topics are required), skipping")

		return nil
	}

	producer, err := sarama.NewSyncProducer(k.Hosts, k.producerConfig())
	if err != nil {
		logger.Errorf("error creating kafka producer for hosts %v: %v", k.Hosts, err)

		return nil
	}

	k.producer = producer

	return k
}

func (k *kafka) producerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = k.MaxRetries
	cfg.Producer.Retry.Backoff = time.Duration(k.RetryFrequency) * time.Millisecond

	if k.Username != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = k.Username
		cfg.Net.SASL.Password = k.Password
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	return cfg
}

func (k *kafka) GetID() string {
	return k.id
}

func (k *kafka) Transport(_ context.Context, payload any) (any, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	errs := []error{}

	for _, topic := range k.Topics {
		partition, offset, err := k.producer.SendMessage(&sarama.ProducerMessage{
			Topic:   topic,
			Key:     nil,
			Value:   sarama.ByteEncoder(data),
			Headers: k.saramaHeader(),
		})

		if err != nil {
			logger.Errorf("error in publishing message for topic: [%v], partition is : [%v], offset : [%v]", topic, partition, offset)
			errs = append(errs, err)

			continue
		}

		logger.Infof("published message successfully for topic: [%v], partition is : [%v], offset : [%v]", topic, partition, offset)
	}

	return nil, errors.Join(errs...)
}

func (k *kafka) saramaHeader() []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0, len(k.HeaderList))

	for _, header := range k.HeaderList {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(header.Key),
			Value: []byte(header.Value),
		})
	}

	return headers
}
