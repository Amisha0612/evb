package relay

import (
	"context"
	"errors"

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
		Password:       obj.Backend,
		MaxRetries:     obj.MaxRetries,
		RetryFrequency: obj.RetryFrequency,
		Backend:        obj.Backend,
	}

	// TODO: create the sarama SyncProducer here; k.producer is nil until then.

	return k
}

func (k *kafka) GetID() string {
	return k.id
}

func (k *kafka) Transport(_ context.Context, _ any) (any, error) {
	errs := []error{}

	for _, topic := range k.Topics {
		partition, offset, err := k.producer.SendMessage(&sarama.ProducerMessage{
			Topic:   topic,
			Key:     nil,
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
