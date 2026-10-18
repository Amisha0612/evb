package relay

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"

	"github.com/Amisha0612/evb/internal/models"
)

func mockProducer(t *testing.T) *mocks.SyncProducer {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	return mocks.NewSyncProducer(t, cfg)
}

func TestKafkaTransportPublishesPayloadToEveryTopic(t *testing.T) {
	producer := mockProducer(t)

	var topics []string

	check := func(msg *sarama.ProducerMessage) error {
		topics = append(topics, msg.Topic)

		if msg.Value == nil {
			t.Error("message has no value, the payload was not published")
			return nil
		}

		value, err := msg.Value.Encode()
		if err != nil {
			return err
		}

		var got models.StitchedEvent
		if err := json.Unmarshal(value, &got); err != nil {
			t.Errorf("message value is not the JSON payload: %q (%v)", value, err)
			return nil
		}

		if got.TxID != 42 || got.Server != "postgres-server" {
			t.Errorf("unexpected message value: %s", value)
		}

		return nil
	}

	producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(check)
	producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(check)

	k := &kafka{producer: producer, Topics: []string{"a", "b"}}
	payload := &models.StitchedEvent{TxID: 42, Server: "postgres-server"}

	if _, err := k.Transport(context.Background(), payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := producer.Close(); err != nil {
		t.Fatalf("producer expectations not met: %v", err)
	}

	if strings.Join(topics, ",") != "a,b" {
		t.Errorf("published to topics %v, want [a b]", topics)
	}
}

func TestKafkaTransportKeepsGoingAfterATopicFails(t *testing.T) {
	producer := mockProducer(t)
	producer.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)
	producer.ExpectSendMessageAndSucceed()

	k := &kafka{producer: producer, Topics: []string{"a", "b"}}

	_, err := k.Transport(context.Background(), &models.StitchedEvent{TxID: 1})
	if err == nil || !strings.Contains(err.Error(), sarama.ErrOutOfBrokers.Error()) {
		t.Fatalf("expected the failed topic's error to be returned, got %v", err)
	}

	if err := producer.Close(); err != nil {
		t.Fatalf("second topic was not attempted: %v", err)
	}
}

func TestKafkaTransportRejectsUnserialisablePayload(t *testing.T) {
	producer := mockProducer(t)

	k := &kafka{producer: producer, Topics: []string{"a"}}

	if _, err := k.Transport(context.Background(), make(chan int)); err == nil {
		t.Fatal("expected an error for a payload that cannot be marshalled")
	}

	if err := producer.Close(); err != nil {
		t.Fatalf("nothing should have been sent: %v", err)
	}
}

func TestKafkaSaramaHeader(t *testing.T) {
	k := &kafka{HeaderList: []headers{{Key: "source", Value: "evb"}, {Key: "env", Value: "test"}}}

	got := k.saramaHeader()
	if len(got) != 2 || string(got[0].Key) != "source" || string(got[1].Value) != "test" {
		t.Errorf("unexpected headers: %+v", got)
	}
}

func TestNewKafkaProducerKeepsCredentials(t *testing.T) {
	// No hosts, so no connection is attempted; the constructor must still report
	// that the sink is disabled rather than return a half-built controller.
	got := newKafkaProducer(&kafka{Username: "u", Password: "secret", Backend: "other"})
	if got != nil {
		t.Fatalf("a sink without hosts or topics must be disabled, got %#v", got)
	}
}

func TestKafkaProducerConfigUsesPasswordNotBackend(t *testing.T) {
	k := &kafka{
		Username:       "u",
		Password:       "secret",
		Backend:        "not-the-password",
		MaxRetries:     4,
		RetryFrequency: 250,
	}

	cfg := k.producerConfig()

	if !cfg.Net.SASL.Enable || cfg.Net.SASL.User != "u" || cfg.Net.SASL.Password != "secret" {
		t.Errorf("SASL not configured from username/password: %+v", cfg.Net.SASL)
	}

	if cfg.Producer.Retry.Max != 4 || cfg.Producer.Retry.Backoff.Milliseconds() != 250 {
		t.Errorf("retry settings not applied: %+v", cfg.Producer.Retry)
	}

	if !cfg.Producer.Return.Successes {
		t.Error("SyncProducer requires Return.Successes")
	}
}
