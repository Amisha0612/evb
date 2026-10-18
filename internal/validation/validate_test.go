package validation

import (
	"context"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

const (
	createEvent = `{"payload":{"op":"c","ts_ms":1700000000000,"before":null,` +
		`"after":{"id":1,"name":"John"},` +
		`"source":{"txId":42,"lsn":7,"table":"users","server":"postgres-server"}}}`
	updateEvent = `{"payload":{"op":"u","ts_ms":1700000000000,` +
		`"before":{"id":1,"name":"John"},"after":{"id":1,"name":"Jane"},` +
		`"source":{"txId":42,"lsn":7,"table":"users","server":"postgres-server"}}}`
	deleteEvent = `{"payload":{"op":"d","ts_ms":1700000000000,` +
		`"before":{"id":1,"name":"John"},"after":null,` +
		`"source":{"txId":42,"lsn":7,"table":"users","server":"postgres-server"}}}`
)

func consumerMessage(value string) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Topic: "cdc-events", Value: []byte(value)}
}

func TestValidatorAcceptsValidEvents(t *testing.T) {
	testCases := []struct {
		name  string
		value string
		op    string
	}{
		{"create", createEvent, "c"},
		{"update", updateEvent, "u"},
		{"delete", deleteEvent, "d"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := consumerMessage(tc.value)

			event, err := New().Validator(context.Background(), msg)
			if err != nil {
				t.Fatalf("expected a valid event, got error: %v", err)
			}

			if event.Op != tc.op || event.TxID != 42 || event.LSN != 7 ||
				event.Table != "users" || event.Server != "postgres-server" {
				t.Errorf("event not populated from the message: %+v", event)
			}

			if event.KafkaMsg != msg {
				t.Error("expected the original Kafka message to be kept on the event")
			}
		})
	}
}

func TestValidatorCopiesRowData(t *testing.T) {
	event, err := New().Validator(context.Background(), consumerMessage(createEvent))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.After["name"] != "John" || event.After["id"] != float64(1) {
		t.Errorf("After not copied from the message: %v", event.After)
	}

	if event.Before != nil {
		t.Errorf("expected nil Before for a create, got %v", event.Before)
	}
}

func TestValidatorRejectsInvalidEvents(t *testing.T) {
	testCases := []struct {
		name    string
		value   string
		wantErr string
	}{
		{"not json", `not json`, "unable to bind"},
		{"empty body", ``, "unable to bind"},
		{"empty object", `{}`, "validation failed"},
		{
			"unknown operation",
			`{"payload":{"op":"x","ts_ms":1,"source":{"txId":1,"lsn":1,"table":"t","server":"s"}}}`,
			"oneof",
		},
		{
			"missing table",
			`{"payload":{"op":"c","ts_ms":1,"after":{"id":1},"source":{"txId":1,"lsn":1,"server":"s"}}}`,
			"Table",
		},
		{
			"zero timestamp",
			`{"payload":{"op":"c","ts_ms":0,"after":{"id":1},"source":{"txId":1,"lsn":1,"table":"t","server":"s"}}}`,
			"TsMs",
		},
		{
			"create without after",
			`{"payload":{"op":"c","ts_ms":1,"before":null,"after":null,` +
				`"source":{"txId":1,"lsn":1,"table":"t","server":"s"}}}`,
			"requiredforop",
		},
		{
			"delete without before",
			`{"payload":{"op":"d","ts_ms":1,"before":null,"after":null,` +
				`"source":{"txId":1,"lsn":1,"table":"t","server":"s"}}}`,
			"requiredforop",
		},
		{
			"update that changes nothing",
			`{"payload":{"op":"u","ts_ms":1,"before":{"id":1},"after":{"id":1},` +
				`"source":{"txId":1,"lsn":1,"table":"t","server":"s"}}}`,
			"noopupdate",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event, err := New().Validator(context.Background(), consumerMessage(tc.value))
			if err == nil {
				t.Fatalf("expected an error, got event %+v", event)
			}

			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("expected error containing %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}

func TestValidatorRejectsNilMessage(t *testing.T) {
	if _, err := New().Validator(context.Background(), nil); err == nil {
		t.Fatal("expected an error for a nil message")
	}
}
