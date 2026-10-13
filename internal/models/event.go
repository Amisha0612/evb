package models

import (
	"context"

	"github.com/IBM/sarama"
)

type EventWrapper struct {
	Server   string
	TxID     float64
	LSN      float64
	Op       string
	Table    string
	Before   map[string]any
	After    map[string]any
	KafkaMsg *sarama.ConsumerMessage
	Callback func(context.Context, EventWrapper) error
}

type Event map[string]any

type TableSnapshot struct {
	Before  *Event `json:"before,omitempty"`
	After   *Event `json:"after,omitempty"`
	Payload Event  `json:"payload"`
}

type StitchedEvent struct {
	TxID       float64                  `json:"txId"`
	Server     string                   `json:"server"`
	IsStitched bool                     `json:"isStitched"`
	Tables     map[string]TableSnapshot `json:"tables"`
}
