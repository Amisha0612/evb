package validation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/IBM/sarama"
	goplv10 "github.com/go-playground/validator/v10"

	"github.com/Amisha0612/evb/internal/models"
)

/*
╔════════════════╦══════════╦════════════════════════════════════════════╗
║ Operation Code ║ Meaning  ║                Description                 ║
╠════════════════╬══════════╬════════════════════════════════════════════╣
║ c              ║ Create   ║ A new row was inserted                     ║
║ u              ║ Update   ║ An existing row was modified               ║
║ d              ║ Delete   ║ A row was deleted                          ║
║ r              ║ Read     ║ A row was read (applies only to snapshots) ║
║ t              ║ Truncate ║ A table was truncated                      ║
║ m              ║ Message  ║ A logical decoding message was emitted     ║
╚════════════════╩══════════╩════════════════════════════════════════════╝
*/

type bufferEvent struct {
	Op     string         `json:"op" validate:"required,oneof=c u d r"`
	TsMs   float64        `json:"ts_ms" validate:"required,gt=0"`
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Source metadata       `json:"source" validate:"required"`
}

type metadata struct {
	TxID   float64 `json:"txId" validate:"required,gt=0"`
	LSN    float64 `json:"lsn" validate:"required,gt=0"`
	Table  string  `json:"table" validate:"required"`
	Server string  `json:"server" validate:"required"`
}

type envelope struct {
	Payload bufferEvent `json:"payload" validate:"required"`
}

type Controller interface {
	Validator(context.Context, *sarama.ConsumerMessage) (*models.EventWrapper, error)
}

type validate struct {
	v10 *goplv10.Validate
}

func New() Controller {
	v10 := goplv10.New()
	v10.RegisterStructValidation(opConditionalValidation, envelope{})

	return &validate{v10: v10}
}

func (v *validate) Validator(_ context.Context, msg *sarama.ConsumerMessage) (*models.EventWrapper, error) {
	var (
		event       models.EventWrapper
		msgEnvelope envelope
	)

	// TODO: bind msg.Value into msgEnvelope; validation currently runs on an empty envelope.

	if err := v.v10.Struct(msgEnvelope); err != nil {
		return nil, errors.New("validation failed: " + err.Error())
	}

	event.TxID = msgEnvelope.Payload.Source.TxID
	event.LSN = msgEnvelope.Payload.Source.LSN
	event.Before = msgEnvelope.Payload.Before
	event.After = msgEnvelope.Payload.After
	event.Table = msgEnvelope.Payload.Source.Table
	event.Op = msgEnvelope.Payload.Op
	event.Server = msgEnvelope.Payload.Source.Server
	event.KafkaMsg = msg
	event.Callback = func(context.Context, models.EventWrapper) error { return nil }

	return &event, nil
}

func opConditionalValidation(sl goplv10.StructLevel) {
	kakfaMsg := sl.Current().Interface().(envelope)
	p := kakfaMsg.Payload

	switch p.Op {
	case "c", "r":
		if p.After == nil {
			sl.ReportError(p.After, "After", "after", "requiredforop", "c or r")
		}
	case "d":
		if p.Before == nil {
			sl.ReportError(p.Before, "Before", "before", "requiredforop", "d")
		}
	case "u":
		if p.Before == nil || p.After == nil {
			sl.ReportError(p.Before, "Before", "before", "requiredforop", "u")
			sl.ReportError(p.After, "After", "after", "requiredforop", "u")
		}

		if p.Before != nil && p.After != nil {
			beforeBytes, _ := json.Marshal(p.Before)
			afterBytes, _ := json.Marshal(p.After)

			if bytes.Equal(beforeBytes, afterBytes) {
				sl.ReportError(p.After, "After", "after", "noopupdate", "u")
			}
		}
	}
}
