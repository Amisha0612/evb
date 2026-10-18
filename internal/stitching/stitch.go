package stitching

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/Amisha0612/evb/internal/models"
	"github.com/Amisha0612/evb/pkg/config"
	"github.com/Amisha0612/evb/pkg/logger"
)

type Controller interface {
	// Aggregate adds the event to its transaction and returns the stitched event once
	// the transaction is complete.
	Aggregate(context.Context, *models.EventWrapper) *models.StitchedEvent

	// AggregateHolding is Aggregate for callers that track Kafka offsets. The returned
	// event carries, in its Messages, the Kafka messages it was built from; those must
	// be acknowledged only after it has been relayed. The returned slice holds messages
	// that are no longer needed (duplicates and expired transactions) and can be
	// acknowledged immediately.
	AggregateHolding(context.Context, *models.EventWrapper) (*models.StitchedEvent, []*sarama.ConsumerMessage)

	// Sweep drops expired transactions and returns their messages. Aggregate does the
	// same on every call; Sweep covers the case where no events arrive.
	Sweep() []*sarama.ConsumerMessage
}

// defaultTTL is how long a transaction is remembered after its last event. Incomplete
// transactions are dropped after this, and completed ones are kept this long so that
// redelivered Kafka messages are still recognised as duplicates.
const defaultTTL = 10 * time.Minute

type stitch struct {
	ruleset map[string]string
	ttl     time.Duration
	buffer  struct {
		mu      sync.Mutex
		data    map[float64]*models.StitchedEvent
		touched map[float64]time.Time
		held    map[float64][]*sarama.ConsumerMessage // messages whose data is only buffered here

		lastSweep time.Time
	}
}

func New(_ context.Context, cfg *config.GoDotEnvProvider) Controller {
	rulesetStr := cfg.Get("RULESET_MAP")

	var rs map[string]string
	if err := json.Unmarshal([]byte(rulesetStr), &rs); err != nil {
		logger.Errorf("invalid RULESET_MAP: %v", err)

		rs = make(map[string]string)
	}

	ttl := defaultTTL

	if v := cfg.Get("STITCH_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			ttl = d
		} else {
			logger.Errorf("invalid STITCH_TTL %q, using %s", v, defaultTTL)
		}
	}

	s := &stitch{ruleset: rs, ttl: ttl}
	s.buffer.data = make(map[float64]*models.StitchedEvent)
	s.buffer.touched = make(map[float64]time.Time)
	s.buffer.held = make(map[float64][]*sarama.ConsumerMessage)

	return s
}

// evictExpired drops transactions that have not seen an event within the TTL and
// returns the Kafka messages they were holding. Scanning every open transaction is
// linear in their number, so unless force is set it runs at most once per tenth of
// the TTL instead of on every event. The caller must hold s.buffer.mu.
func (s *stitch) evictExpired(now time.Time, force bool) []*sarama.ConsumerMessage {
	if !force && now.Sub(s.buffer.lastSweep) < s.ttl/10 {
		return nil
	}

	s.buffer.lastSweep = now

	var released []*sarama.ConsumerMessage

	for txID, last := range s.buffer.touched {
		if now.Sub(last) > s.ttl {
			released = append(released, s.buffer.held[txID]...)

			delete(s.buffer.data, txID)
			delete(s.buffer.touched, txID)
			delete(s.buffer.held, txID)
		}
	}

	return released
}

func (s *stitch) Sweep() []*sarama.ConsumerMessage {
	s.buffer.mu.Lock()
	defer s.buffer.mu.Unlock()

	return s.evictExpired(time.Now(), true)
}

func (s *stitch) Aggregate(ctx context.Context, event *models.EventWrapper) *models.StitchedEvent {
	stitched, _ := s.AggregateHolding(ctx, event)

	return stitched
}

func (s *stitch) AggregateHolding(_ context.Context, event *models.EventWrapper) (*models.StitchedEvent, []*sarama.ConsumerMessage) {
	s.buffer.mu.Lock()
	defer s.buffer.mu.Unlock()

	now := time.Now()
	released := s.evictExpired(now, false)
	s.buffer.touched[event.TxID] = now

	// A redelivered event for a table that is already part of a stitched transaction.
	if txn, ok := s.buffer.data[event.TxID]; ok && txn.IsStitched {
		if _, found := txn.Tables[event.Table]; found {
			logger.Infof("Event already processed for TxID: %.0f, Table: %s", event.TxID, event.Table)

			return nil, appendMessage(released, event.KafkaMsg)
		}
	}

	txn := s.record(event)
	if event.KafkaMsg != nil {
		s.buffer.held[event.TxID] = append(s.buffer.held[event.TxID], event.KafkaMsg)
	}

	txn.IsStitched = s.isComplete(txn, event.Op)
	if !txn.IsStitched {
		return nil, released
	}

	out := snapshot(txn)
	out.Messages = s.buffer.held[event.TxID]
	delete(s.buffer.held, event.TxID)

	return out, released
}

// record stores the event's table snapshot in its transaction, creating it if needed.
func (s *stitch) record(event *models.EventWrapper) *models.StitchedEvent {
	txn, ok := s.buffer.data[event.TxID]
	if !ok {
		txn = &models.StitchedEvent{
			TxID:   event.TxID,
			Server: event.Server,
			Tables: make(map[string]models.TableSnapshot),
		}
		s.buffer.data[event.TxID] = txn
	}

	txn.Tables[event.Table] = models.TableSnapshot{
		Before:  mapToEventPtr(event.Before),
		After:   mapToEventPtr(event.After),
		Payload: capturePayload(event),
	}

	return txn
}

func (s *stitch) isComplete(txn *models.StitchedEvent, op string) bool {
	tables := make([]string, 0, len(txn.Tables))
	for table := range txn.Tables {
		tables = append(tables, table)
	}

	return s.matchesSequencePresent(tables, op)
}

// snapshot copies a transaction so that the event handed downstream is not the map
// that later events keep modifying while the relay is still serialising it.
func snapshot(txn *models.StitchedEvent) *models.StitchedEvent {
	tables := make(map[string]models.TableSnapshot, len(txn.Tables))
	for table, snap := range txn.Tables {
		tables[table] = snap
	}

	return &models.StitchedEvent{
		TxID:       txn.TxID,
		Server:     txn.Server,
		IsStitched: txn.IsStitched,
		Tables:     tables,
	}
}

func appendMessage(msgs []*sarama.ConsumerMessage, msg *sarama.ConsumerMessage) []*sarama.ConsumerMessage {
	if msg == nil {
		return msgs
	}

	return append(msgs, msg)
}

func (s *stitch) matchesSequencePresent(tablesForTxn []string, op string) bool {
	raw, ok := s.ruleset[op]
	if !ok {
		return false
	}

	requiredTables := strings.Split(raw, ",")
	for i := range requiredTables {
		requiredTables[i] = strings.TrimSpace(requiredTables[i])
	}

	tableSet := make(map[string]struct{})
	for _, table := range tablesForTxn {
		tableSet[table] = struct{}{}
	}

	for _, required := range requiredTables {
		if _, found := tableSet[required]; !found {
			return false
		}
	}

	return true
}

func mapToEventPtr(m map[string]any) *models.Event {
	if m == nil {
		return nil
	}

	ev := models.Event(m)

	return &ev
}

func capturePayload(event *models.EventWrapper) models.Event {
	switch event.Op {
	case "c", "u":
		return event.After
	case "d":
		return event.Before
	default:
		return models.Event{"error": "unsupported operation"}
	}
}
