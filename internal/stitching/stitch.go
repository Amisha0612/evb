package stitching

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/Amisha0612/evb/internal/models"
	"github.com/Amisha0612/evb/pkg/config"
	"github.com/Amisha0612/evb/pkg/logger"
)

type Controller interface {
	Aggregate(context.Context, *models.EventWrapper) *models.StitchedEvent
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

	return s
}

// evictExpired drops transactions that have not seen an event within the TTL.
// The caller must hold s.buffer.mu.
func (s *stitch) evictExpired(now time.Time) {
	for txID, last := range s.buffer.touched {
		if now.Sub(last) > s.ttl {
			delete(s.buffer.data, txID)
			delete(s.buffer.touched, txID)
		}
	}
}

// nolint: gocognit,gocritic // logic can't be altered as simplifying it would reduce readability and correctness
func (s *stitch) Aggregate(_ context.Context, event *models.EventWrapper) *models.StitchedEvent {
	s.buffer.mu.Lock()
	defer s.buffer.mu.Unlock()

	now := time.Now()
	s.evictExpired(now)
	s.buffer.touched[event.TxID] = now

	payload, exists := s.buffer.data[event.TxID]

	// If event already stitched and this table is already present, skip
	if exists {
		if payload.IsStitched {
			if _, found := payload.Tables[event.Table]; found {
				logger.Infof("Event already processed for TxID: %.0f, Table: %s", event.TxID, event.Table)
				return nil
			}
		}
	}

	// Initialize if not exists
	if !exists {
		s.buffer.data[event.TxID] = &models.StitchedEvent{
			TxID:       event.TxID,
			IsStitched: false,
			Server:     event.Server,
			Tables: map[string]models.TableSnapshot{
				event.Table: {
					Before:  mapToEventPtr(event.Before),
					After:   mapToEventPtr(event.After),
					Payload: capturePayload(event),
				},
			},
		}
	} else {
		payload.Tables[event.Table] = models.TableSnapshot{
			Before:  mapToEventPtr(event.Before),
			After:   mapToEventPtr(event.After),
			Payload: capturePayload(event),
		}
	}

	// Check for stitching rule completion
	tablesForTxn := make([]string, 0, len(s.buffer.data[event.TxID].Tables))
	for table := range s.buffer.data[event.TxID].Tables {
		tablesForTxn = append(tablesForTxn, table)
	}

	stitched := s.matchesSequencePresent(tablesForTxn, event.Op)
	s.buffer.data[event.TxID].IsStitched = stitched

	if stitched {
		return s.buffer.data[event.TxID]
	}

	return nil
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
