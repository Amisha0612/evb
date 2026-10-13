package stitching

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/d3ltmeshQ/evb/internal/models"
	"github.com/d3ltmeshQ/evb/pkg/config"
	"github.com/d3ltmeshQ/evb/pkg/logger"
)

type Controller interface {
	Aggregate(context.Context, *models.EventWrapper) *models.StitchedEvent
}

type stitch struct {
	ruleset map[string]string
	buffer  struct {
		mu   sync.Mutex
		data map[float64]*models.StitchedEvent
	}
}

func New(ctx context.Context, cfg *config.GoDotEnvProvider) Controller {
	rulesetStr := cfg.Get("RULESET_MAP")

	var rs map[string]string
	if err := json.Unmarshal([]byte(rulesetStr), &rs); err != nil {
		logger.Errorf("invalid RULESET_MAP: %v", err)

		rs = make(map[string]string)
	}

	return &stitch{
		ruleset: rs,
		buffer: struct {
			mu   sync.Mutex
			data map[float64]*models.StitchedEvent
		}{
			mu:   sync.Mutex{},
			data: make(map[float64]*models.StitchedEvent),
		},
	}
}

// nolint: gocognit,gocritic // logic can't be altered as simplifying it would reduce readability and correctness
func (s *stitch) Aggregate(ctx context.Context, event *models.EventWrapper) *models.StitchedEvent {
	s.buffer.mu.Lock()
	defer s.buffer.mu.Unlock()

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
