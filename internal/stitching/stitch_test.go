package stitching

import (
	"context"
	"testing"

	"github.com/d3ltmeshQ/evb/internal/models"
	"github.com/d3ltmeshQ/evb/pkg/config"
)

func TestNew(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GoDotEnvProvider{}

	s := New(ctx, cfg)
	if s == nil {
		t.Fatal("Expected stitch controller, got nil")
	}
}

func TestAggregateNewEvent(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GoDotEnvProvider{}

	s := New(ctx, cfg)

	event := &models.EventWrapper{
		TxID:   12345,
		Table:  "users",
		Op:     "c",
		Server: "postgres-server",
		Before: nil,
		After: map[string]any{
			"id":   1,
			"name": "John",
		},
	}

	result := s.Aggregate(ctx, event)

	// First event should not be stitched yet
	if result != nil {
		t.Error("Expected nil for first event (no ruleset match)")
	}
}

func TestAggregateMultipleTables(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GoDotEnvProvider{}

	s := New(ctx, cfg).(*stitch)

	// Set up a simple ruleset
	s.ruleset = map[string]string{
		"c": "users,orders",
	}

	txID := float64(12345)

	// First event - users table
	event1 := &models.EventWrapper{
		TxID:   txID,
		Table:  "users",
		Op:     "c",
		Server: "postgres-server",
		After: map[string]any{
			"id":   1,
			"name": "John",
		},
	}

	result1 := s.Aggregate(ctx, event1)
	if result1 != nil {
		t.Error("Expected nil - not all tables present yet")
	}

	// Second event - orders table
	event2 := &models.EventWrapper{
		TxID:   txID,
		Table:  "orders",
		Op:     "c",
		Server: "postgres-server",
		After: map[string]any{
			"id":      100,
			"user_id": 1,
		},
	}

	result2 := s.Aggregate(ctx, event2)
	if result2 == nil {
		t.Fatal("Expected stitched event after all required tables")
	}

	if !result2.IsStitched {
		t.Error("Expected IsStitched to be true")
	}

	if len(result2.Tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(result2.Tables))
	}

	if _, found := result2.Tables["users"]; !found {
		t.Error("Expected users table in stitched event")
	}

	if _, found := result2.Tables["orders"]; !found {
		t.Error("Expected orders table in stitched event")
	}
}

func TestAggregateDuplicateTable(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GoDotEnvProvider{}

	s := New(ctx, cfg).(*stitch)

	s.ruleset = map[string]string{
		"u": "users",
	}

	txID := float64(12345)

	// First event
	event1 := &models.EventWrapper{
		TxID:   txID,
		Table:  "users",
		Op:     "u",
		Server: "postgres-server",
		Before: map[string]any{"id": 1, "name": "John"},
		After:  map[string]any{"id": 1, "name": "Jane"},
	}

	result1 := s.Aggregate(ctx, event1)
	if result1 == nil {
		t.Fatal("Expected stitched event")
	}

	// Duplicate event with same table (should be skipped)
	event2 := &models.EventWrapper{
		TxID:   txID,
		Table:  "users",
		Op:     "u",
		Server: "postgres-server",
		Before: map[string]any{"id": 1, "name": "Jane"},
		After:  map[string]any{"id": 1, "name": "Jack"},
	}

	result2 := s.Aggregate(ctx, event2)
	if result2 != nil {
		t.Error("Expected nil for duplicate table in already stitched event")
	}
}

func TestCapturePayload(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		before   map[string]any
		after    map[string]any
		expected map[string]any
	}{
		{
			name:     "Create operation",
			op:       "c",
			before:   nil,
			after:    map[string]any{"id": 1},
			expected: map[string]any{"id": 1},
		},
		{
			name:     "Update operation",
			op:       "u",
			before:   map[string]any{"id": 1, "name": "old"},
			after:    map[string]any{"id": 1, "name": "new"},
			expected: map[string]any{"id": 1, "name": "new"},
		},
		{
			name:     "Delete operation",
			op:       "d",
			before:   map[string]any{"id": 1},
			after:    nil,
			expected: map[string]any{"id": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &models.EventWrapper{
				Op:     tt.op,
				Before: tt.before,
				After:  tt.after,
			}

			result := capturePayload(event)

			if tt.op == "c" || tt.op == "u" {
				if result["id"] != tt.expected["id"] {
					t.Errorf("Expected id %v, got %v", tt.expected["id"], result["id"])
				}
			} else if tt.op == "d" {
				if result["id"] != tt.expected["id"] {
					t.Errorf("Expected id %v, got %v", tt.expected["id"], result["id"])
				}
			}
		})
	}
}
