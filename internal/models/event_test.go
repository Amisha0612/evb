package models

import (
	"testing"
)

func TestStitchedEvent(t *testing.T) {
	event := &StitchedEvent{
		TxID:       12345,
		Server:     "postgres-server",
		IsStitched: true,
		Tables: map[string]TableSnapshot{
			"users": {
				Before: nil,
				After: &Event{
					"id":   1,
					"name": "John",
				},
				Payload: Event{
					"id":   1,
					"name": "John",
				},
			},
		},
	}

	if event.TxID != 12345 {
		t.Errorf("Expected TxID 12345, got %v", event.TxID)
	}

	if event.Server != "postgres-server" {
		t.Errorf("Expected server postgres-server, got %s", event.Server)
	}

	if !event.IsStitched {
		t.Error("Expected IsStitched to be true")
	}

	if len(event.Tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(event.Tables))
	}

	users, found := event.Tables["users"]
	if !found {
		t.Fatal("Expected users table to be present")
	}

	if users.Before != nil {
		t.Error("Expected Before to be nil")
	}

	if users.After == nil {
		t.Fatal("Expected After to be non-nil")
	}

	if (*users.After)["name"] != "John" {
		t.Errorf("Expected name John, got %v", (*users.After)["name"])
	}
}

func TestEventWrapper(t *testing.T) {
	wrapper := &EventWrapper{
		Server: "mysql-server",
		TxID:   67890,
		LSN:    12345,
		Op:     "u",
		Table:  "orders",
		Before: map[string]any{"status": "pending"},
		After:  map[string]any{"status": "completed"},
	}

	if wrapper.TxID != 67890 {
		t.Errorf("Expected TxID 67890, got %v", wrapper.TxID)
	}

	if wrapper.Op != "u" {
		t.Errorf("Expected Op u, got %s", wrapper.Op)
	}

	if wrapper.Table != "orders" {
		t.Errorf("Expected Table orders, got %s", wrapper.Table)
	}

	if wrapper.Before["status"] != "pending" {
		t.Errorf("Expected Before status pending, got %v", wrapper.Before["status"])
	}

	if wrapper.After["status"] != "completed" {
		t.Errorf("Expected After status completed, got %v", wrapper.After["status"])
	}
}
