package queue

import (
	"context"
	"testing"

	"github.com/Amisha0612/evb/internal/models"
	"github.com/Amisha0612/evb/pkg/config"
)

func TestNew(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GoDotEnvProvider{}

	q := New(ctx, cfg)
	if q == nil {
		t.Fatal("Expected queue controller, got nil")
	}
}

func TestEnqueueDequeue(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GoDotEnvProvider{}

	q := New(ctx, cfg)

	if !q.IsEmpty() {
		t.Error("Expected queue to be empty initially")
	}

	event := &models.StitchedEvent{
		TxID:       12345,
		Server:     "test-server",
		IsStitched: true,
		Tables:     make(map[string]models.TableSnapshot),
	}

	q.Enqueue(event)

	if q.IsEmpty() {
		t.Error("Expected queue to not be empty after enqueue")
	}

	if q.QueueSize() != 1 {
		t.Errorf("Expected queue size 1, got %d", q.QueueSize())
	}

	dequeued := q.Dequeue()
	if dequeued == nil {
		t.Fatal("Expected event from dequeue, got nil")
	}

	if dequeued.TxID != event.TxID {
		t.Errorf("Expected TxID %v, got %v", event.TxID, dequeued.TxID)
	}

	if !q.IsEmpty() {
		t.Error("Expected queue to be empty after dequeue")
	}
}

func TestQueueFull(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GoDotEnvProvider{}

	q := New(ctx, cfg)
	pipe := q.(*pipe)

	// Fill the queue
	for i := 0; i < cap(pipe.queue); i++ {
		event := &models.StitchedEvent{
			TxID:   float64(i),
			Server: "test",
		}
		q.Enqueue(event)
	}

	if !q.IsFull() {
		t.Error("Expected queue to be full")
	}

	if q.QueueSize() != cap(pipe.queue) {
		t.Errorf("Expected queue size %d, got %d", cap(pipe.queue), q.QueueSize())
	}
}

func TestDequeueEmpty(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GoDotEnvProvider{}

	q := New(ctx, cfg)

	dequeued := q.Dequeue()
	if dequeued != nil {
		t.Error("Expected nil from dequeue on empty queue")
	}
}

func TestQueueOrdering(t *testing.T) {
	ctx := context.Background()
	cfg := &config.GoDotEnvProvider{}

	q := New(ctx, cfg)

	// Enqueue events in order
	for i := 1; i <= 5; i++ {
		event := &models.StitchedEvent{
			TxID:   float64(i),
			Server: "test",
		}
		q.Enqueue(event)
	}

	// Verify FIFO ordering
	for i := 1; i <= 5; i++ {
		event := q.Dequeue()
		if event == nil {
			t.Fatalf("Expected event %d, got nil", i)
		}
		if event.TxID != float64(i) {
			t.Errorf("Expected TxID %d, got %v", i, event.TxID)
		}
	}
}
