package queue

import (
	"context"
	"testing"
	"time"

	"github.com/Amisha0612/evb/internal/models"
	"github.com/Amisha0612/evb/pkg/config"
)

func newSizedQueue(t *testing.T, size string) Controller {
	t.Helper()
	t.Setenv("QUEUE_SIZE", size)

	return New(context.Background(), &config.GoDotEnvProvider{})
}

// A full queue must make the producer wait, not drop the event.
func TestEnqueueContextBlocksWhileFullThenSucceeds(t *testing.T) {
	q := newSizedQueue(t, "1")

	if err := q.EnqueueContext(context.Background(), &models.StitchedEvent{TxID: 1}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)

	go func() { done <- q.EnqueueContext(context.Background(), &models.StitchedEvent{TxID: 2}) }()

	select {
	case err := <-done:
		t.Fatalf("EnqueueContext returned (%v) while the queue was full", err)
	case <-time.After(50 * time.Millisecond):
	}

	if first := q.Dequeue(); first == nil || first.TxID != 1 {
		t.Fatalf("dequeued %+v, want tx 1", first)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error once there was room: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("EnqueueContext stayed blocked after room was made")
	}

	if second := q.Dequeue(); second == nil || second.TxID != 2 {
		t.Errorf("the waiting event was not enqueued: %+v", second)
	}
}

func TestEnqueueContextReturnsWhenCancelledWhileFull(t *testing.T) {
	q := newSizedQueue(t, "1")

	if err := q.EnqueueContext(context.Background(), &models.StitchedEvent{TxID: 1}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)

	done := make(chan error, 1)

	go func() { done <- q.EnqueueContext(ctx, &models.StitchedEvent{TxID: 2}) }()

	select {
	case err := <-done:
		if err == nil {
			t.Error("a cancelled enqueue must report the context error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("EnqueueContext ignored the cancelled context")
	}

	if got := q.QueueSize(); got != 1 {
		t.Errorf("the abandoned event must not be enqueued, size = %d", got)
	}
}

func TestEnqueueContextRefusesWhenAlreadyCancelled(t *testing.T) {
	q := newSizedQueue(t, "10")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := q.EnqueueContext(ctx, &models.StitchedEvent{TxID: 1}); err == nil {
		t.Error("expected an error for a cancelled context even though there is room")
	}

	if !q.IsEmpty() {
		t.Error("nothing should have been enqueued")
	}
}
