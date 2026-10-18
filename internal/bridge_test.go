package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Amisha0612/evb/internal/models"
)

const waitTimeout = 2 * time.Second

type fakePipe struct {
	events chan *models.StitchedEvent
}

func newFakePipe() *fakePipe {
	return &fakePipe{events: make(chan *models.StitchedEvent, 16)}
}

func (f *fakePipe) Enqueue(e *models.StitchedEvent) { f.events <- e }

func (f *fakePipe) Dequeue() *models.StitchedEvent {
	select {
	case e := <-f.events:
		return e
	default:
		return nil
	}
}

func (f *fakePipe) IsEmpty() bool  { return len(f.events) == 0 }
func (f *fakePipe) IsFull() bool   { return len(f.events) == cap(f.events) }
func (f *fakePipe) QueueSize() int { return len(f.events) }

type fakeRelay struct {
	relayed chan *models.StitchedEvent
	errs    []error
	calls   int
}

func (f *fakeRelay) Transport(_ context.Context, e *models.StitchedEvent) error {
	var err error
	if f.calls < len(f.errs) {
		err = f.errs[f.calls]
	}

	f.calls++
	f.relayed <- e

	return err
}

func startWorker(t *testing.T, b *bridge) (cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})

	go func() {
		b.relayWorker(ctx)
		close(finished)
	}()

	t.Cleanup(cancel)

	return cancel, finished
}

func expectRelayed(t *testing.T, r *fakeRelay, want *models.StitchedEvent) {
	t.Helper()

	select {
	case got := <-r.relayed:
		if got != want {
			t.Fatalf("relayed %+v, want %+v", got, want)
		}
	case <-time.After(waitTimeout):
		t.Fatalf("timed out waiting for event %+v to be relayed", want)
	}
}

// The worker must keep running after the queue drains; events enqueued later
// still have to be relayed.
func TestRelayWorkerKeepsRunningAfterQueueDrains(t *testing.T) {
	pipe, relay := newFakePipe(), &fakeRelay{relayed: make(chan *models.StitchedEvent, 16)}
	b := &bridge{pipe: pipe, relay: relay}

	startWorker(t, b)

	// Let several ticks pass with an empty queue.
	time.Sleep(150 * time.Millisecond)

	event := &models.StitchedEvent{TxID: 1}
	pipe.Enqueue(event)

	expectRelayed(t, relay, event)
}

func TestRelayWorkerRelaysEverythingQueued(t *testing.T) {
	pipe, relay := newFakePipe(), &fakeRelay{relayed: make(chan *models.StitchedEvent, 16)}
	b := &bridge{pipe: pipe, relay: relay}

	first, second := &models.StitchedEvent{TxID: 1}, &models.StitchedEvent{TxID: 2}
	pipe.Enqueue(first)
	pipe.Enqueue(second)

	startWorker(t, b)

	expectRelayed(t, relay, first)
	expectRelayed(t, relay, second)
}

func TestRelayWorkerContinuesAfterRelayError(t *testing.T) {
	pipe := newFakePipe()
	relay := &fakeRelay{relayed: make(chan *models.StitchedEvent, 16), errs: []error{errors.New("sink down")}}
	b := &bridge{pipe: pipe, relay: relay}

	first, second := &models.StitchedEvent{TxID: 1}, &models.StitchedEvent{TxID: 2}
	pipe.Enqueue(first)
	pipe.Enqueue(second)

	startWorker(t, b)

	expectRelayed(t, relay, first)
	expectRelayed(t, relay, second)
}

func TestRelayWorkerStopsWhenContextIsCancelled(t *testing.T) {
	b := &bridge{pipe: newFakePipe(), relay: &fakeRelay{relayed: make(chan *models.StitchedEvent, 1)}}

	cancel, done := startWorker(t, b)

	cancel()

	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("relayWorker did not stop after the context was cancelled")
	}
}
