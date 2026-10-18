package internal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/Amisha0612/evb/internal/models"
	"github.com/Amisha0612/evb/internal/relay"
)

// recorder remembers which Kafka messages were acknowledged.
type recorder struct {
	mu    sync.Mutex
	acked []int64
}

func (r *recorder) Ack(m *sarama.ConsumerMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.acked = append(r.acked, m.Offset)
}

func (r *recorder) offsets() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]int64(nil), r.acked...)
}

func (r *recorder) waitFor(t *testing.T, n int) []int64 {
	t.Helper()

	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if got := r.offsets(); len(got) >= n {
			return got
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("timed out waiting for %d acks, got %v", n, r.offsets())

	return nil
}

func msg(offset int64) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Topic: "t", Offset: offset}
}

// fakeStitcher returns scripted results, one per Aggregate call.
type fakeStitcher struct {
	mu      sync.Mutex
	results []stitchResult
	sweep   []*sarama.ConsumerMessage
}

type stitchResult struct {
	event *models.StitchedEvent
	held  []*sarama.ConsumerMessage // released for immediate acknowledgement
}

// stitched builds a stitched event that carries the given Kafka messages.
func stitched(txID float64, offsets ...int64) *models.StitchedEvent {
	ev := &models.StitchedEvent{TxID: txID}
	for _, o := range offsets {
		ev.Messages = append(ev.Messages, msg(o))
	}

	return ev
}

func (f *fakeStitcher) Aggregate(ctx context.Context, e *models.EventWrapper) *models.StitchedEvent {
	ev, _ := f.AggregateHolding(ctx, e)

	return ev
}

func (f *fakeStitcher) AggregateHolding(context.Context, *models.EventWrapper) (*models.StitchedEvent, []*sarama.ConsumerMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()

	r := f.results[0]
	f.results = f.results[1:]

	return r.event, r.held
}

func (f *fakeStitcher) Sweep() []*sarama.ConsumerMessage {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := f.sweep
	f.sweep = nil

	return out
}

type fakeValidator struct {
	fail map[int64]bool
}

func (f *fakeValidator) Validator(_ context.Context, m *sarama.ConsumerMessage) (*models.EventWrapper, error) {
	if f.fail[m.Offset] {
		return nil, errors.New("invalid")
	}

	return &models.EventWrapper{TxID: float64(m.Offset), KafkaMsg: m}, nil
}

// queueWithCap is a real-ish bounded queue so backpressure can be observed.
type queueWithCap struct {
	events chan *models.StitchedEvent
}

func newQueueWithCap(n int) *queueWithCap {
	return &queueWithCap{events: make(chan *models.StitchedEvent, n)}
}

func (q *queueWithCap) Enqueue(e *models.StitchedEvent) { q.events <- e }

func (q *queueWithCap) EnqueueContext(ctx context.Context, e *models.StitchedEvent) error {
	select {
	case q.events <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *queueWithCap) Dequeue() *models.StitchedEvent {
	select {
	case e := <-q.events:
		return e
	default:
		return nil
	}
}

func (q *queueWithCap) IsEmpty() bool  { return len(q.events) == 0 }
func (q *queueWithCap) IsFull() bool   { return len(q.events) == cap(q.events) }
func (q *queueWithCap) QueueSize() int { return len(q.events) }

func newAckBridge(acks *recorder, st *fakeStitcher, v *fakeValidator, q *queueWithCap, rl relay.Controller) *bridge {
	return &bridge{validate: v, stitch: st, pipe: q, relay: rl, acker: acks}
}

func TestInvalidMessageIsAcknowledgedImmediately(t *testing.T) {
	acks := &recorder{}
	b := newAckBridge(acks, &fakeStitcher{}, &fakeValidator{fail: map[int64]bool{4: true}}, newQueueWithCap(4), &fakeRelay{relayed: make(chan *models.StitchedEvent, 4)})

	b.Process(context.Background(), msg(4))

	if got := acks.offsets(); len(got) != 1 || got[0] != 4 {
		t.Errorf("a message that can never be valid must not be re-read forever, acked %v", got)
	}
}

func TestBufferedMessageIsNotAcknowledgedUntilItsEventIsRelayed(t *testing.T) {
	acks := &recorder{}
	relayed := make(chan *models.StitchedEvent, 4)

	st := &fakeStitcher{results: []stitchResult{
		{event: nil, held: nil},               // first table: buffered, nothing to ack
		{event: stitched(1, 1, 2), held: nil}, // second table completes the transaction
	}}

	b := newAckBridge(acks, st, &fakeValidator{}, newQueueWithCap(4), &fakeRelay{relayed: relayed})

	b.Process(context.Background(), msg(1))

	if got := acks.offsets(); len(got) != 0 {
		t.Fatalf("message 1 is only buffered in memory; acking it now would lose it on a crash, acked %v", got)
	}

	b.Process(context.Background(), msg(2))

	if got := acks.offsets(); len(got) != 0 {
		t.Fatalf("the stitched event is queued but not relayed yet, acked %v", got)
	}

	startWorker(t, b)

	got := acks.waitFor(t, 2)
	if got[0] != 1 || got[1] != 2 {
		t.Errorf("both messages of the transaction are acked once it is relayed, got %v", got)
	}
}

func TestFailedDeliveryThatWasDeadLetteredIsStillAcknowledged(t *testing.T) {
	acks := &recorder{}
	st := &fakeStitcher{results: []stitchResult{{event: stitched(1, 9)}}}

	rl := &fakeRelay{relayed: make(chan *models.StitchedEvent, 4), errs: []error{errors.New("sink down, dead-lettered")}}
	b := newAckBridge(acks, st, &fakeValidator{}, newQueueWithCap(4), rl)

	b.Process(context.Background(), msg(9))
	startWorker(t, b)

	if got := acks.waitFor(t, 1); got[0] != 9 {
		t.Errorf("a recorded failure is finished business and must be acked, got %v", got)
	}
}

func TestUnconfirmedDeliveryIsNotAcknowledged(t *testing.T) {
	acks := &recorder{}
	st := &fakeStitcher{results: []stitchResult{{event: stitched(1, 9)}}}

	rl := &fakeRelay{relayed: make(chan *models.StitchedEvent, 4), errs: []error{relay.ErrUnconfirmed}}
	b := newAckBridge(acks, st, &fakeValidator{}, newQueueWithCap(4), rl)

	b.Process(context.Background(), msg(9))
	startWorker(t, b)

	select {
	case <-rl.relayed:
	case <-time.After(waitTimeout):
		t.Fatal("event was never relayed")
	}

	time.Sleep(50 * time.Millisecond)

	if got := acks.offsets(); len(got) != 0 {
		t.Errorf("an unconfirmed delivery must be re-read after restart, so it must not be acked: %v", got)
	}
}

func TestDuplicateAndExpiredMessagesAreAcknowledgedAtOnce(t *testing.T) {
	acks := &recorder{}
	st := &fakeStitcher{results: []stitchResult{{event: nil, held: []*sarama.ConsumerMessage{msg(3), msg(1)}}}}

	b := newAckBridge(acks, st, &fakeValidator{}, newQueueWithCap(4), &fakeRelay{relayed: make(chan *models.StitchedEvent, 4)})

	b.Process(context.Background(), msg(3))

	got := acks.offsets()
	if len(got) != 2 {
		t.Errorf("released messages must be acked immediately, got %v", got)
	}
}

func TestSweepAcknowledgesExpiredMessagesWhenIdle(t *testing.T) {
	acks := &recorder{}
	st := &fakeStitcher{sweep: []*sarama.ConsumerMessage{msg(70), msg(71)}}

	b := newAckBridge(acks, st, &fakeValidator{}, newQueueWithCap(4), &fakeRelay{relayed: make(chan *models.StitchedEvent, 4)})

	startWorker(t, b)

	if got := acks.waitFor(t, 2); got[0] != 70 || got[1] != 71 {
		t.Errorf("the worker must sweep periodically so a quiet topic does not pin offsets, got %v", got)
	}
}

// When the sinks are slower than Kafka the bridge must slow consumption down instead
// of dropping events that were already read.
func TestFullQueueAppliesBackpressureInsteadOfDropping(t *testing.T) {
	acks := &recorder{}
	q := newQueueWithCap(1)

	results := make([]stitchResult, 3)
	for i := range results {
		results[i] = stitchResult{event: stitched(float64(i), int64(i))}
	}

	rl := &fakeRelay{relayed: make(chan *models.StitchedEvent, 8)}
	b := newAckBridge(acks, &fakeStitcher{results: results}, &fakeValidator{}, q, rl)

	finished := make(chan struct{})

	go func() {
		for i := 0; i < 3; i++ {
			b.Process(context.Background(), msg(int64(i)))
		}

		close(finished)
	}()

	select {
	case <-finished:
		t.Fatal("Process returned for all messages although the queue (capacity 1) was never drained")
	case <-time.After(100 * time.Millisecond):
	}

	startWorker(t, b)

	select {
	case <-finished:
	case <-time.After(waitTimeout):
		t.Fatal("Process stayed blocked after the worker started draining")
	}

	if got := acks.waitFor(t, 3); len(got) != 3 {
		t.Errorf("no event may be lost to a full queue, acked %v", got)
	}
}

func TestProcessGivesUpWhenContextIsCancelledWhileQueueIsFull(t *testing.T) {
	acks := &recorder{}
	q := newQueueWithCap(1)
	q.events <- &models.StitchedEvent{TxID: 99}

	st := &fakeStitcher{results: []stitchResult{{event: stitched(1, 1)}}}
	b := newAckBridge(acks, st, &fakeValidator{}, q, &fakeRelay{relayed: make(chan *models.StitchedEvent, 1)})

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)

	done := make(chan struct{})

	go func() {
		b.Process(ctx, msg(1))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("Process ignored the cancelled context")
	}

	if got := acks.offsets(); len(got) != 0 {
		t.Errorf("an event that never made it into the queue must not be acked: %v", got)
	}
}
