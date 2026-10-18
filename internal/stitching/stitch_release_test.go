package stitching

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/Amisha0612/evb/internal/models"
)

func withMsg(e *models.EventWrapper, offset int64) *models.EventWrapper {
	e.KafkaMsg = &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: offset}

	return e
}

func offsets(msgs []*sarama.ConsumerMessage) []int64 {
	out := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Offset)
	}

	return out
}

// Messages whose data is only buffered in memory cannot be acknowledged yet, because
// a crash would lose them. They are handed back when the transaction is stitched.
func TestAggregateReleasesHeldMessagesWhenStitched(t *testing.T) {
	s := newTestStitch(time.Minute)
	ctx := context.Background()

	ev, held := s.AggregateHolding(ctx, withMsg(createEvent(1, "users"), 10))
	if ev != nil || len(held) != 0 {
		t.Fatalf("first table: stitched=%v released=%v, want nothing yet", ev, offsets(held))
	}

	ev, held = s.AggregateHolding(ctx, withMsg(createEvent(1, "orders"), 11))
	if ev == nil {
		t.Fatal("transaction should be stitched")
	}

	if got := offsets(ev.Messages); len(got) != 2 || got[0] != 10 || got[1] != 11 {
		t.Errorf("the stitched event must carry every message it consumed, got %v", got)
	}

	if len(held) != 0 {
		t.Errorf("those messages must wait for the relay, so nothing is released yet: %v", offsets(held))
	}
}

func TestDuplicateEventIsReleasedImmediately(t *testing.T) {
	s := newTestStitch(time.Minute)
	ctx := context.Background()

	s.AggregateHolding(ctx, withMsg(createEvent(1, "users"), 1))
	s.AggregateHolding(ctx, withMsg(createEvent(1, "orders"), 2))

	ev, held := s.AggregateHolding(ctx, withMsg(createEvent(1, "orders"), 3))
	if ev != nil {
		t.Error("a redelivered event must not be stitched again")
	}

	if got := offsets(held); len(got) != 1 || got[0] != 3 {
		t.Errorf("a duplicate has nothing buffered, so it must be released at once, got %v", got)
	}
}

// If the process dies while a transaction is incomplete, its messages are read again
// after restart. Messages of a transaction that EXPIRES are released instead: they
// were deliberately given up on and must not block the committed offset forever.
func TestExpiredTransactionsReleaseTheirMessages(t *testing.T) {
	s := newTestStitch(10 * time.Millisecond)
	ctx := context.Background()

	s.AggregateHolding(ctx, withMsg(createEvent(1, "users"), 5))

	time.Sleep(30 * time.Millisecond)

	_, held := s.AggregateHolding(ctx, withMsg(createEvent(2, "users"), 6))

	if got := offsets(held); len(got) != 1 || got[0] != 5 {
		t.Errorf("the expired transaction's message 5 must be released, got %v", got)
	}
}

func TestEventWithoutAKafkaMessageIsAccepted(t *testing.T) {
	s := newTestStitch(time.Minute)

	ev, held := s.AggregateHolding(context.Background(), createEvent(1, "users"))
	if ev != nil || len(held) != 0 {
		t.Errorf("unexpected result: %v %v", ev, offsets(held))
	}
}

func TestSweepReleasesMessagesWithoutNewEvents(t *testing.T) {
	s := newTestStitch(10 * time.Millisecond)

	s.AggregateHolding(context.Background(), withMsg(createEvent(1, "users"), 7))

	time.Sleep(30 * time.Millisecond)

	if got := offsets(s.Sweep()); len(got) != 1 || got[0] != 7 {
		t.Errorf("Sweep must release expired messages even when the topic is quiet, got %v", got)
	}

	if got := bufferedTransactions(s); got != 0 {
		t.Errorf("Sweep left %d transactions buffered", got)
	}

	if got := s.Sweep(); len(got) != 0 {
		t.Errorf("a second Sweep must find nothing, got %v", offsets(got))
	}
}

// Scanning every buffered transaction on every event would be quadratic under load,
// so the scan is rate limited; Sweep always runs it.
func TestEvictionIsRateLimitedButSweepForcesIt(t *testing.T) {
	s := newTestStitch(time.Second) // scan at most every 100ms
	ctx := context.Background()

	s.AggregateHolding(ctx, withMsg(createEvent(1, "users"), 1)) // first call scans and records the time

	s.buffer.mu.Lock()
	s.buffer.touched[1] = time.Now().Add(-time.Hour) // long expired
	s.buffer.mu.Unlock()

	_, held := s.AggregateHolding(ctx, withMsg(createEvent(2, "users"), 2))
	if len(held) != 0 || bufferedTransactions(s) != 2 {
		t.Fatalf("the second event came right after a scan, so no scan should run: released=%v buffered=%d",
			offsets(held), bufferedTransactions(s))
	}

	if got := offsets(s.Sweep()); len(got) != 1 || got[0] != 1 {
		t.Errorf("Sweep must scan regardless of the rate limit and release message 1, got %v", got)
	}
}
