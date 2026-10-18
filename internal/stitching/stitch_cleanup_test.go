package stitching

import (
	"context"
	"testing"
	"time"

	"github.com/Amisha0612/evb/internal/models"
	"github.com/Amisha0612/evb/pkg/config"
)

func newTestStitch(ttl time.Duration) *stitch {
	s := New(context.Background(), &config.GoDotEnvProvider{}).(*stitch)
	s.ruleset = map[string]string{"c": "users,orders"}
	s.ttl = ttl

	return s
}

func createEvent(txID float64, table string) *models.EventWrapper {
	return &models.EventWrapper{
		TxID:   txID,
		Table:  table,
		Op:     "c",
		Server: "postgres-server",
		After:  map[string]any{"id": 1},
	}
}

func bufferedTransactions(s *stitch) int {
	s.buffer.mu.Lock()
	defer s.buffer.mu.Unlock()

	return len(s.buffer.data)
}

// A completed transaction is remembered for the TTL so a redelivered message is
// still treated as a duplicate, and is forgotten afterwards.
func TestStitchedTransactionIsRememberedThenExpires(t *testing.T) {
	s := newTestStitch(20 * time.Millisecond)
	ctx := context.Background()

	if s.Aggregate(ctx, createEvent(1, "users")) != nil {
		t.Fatal("transaction must not be stitched after the first table")
	}

	if s.Aggregate(ctx, createEvent(1, "orders")) == nil {
		t.Fatal("transaction should be stitched once both tables arrived")
	}

	if s.Aggregate(ctx, createEvent(1, "orders")) != nil {
		t.Error("a redelivered event inside the TTL must be suppressed as a duplicate")
	}

	time.Sleep(50 * time.Millisecond)

	s.Aggregate(ctx, createEvent(2, "users"))

	if got := bufferedTransactions(s); got != 1 {
		t.Errorf("completed transaction was not evicted after the TTL, %d entries buffered", got)
	}
}

// A transaction whose remaining tables never arrive must be dropped after the TTL.
func TestIncompleteTransactionsExpire(t *testing.T) {
	s := newTestStitch(10 * time.Millisecond)
	ctx := context.Background()

	s.Aggregate(ctx, createEvent(1, "users"))

	if got := bufferedTransactions(s); got != 1 {
		t.Fatalf("expected 1 buffered transaction, got %d", got)
	}

	time.Sleep(30 * time.Millisecond)

	// Any later event triggers cleanup of expired transactions.
	s.Aggregate(ctx, createEvent(2, "users"))

	if got := bufferedTransactions(s); got != 1 {
		t.Errorf("expired transaction was not evicted, %d entries buffered", got)
	}
}

func TestFreshTransactionsAreNotEvicted(t *testing.T) {
	s := newTestStitch(time.Minute)
	ctx := context.Background()

	s.Aggregate(ctx, createEvent(1, "users"))
	s.Aggregate(ctx, createEvent(2, "users"))

	if got := bufferedTransactions(s); got != 2 {
		t.Errorf("fresh transactions were evicted, %d entries buffered", got)
	}
}

func TestStitchTTLComesFromConfig(t *testing.T) {
	t.Setenv("STITCH_TTL", "90s")

	s := New(context.Background(), &config.GoDotEnvProvider{}).(*stitch)
	if s.ttl != 90*time.Second {
		t.Errorf("ttl = %s, want 90s", s.ttl)
	}
}

func TestInvalidStitchTTLFallsBackToDefault(t *testing.T) {
	for _, bad := range []string{"soon", "-5m", "0s"} {
		t.Setenv("STITCH_TTL", bad)

		s := New(context.Background(), &config.GoDotEnvProvider{}).(*stitch)
		if s.ttl != defaultTTL {
			t.Errorf("STITCH_TTL=%q: ttl = %s, want default %s", bad, s.ttl, defaultTTL)
		}
	}
}
