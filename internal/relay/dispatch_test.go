package relay

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Amisha0612/evb/internal/models"
)

// fakeSink is a scriptable sink: errs[i] is returned by call i (nil means success).
type fakeSink struct {
	id    string
	mu    sync.Mutex
	errs  []error
	calls int
	got   []*models.StitchedEvent
}

func (f *fakeSink) GetID() string { return f.id }

func (f *fakeSink) Transport(_ context.Context, payload any) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var err error
	if f.calls < len(f.errs) {
		err = f.errs[f.calls]
	}

	f.calls++

	if ev, ok := payload.(*models.StitchedEvent); ok {
		f.got = append(f.got, ev)
	}

	return nil, err
}

func (f *fakeSink) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

func alwaysFail(n int, err error) []error {
	out := make([]error, n)
	for i := range out {
		out[i] = err
	}

	return out
}

func fastPolicy(attempts int) retryPolicy {
	return retryPolicy{MaxAttempts: attempts, Base: time.Microsecond, Max: time.Millisecond}
}

func newTestRelay(t *testing.T, policy retryPolicy) (r *relay, deadLetterPath string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "dead.jsonl")

	return &relay{
		httpSinks:  map[string][]httpController{},
		kafkaSinks: map[string]kafkaController{},
		blobSinks:  map[string][]blobController{},
		retry:      policy,
		dead:       newDeadLetter(path),
	}, path
}

func testEvent() *models.StitchedEvent {
	return &models.StitchedEvent{TxID: 9, Server: "postgres-server"}
}

// Limitation: a failing sink used to stop delivery to every sink after it.
func TestFailingSinkDoesNotBlockTheOthers(t *testing.T) {
	r, path := newTestRelay(t, fastPolicy(2))

	bad := &fakeSink{id: "https-bad", errs: alwaysFail(10, errors.New("connection refused"))}
	goodKafka := &fakeSink{id: "kafka"}
	goodBlob := &fakeSink{id: "blob"}

	r.httpSinks["postgres-server"] = []httpController{bad}
	r.kafkaSinks["postgres-server"] = goodKafka
	r.blobSinks["postgres-server"] = []blobController{goodBlob}

	err := r.Transport(context.Background(), testEvent())
	if err == nil {
		t.Fatal("the failure must still be reported")
	}

	if goodKafka.callCount() != 1 || goodBlob.callCount() != 1 {
		t.Errorf("healthy sinks must still receive the event: kafka=%d blob=%d", goodKafka.callCount(), goodBlob.callCount())
	}

	dead := readDeadLetters(t, path)
	if len(dead) != 1 || dead[0].SinkID != "https-bad" {
		t.Errorf("only the failing sink should be dead-lettered, got %+v", dead)
	}
}

func TestTransientFailureIsRetriedUntilItSucceeds(t *testing.T) {
	r, path := newTestRelay(t, fastPolicy(3))

	flaky := &fakeSink{id: "kafka", errs: []error{errors.New("leader not available"), errors.New("leader not available")}}
	r.kafkaSinks["postgres-server"] = flaky

	if err := r.Transport(context.Background(), testEvent()); err != nil {
		t.Fatalf("a sink that recovers within the retry budget must not fail the event: %v", err)
	}

	if flaky.callCount() != 3 {
		t.Errorf("expected 3 attempts, got %d", flaky.callCount())
	}

	// The dead-letter file is created lazily, so a successful retry leaves no file.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("nothing should be dead-lettered after a successful retry (stat err: %v)", err)
	}
}

func TestExhaustedRetriesGoToTheDeadLetterStore(t *testing.T) {
	r, path := newTestRelay(t, fastPolicy(3))

	down := &fakeSink{id: "kafka", errs: alwaysFail(10, errors.New("broker down"))}
	r.kafkaSinks["postgres-server"] = down

	err := r.Transport(context.Background(), testEvent())
	if err == nil || !strings.Contains(err.Error(), "broker down") {
		t.Fatalf("expected the sink error, got %v", err)
	}

	if down.callCount() != 3 {
		t.Errorf("expected exactly MaxAttempts=3 attempts, got %d", down.callCount())
	}

	dead := readDeadLetters(t, path)
	if len(dead) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(dead))
	}

	if dead[0].Attempts != 3 || dead[0].SinkType != "kafka" || dead[0].Event.TxID != 9 {
		t.Errorf("unexpected dead letter: %+v", dead[0])
	}
}

func TestPermanentErrorsAreNotRetried(t *testing.T) {
	r, path := newTestRelay(t, fastPolicy(5))

	rejected := &fakeSink{id: "https", errs: alwaysFail(10, permanent(errors.New("HTTP 400 Bad Request")))}
	r.httpSinks["postgres-server"] = []httpController{rejected}

	if err := r.Transport(context.Background(), testEvent()); err == nil {
		t.Fatal("expected an error")
	}

	if rejected.callCount() != 1 {
		t.Errorf("a permanent failure must not be retried, got %d attempts", rejected.callCount())
	}

	if dead := readDeadLetters(t, path); len(dead) != 1 || dead[0].Attempts != 1 {
		t.Errorf("expected one dead letter after one attempt, got %+v", dead)
	}
}

func TestAllSinkErrorsAreReported(t *testing.T) {
	r, _ := newTestRelay(t, fastPolicy(1))

	r.httpSinks["postgres-server"] = []httpController{&fakeSink{id: "h", errs: alwaysFail(1, errors.New("http down"))}}
	r.kafkaSinks["postgres-server"] = &fakeSink{id: "k", errs: alwaysFail(1, errors.New("kafka down"))}

	err := r.Transport(context.Background(), testEvent())
	if err == nil || !strings.Contains(err.Error(), "http down") || !strings.Contains(err.Error(), "kafka down") {
		t.Errorf("both failures must be visible, got %v", err)
	}
}

func TestRetryStopsWhenContextIsCancelled(t *testing.T) {
	r, _ := newTestRelay(t, retryPolicy{MaxAttempts: 50, Base: 50 * time.Millisecond, Max: time.Second})

	sink := &fakeSink{id: "kafka", errs: alwaysFail(100, errors.New("down"))}
	r.kafkaSinks["postgres-server"] = sink

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(40*time.Millisecond, cancel)

	done := make(chan error, 1)

	go func() { done <- r.Transport(ctx, testEvent()) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Transport kept retrying after the context was cancelled")
	}

	if sink.callCount() >= 50 {
		t.Errorf("retries were not interrupted: %d attempts", sink.callCount())
	}
}

func TestUnknownServerIsDeadLetteredNotSilentlyDropped(t *testing.T) {
	r, path := newTestRelay(t, fastPolicy(1))

	ev := &models.StitchedEvent{TxID: 1, Server: "unconfigured-server"}

	if err := r.Transport(context.Background(), ev); err == nil {
		t.Error("an event for a server with no sinks must be reported")
	}

	if dead := readDeadLetters(t, path); len(dead) != 1 || dead[0].SinkType != "none" {
		t.Errorf("expected one dead letter for the unrouted event, got %+v", dead)
	}
}

func TestServerWithOnlyDisabledSinksIsNotAnError(t *testing.T) {
	r, _ := newTestRelay(t, fastPolicy(1))

	// A configured server whose sinks were all skipped (nil controllers) is a
	// configuration choice, not a delivery failure.
	r.httpSinks["postgres-server"] = []httpController{}
	r.kafkaSinks["postgres-server"] = nil
	r.blobSinks["postgres-server"] = []blobController{nil}

	if err := r.Transport(context.Background(), testEvent()); err != nil {
		t.Errorf("disabled sinks must be skipped without error: %v", err)
	}
}

// An event whose failure could not be written to the dead-letter store is neither
// delivered nor recoverable, so the caller must not acknowledge its Kafka message.
func TestUnrecordedFailureIsReportedAsUnconfirmed(t *testing.T) {
	r, _ := newTestRelay(t, fastPolicy(1))
	r.dead = newDeadLetter(filepath.Join(t.TempDir(), "missing-dir", "dead.jsonl"))
	r.kafkaSinks["postgres-server"] = &fakeSink{id: "k", errs: alwaysFail(1, errors.New("broker down"))}

	err := r.Transport(context.Background(), testEvent())
	if !errors.Is(err, ErrUnconfirmed) {
		t.Errorf("a failure that was not dead-lettered must be ErrUnconfirmed, got %v", err)
	}
}

func TestRecordedFailureIsNotUnconfirmed(t *testing.T) {
	r, _ := newTestRelay(t, fastPolicy(1))
	r.kafkaSinks["postgres-server"] = &fakeSink{id: "k", errs: alwaysFail(1, errors.New("broker down"))}

	err := r.Transport(context.Background(), testEvent())
	if err == nil || errors.Is(err, ErrUnconfirmed) {
		t.Errorf("a dead-lettered failure is finished business, got %v", err)
	}
}

// DEAD_LETTER_PATH=off is an explicit choice to drop failed events.
func TestDisabledDeadLetterDropsButStillConfirms(t *testing.T) {
	r, _ := newTestRelay(t, fastPolicy(1))
	r.dead = newDeadLetter("")
	r.kafkaSinks["postgres-server"] = &fakeSink{id: "k", errs: alwaysFail(1, errors.New("broker down"))}

	err := r.Transport(context.Background(), testEvent())
	if err == nil || errors.Is(err, ErrUnconfirmed) {
		t.Errorf("with the dead-letter store switched off the failure is returned but confirmed, got %v", err)
	}
}

// Shutdown interrupts the retries. The event must be left for the next start rather
// than dead-lettered, and its message must not be acknowledged.
func TestInterruptedRetriesAreUnconfirmedAndNotDeadLettered(t *testing.T) {
	r, path := newTestRelay(t, retryPolicy{MaxAttempts: 50, Base: 50 * time.Millisecond, Max: time.Second})
	r.kafkaSinks["postgres-server"] = &fakeSink{id: "k", errs: alwaysFail(100, errors.New("down"))}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(40*time.Millisecond, cancel)

	err := r.Transport(ctx, testEvent())
	if !errors.Is(err, ErrUnconfirmed) {
		t.Errorf("an interrupted delivery must be ErrUnconfirmed, got %v", err)
	}

	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Errorf("an interrupted event must not be dead-lettered (stat err: %v)", statErr)
	}
}

func TestAlreadyCancelledContextIsUnconfirmed(t *testing.T) {
	r, _ := newTestRelay(t, fastPolicy(3))
	sink := &fakeSink{id: "k"}
	r.kafkaSinks["postgres-server"] = sink

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := r.Transport(ctx, testEvent()); !errors.Is(err, ErrUnconfirmed) {
		t.Errorf("got %v, want ErrUnconfirmed", err)
	}

	if sink.callCount() != 0 {
		t.Errorf("no delivery should be attempted after shutdown, got %d calls", sink.callCount())
	}
}
