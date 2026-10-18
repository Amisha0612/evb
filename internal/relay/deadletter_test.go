package relay

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Amisha0612/evb/internal/models"
)

func readDeadLetters(t *testing.T, path string) []deadLetter {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening dead-letter file: %v", err)
	}
	defer func() { _ = f.Close() }()

	var out []deadLetter

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var dl deadLetter
		if err := json.Unmarshal(sc.Bytes(), &dl); err != nil {
			t.Fatalf("dead-letter line is not JSON: %q (%v)", sc.Text(), err)
		}

		out = append(out, dl)
	}

	return out
}

func TestDeadLetterWritesOneJSONLinePerFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dead.jsonl")
	dl := newDeadLetter(path)

	event := &models.StitchedEvent{TxID: 7, Server: "postgres-server"}

	if err := dl.Write("kafka", "sink-1", event, 3, errString("broker down")); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := dl.Write("https", "sink-2", event, 1, errString("HTTP 400")); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := readDeadLetters(t, path)
	if len(got) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(got))
	}

	first := got[0]
	if first.SinkType != "kafka" || first.SinkID != "sink-1" || first.Attempts != 3 ||
		first.Error != "broker down" || first.Server != "postgres-server" || first.FailedAt.IsZero() {
		t.Errorf("unexpected dead letter: %+v", first)
	}

	if first.Event == nil || first.Event.TxID != 7 {
		t.Errorf("the failed event must be stored so it can be replayed: %+v", first.Event)
	}
}

func TestDeadLetterAppendsAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dead.jsonl")
	event := &models.StitchedEvent{TxID: 1}

	if err := newDeadLetter(path).Write("blob", "a", event, 1, errString("x")); err != nil {
		t.Fatal(err)
	}

	if err := newDeadLetter(path).Write("blob", "b", event, 1, errString("y")); err != nil {
		t.Fatal(err)
	}

	if got := readDeadLetters(t, path); len(got) != 2 {
		t.Errorf("a restart must not truncate the file, got %d lines", len(got))
	}
}

func TestDeadLetterIsSafeForConcurrentWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dead.jsonl")
	dl := newDeadLetter(path)

	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := dl.Write("https", "s", &models.StitchedEvent{TxID: float64(i)}, 1, errString("boom")); err != nil {
				t.Errorf("write: %v", err)
			}
		}()
	}

	wg.Wait()

	if got := readDeadLetters(t, path); len(got) != 20 {
		t.Errorf("expected 20 intact lines, got %d", len(got))
	}
}

func TestDeadLetterWithoutPathIsDisabledNotFatal(t *testing.T) {
	dl := newDeadLetter("")

	if err := dl.Write("kafka", "s", &models.StitchedEvent{}, 1, errString("x")); err != nil {
		t.Errorf("a disabled dead-letter store must not fail the pipeline: %v", err)
	}
}

func TestDeadLetterReportsUnwritablePath(t *testing.T) {
	dl := newDeadLetter(filepath.Join(t.TempDir(), "missing-dir", "dead.jsonl"))

	if err := dl.Write("kafka", "s", &models.StitchedEvent{}, 1, errString("x")); err == nil {
		t.Error("expected an error when the file cannot be created")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	p := retryPolicy{MaxAttempts: 6, Base: 100 * time.Millisecond, Max: 1 * time.Second}

	previousCeiling := time.Duration(0)

	for attempt := 1; attempt <= 6; attempt++ {
		ceiling := p.ceiling(attempt)

		if ceiling > p.Max {
			t.Errorf("attempt %d: ceiling %s exceeds the cap %s", attempt, ceiling, p.Max)
		}

		if ceiling < previousCeiling {
			t.Errorf("attempt %d: ceiling %s shrank from %s", attempt, ceiling, previousCeiling)
		}

		previousCeiling = ceiling
	}

	if p.ceiling(1) != 100*time.Millisecond || p.ceiling(2) != 200*time.Millisecond || p.ceiling(3) != 400*time.Millisecond {
		t.Errorf("expected exponential growth 100ms,200ms,400ms, got %s,%s,%s", p.ceiling(1), p.ceiling(2), p.ceiling(3))
	}

	if p.ceiling(6) != p.Max {
		t.Errorf("attempt 6 should be capped at %s, got %s", p.Max, p.ceiling(6))
	}
}

func TestBackoffDelayStaysWithinCeiling(t *testing.T) {
	p := retryPolicy{MaxAttempts: 5, Base: 50 * time.Millisecond, Max: time.Second}

	for attempt := 1; attempt <= 5; attempt++ {
		for i := 0; i < 200; i++ {
			d := p.delay(attempt)
			if d < 0 || d > p.ceiling(attempt) {
				t.Fatalf("attempt %d: delay %s outside [0, %s]", attempt, d, p.ceiling(attempt))
			}
		}
	}
}

func TestBackoffDoesNotOverflowOnHugeAttemptCounts(t *testing.T) {
	p := retryPolicy{MaxAttempts: 100, Base: time.Second, Max: time.Minute}

	if got := p.ceiling(90); got != time.Minute {
		t.Errorf("ceiling(90) = %s, want the %s cap", got, time.Minute)
	}
}
