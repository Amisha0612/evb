package logger

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

// syncWriter serialises writes so the test only exercises the logger's own state.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buf.Write(p)
}

func TestConcurrentLoggingIsRaceFree(t *testing.T) {
	w := &syncWriter{}
	l := NewMockLogger(w)

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				l.logf(InfoLevel, "message %d", j)
				l.Update("DEBUG")
			}
		}()
	}

	wg.Wait()

	if got := strings.Count(w.buf.String(), "message"); got != 8*50 {
		t.Errorf("expected %d log lines, got %d", 8*50, got)
	}
}

func TestStatsReportsRuntimeMetrics(t *testing.T) {
	stats := NewMockLogger(new(bytes.Buffer)).Stats()

	for _, key := range []string{"alloc", "totalAlloc", "sys", "numGC", "goRoutines", "host"} {
		if _, ok := stats[key]; !ok {
			t.Errorf("Stats() is missing %q: %v", key, stats)
		}
	}
}
