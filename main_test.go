package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/Amisha0612/evb/internal"
	"github.com/Amisha0612/evb/pkg/config"
	"github.com/Amisha0612/evb/pubsub"
)

type fakeClient struct {
	pingErr        error
	subscribeCalls atomic.Int64
	closeCalls     atomic.Int64
}

func (f *fakeClient) Subscribe(ctx context.Context) (*sarama.ConsumerMessage, error) {
	f.subscribeCalls.Add(1)

	<-ctx.Done()

	return nil, ctx.Err()
}

func (f *fakeClient) Publish(_, _ []byte) error { return nil }
func (f *fakeClient) Ping() error               { return f.pingErr }

func (f *fakeClient) Close() error {
	f.closeCalls.Add(1)

	return nil
}

// resetState isolates each test from the package-level subscription state and
// points the bridge at an empty sink config so it needs no files or brokers.
func resetState(t *testing.T) *config.GoDotEnvProvider {
	t.Helper()

	sinks := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(sinks, []byte("config:\n  servers: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("SINK_CONFIG_PATH", sinks)
	t.Setenv("RULESET_MAP", "{}")

	prevFactory := newClient

	t.Cleanup(func() {
		newClient = prevFactory

		subscribeMu.Lock()
		pubsubClient = nil
		bridge = internal.Controller(nil)
		subscribeMu.Unlock()
	})

	subscribeMu.Lock()
	pubsubClient = nil
	subscribeMu.Unlock()

	return &config.GoDotEnvProvider{}
}

func TestStartSubscriptionCreatesTheClientOnce(t *testing.T) {
	cfg := resetState(t)

	var created atomic.Int64

	newClient = func() (pubsub.Client, error) {
		created.Add(1)

		return &fakeClient{}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := startSubscription(ctx, cfg); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()

	if got := created.Load(); got != 1 {
		t.Errorf("Kafka client created %d times, want exactly 1", got)
	}
}

func TestStartSubscriptionRetriesAfterAFailedAttempt(t *testing.T) {
	cfg := resetState(t)

	attempts := 0
	newClient = func() (pubsub.Client, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("brokers unreachable")
		}

		return &fakeClient{}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := startSubscription(ctx, cfg); err == nil {
		t.Fatal("expected the first attempt to fail")
	}

	if currentClient() != nil {
		t.Fatal("a failed attempt must not leave a client behind")
	}

	if err := startSubscription(ctx, cfg); err != nil {
		t.Fatalf("second attempt should succeed: %v", err)
	}

	if currentClient() == nil {
		t.Error("client missing after a successful retry")
	}
}

func TestSubscriptionLoopStopsWhenContextIsCancelled(t *testing.T) {
	cfg := resetState(t)

	fake := &fakeClient{}
	newClient = func() (pubsub.Client, error) { return fake, nil }

	ctx, cancel := context.WithCancel(context.Background())

	if err := startSubscription(ctx, cfg); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for fake.subscribeCalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscription loop never called Subscribe")
		}

		time.Sleep(time.Millisecond)
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	first := fake.subscribeCalls.Load()

	time.Sleep(50 * time.Millisecond)

	if second := fake.subscribeCalls.Load(); second != first {
		t.Errorf("loop kept spinning after cancel: Subscribe calls went from %d to %d", first, second)
	}
}

func healthResponse(t *testing.T) (code int, body map[string]any) {
	t.Helper()

	rec := httptest.NewRecorder()
	healthHandler(rec, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))

	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("health response is not JSON: %q (%v)", rec.Body.String(), err)
	}

	return rec.Code, body
}

func TestHealthBeforeSubscription(t *testing.T) {
	resetState(t)

	code, body := healthResponse(t)

	if code != http.StatusOK || body["kafka"] != "not_initialized" || body["status"] != "healthy" {
		t.Errorf("unexpected health before subscribe: %d %v", code, body)
	}
}

func TestHealthReflectsKafkaConnectivity(t *testing.T) {
	cfg := resetState(t)

	fake := &fakeClient{}
	newClient = func() (pubsub.Client, error) { return fake, nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := startSubscription(ctx, cfg); err != nil {
		t.Fatal(err)
	}

	if code, body := healthResponse(t); code != http.StatusOK || body["kafka"] != "connected" {
		t.Errorf("unexpected health with a reachable broker: %d %v", code, body)
	}

	fake.pingErr = errors.New("no brokers")

	code, body := healthResponse(t)
	if code != http.StatusServiceUnavailable || body["status"] != "unhealthy" || body["kafka"] != "disconnected" {
		t.Errorf("unexpected health with an unreachable broker: %d %v", code, body)
	}
}

func TestSubscribeHandlerReportsClientFailure(t *testing.T) {
	cfg := resetState(t)

	newClient = func() (pubsub.Client, error) { return nil, errors.New("KAFKA_HOSTS env required") }

	rec := httptest.NewRecorder()
	subscribeHandler(context.Background(), rec, httptest.NewRequest(http.MethodGet, "/subscribe", http.NoBody), cfg)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}
