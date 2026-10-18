package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Amisha0612/evb/internal"
	"github.com/Amisha0612/evb/pkg/config"
	"github.com/Amisha0612/evb/pubsub"
)

var (
	bridge       internal.Controller
	pubsubClient pubsub.Client
	subscribeMu  sync.Mutex // guards bridge and pubsubClient
	startTime    time.Time

	// newClient and newBridge are variables so tests can run without Kafka or sinks.
	newClient = pubsub.NewClient
	newBridge = func(ctx context.Context, cfg *config.GoDotEnvProvider, client pubsub.Client) internal.Controller {
		return internal.New(ctx, cfg, client)
	}
)

// maxInFlight bounds how many Kafka messages are processed at the same time. Without
// it every message got its own goroutine, so a slow sink let them pile up without limit.
const maxInFlight = 64

func main() {
	startTime = time.Now()
	r := chi.NewRouter()
	cfg := config.NewGoDotEnvProvider("configs")
	port := cfg.GetOrDefault("PORT", "8080")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.Get("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		subscribeHandler(ctx, w, r, cfg)
	})

	r.Get("/health", healthHandler)

	// Create HTTP server
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	// Channel to listen for interrupt signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// Start server in a goroutine
	go func() {
		log.Printf("Starting server on :%s...\n", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("ListenAndServe error: %v", err)
			stop <- syscall.SIGTERM
		}
	}()

	// Wait for interrupt signal
	<-stop
	log.Println("Shutting down server gracefully...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	// Cancel main context first so the subscription loop and relay worker stop
	cancel()

	// Close Kafka client if initialized
	if client := currentClient(); client != nil {
		log.Println("Closing Kafka connection...")
		if err := client.Close(); err != nil {
			log.Printf("Error closing Kafka client: %v", err)
		}
	}

	log.Println("Server stopped gracefully")
}

func subscribeHandler(ctx context.Context, w http.ResponseWriter, _ *http.Request, cfg *config.GoDotEnvProvider) {
	if err := startSubscription(ctx, cfg); err != nil {
		log.Printf("failed to create pubsub client: %v", err)
		http.Error(w, "Failed to create pubsub client", http.StatusInternalServerError)

		return
	}

	if _, err := w.Write([]byte("Subscribed and processing in background\n")); err != nil {
		log.Printf("failed to write response: %v", err)
	}
}

// startSubscription creates the Kafka client and the processing bridge on the
// first call and is a no-op afterwards. A failed attempt is retried on the next call.
func startSubscription(ctx context.Context, cfg *config.GoDotEnvProvider) error {
	subscribeMu.Lock()
	defer subscribeMu.Unlock()

	if pubsubClient != nil {
		return nil
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	pubsubClient = client
	bridge = newBridge(ctx, cfg, client)

	go consume(ctx, client, bridge)

	return nil
}

// consume feeds Kafka messages to the bridge until the context ends or the client is
// closed. At most maxInFlight messages are processed concurrently; when they are all
// busy the loop waits, which in turn stops it reading from Kafka.
func consume(ctx context.Context, client pubsub.Client, b internal.Controller) {
	slots := make(chan struct{}, maxInFlight)

	var wg sync.WaitGroup
	defer wg.Wait()

	for ctx.Err() == nil {
		msg, err := client.Subscribe(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, pubsub.ErrClosed) {
				return
			}

			log.Printf("subscription error: %v", err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}

			continue
		}

		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}

		wg.Add(1)

		go func() {
			defer wg.Done()
			defer func() { <-slots }()

			b.Process(ctx, msg)
		}()
	}
}

// currentClient returns the Kafka client once /subscribe has created it, or nil before that.
func currentClient() pubsub.Client {
	subscribeMu.Lock()
	defer subscribeMu.Unlock()

	return pubsubClient
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	health := map[string]interface{}{
		"status": "healthy",
		"uptime": time.Since(startTime).String(),
	}

	// Check Kafka connectivity if client is initialized
	if client := currentClient(); client != nil {
		if err := client.Ping(); err != nil {
			health["status"] = "unhealthy"
			health["kafka"] = "disconnected"
			health["error"] = err.Error()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			encodeHealth(w, health)
			return
		}
		health["kafka"] = "connected"
	} else {
		health["kafka"] = "not_initialized"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encodeHealth(w, health)
}

func encodeHealth(w http.ResponseWriter, health map[string]interface{}) {
	if err := json.NewEncoder(w).Encode(health); err != nil {
		log.Printf("failed to encode health response: %v", err)
	}
}
