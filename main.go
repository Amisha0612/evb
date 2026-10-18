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

	// newClient is a variable so tests can run without a Kafka cluster.
	newClient = pubsub.NewClient
)

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
	bridge = internal.New(ctx, cfg)
	b := bridge

	go func() {
		for ctx.Err() == nil {
			msg, err := client.Subscribe(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("subscription error: %v", err)
					time.Sleep(time.Second)
				}

				continue
			}

			go b.Process(ctx, msg)
		}
	}()

	return nil
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
