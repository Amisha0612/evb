package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/d3ltmeshQ/evb/internal"
	"github.com/d3ltmeshQ/evb/pkg/config"
	"github.com/d3ltmeshQ/evb/pubsub"
	"github.com/go-chi/chi/v5"
)

var (
	bridge       internal.Controller
	pubsubClient pubsub.Client
	once         sync.Once
	startTime    time.Time
)

func main() {
	startTime = time.Now()
	r := chi.NewRouter()
	cfg := config.NewGoDotEnvProvider("configs")
	port := cfg.GetOrDefault("PORT", "8080")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.Get("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		subscribeHandler(w, r, cfg, ctx)
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
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe error: %v", err)
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

	// Close Kafka client if initialized
	if pubsubClient != nil {
		log.Println("Closing Kafka connection...")
		if err := pubsubClient.Close(); err != nil {
			log.Printf("Error closing Kafka client: %v", err)
		}
	}

	// Cancel main context
	cancel()

	log.Println("Server stopped gracefully")
}

func subscribeHandler(w http.ResponseWriter, _ *http.Request, cfg *config.GoDotEnvProvider, ctx context.Context) {
	client, err := pubsub.NewClient()
	if err != nil {
		log.Printf("failed to create pubsub client: %v", err)
		http.Error(w, "Failed to create pubsub client", http.StatusInternalServerError)
		return
	}

	once.Do(func() {
		pubsubClient = client
		bridge = internal.New(ctx, cfg)

		// Start background subscription loop
		go func() {
			for {
				msg, err := client.Subscribe(ctx)
				if err != nil {
					log.Printf("subscription error: %v", err)
					continue
				}
				go bridge.Process(ctx, msg)
			}
		}()
	})

	w.Write([]byte("Subscribed and processing in background\n"))
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	health := map[string]interface{}{
		"status": "healthy",
		"uptime": time.Since(startTime).String(),
	}

	// Check Kafka connectivity if client is initialized
	if pubsubClient != nil {
		if err := pubsubClient.Ping(); err != nil {
			health["status"] = "unhealthy"
			health["kafka"] = "disconnected"
			health["error"] = err.Error()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(health)
			return
		}
		health["kafka"] = "connected"
	} else {
		health["kafka"] = "not_initialized"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(health)
}
