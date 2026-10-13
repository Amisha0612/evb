package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/d3ltmeshQ/evb/pkg/logger"
	"github.com/d3ltmeshQ/evb/pubsub"
)

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)

	psClient, err := pubsub.NewClient()
	if err != nil {
		logger.Printf("error occured in starting pubsub client: %v", err)
	}

	r.Get("/consume", func(w http.ResponseWriter, r *http.Request) {
		// Setting a timeout of 15 seconds to process any msg in kafka
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()

		msg, err := psClient.Subscribe(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				http.Error(w, "No message received in time", http.StatusRequestTimeout)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Print the msg
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, msg.Value, "", "  "); err != nil {
			fmt.Printf("Received message:\n%s\n", string(msg.Value))
		} else {
			fmt.Printf("Received message:\n%s\n", pretty.String())
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Message printed to console"))
	})

	logger.Print("Server starting on :8000")
	if err := http.ListenAndServe(":8000", r); err != nil {
		logger.Fatal(err)
	}
}
