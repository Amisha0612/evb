package queue

import (
	"context"
	"strconv"

	"github.com/d3ltmeshQ/evb/internal/models"
	"github.com/d3ltmeshQ/evb/pkg/config"
)

type Controller interface {
	Enqueue(*models.StitchedEvent)
	Dequeue() *models.StitchedEvent
	IsEmpty() bool
	IsFull() bool
	QueueSize() int
}

type pipe struct {
	queue chan *models.StitchedEvent
}

func New(ctx context.Context, cfg *config.GoDotEnvProvider) Controller {
	queueSize, err := strconv.Atoi(cfg.Get("QUEUE_SIZE"))
	if err != nil {
		queueSize = 1000
	}

	// Create a new queue
	q := make(chan *models.StitchedEvent, queueSize)

	return &pipe{queue: q}
}

// Enqueue adds or pushes an event to the queue.
func (c *pipe) Enqueue(event *models.StitchedEvent) {
	c.queue <- event
}

// Dequeue removes and returns the next event from the queue.
func (c *pipe) Dequeue() *models.StitchedEvent {
	if c.IsEmpty() {
		return nil
	}

	return <-c.queue
}

// Check if the queue is empty before pulling events from it to prevent underflow condition
func (c *pipe) IsEmpty() bool {
	return len(c.queue) == 0
}

// Check if queue is full before pushing events to it to prevent overflow condition
func (c *pipe) IsFull() bool {
	return len(c.queue) == cap(c.queue)
}

// Check the size of the queue
func (c *pipe) QueueSize() int {
	return len(c.queue)
}
