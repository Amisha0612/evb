package internal

import (
	"context"
	"time"

	"github.com/IBM/sarama"

	"github.com/Amisha0612/evb/internal/queue"
	"github.com/Amisha0612/evb/internal/relay"
	"github.com/Amisha0612/evb/internal/stitching"
	"github.com/Amisha0612/evb/internal/validation"
	"github.com/Amisha0612/evb/pkg/config"
	"github.com/Amisha0612/evb/pkg/logger"
)

type bridge struct {
	validate validation.Controller
	stitch   stitching.Controller
	pipe     queue.Controller
	relay    relay.Controller
}

type Controller interface {
	Process(context.Context, *sarama.ConsumerMessage)
}

func New(ctx context.Context, cfg *config.GoDotEnvProvider) Controller {
	configPath := cfg.GetOrDefault("SINK_CONFIG_PATH", "config.yaml")

	b := &bridge{
		stitch:   stitching.New(ctx, cfg),
		validate: validation.New(),
		pipe:     queue.New(ctx, cfg),
		relay:    relay.New(ctx, configPath),
	}

	go b.relayWorker(ctx)

	return b
}

func (b *bridge) Process(ctx context.Context, msg *sarama.ConsumerMessage) {
	if b.pipe.IsFull() {
		logger.Errorf("queue is full, size is: [%v]", b.pipe.QueueSize())
		return
	}

	event, err := b.validate.Validator(ctx, msg)
	if err != nil {
		logger.Errorf("validation failed! error : [%v]", err)
		return
	}

	stitchedEvent := b.stitch.Aggregate(ctx, event)
	if stitchedEvent != nil {
		logger.Infof("stitched event: %v", stitchedEvent)
		b.pipe.Enqueue(stitchedEvent)
	}

	// TODO: commit the Kafka offset once the event has been processed.
}

func (b *bridge) relayWorker(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		b.drain(ctx)
	}
}

// drain relays everything that is queued right now. The tick only sets how often
// the queue is polled; it does not cap throughput.
func (b *bridge) drain(ctx context.Context) {
	for ctx.Err() == nil {
		event := b.pipe.Dequeue()
		if event == nil {
			return
		}

		if err := b.relay.Transport(ctx, event); err != nil {
			logger.Errorf("relay failed! error : [%v]", err)
		}
	}
}
