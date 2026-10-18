package internal

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"

	"github.com/Amisha0612/evb/internal/queue"
	"github.com/Amisha0612/evb/internal/relay"
	"github.com/Amisha0612/evb/internal/stitching"
	"github.com/Amisha0612/evb/internal/validation"
	"github.com/Amisha0612/evb/pkg/config"
	"github.com/Amisha0612/evb/pkg/logger"
)

// acker is how the bridge reports that a Kafka message has been dealt with.
type acker interface {
	Ack(*sarama.ConsumerMessage)
}

type bridge struct {
	validate validation.Controller
	stitch   stitching.Controller
	pipe     queue.Controller
	relay    relay.Controller
	acker    acker
}

type Controller interface {
	// Process handles one Kafka message. It blocks while the queue to the sinks is full,
	// which slows the consumer down instead of dropping events.
	Process(context.Context, *sarama.ConsumerMessage)
}

// New builds the pipeline. Messages are acknowledged through acker once they are done
// with, so the consumer only commits offsets of work that cannot be lost any more.
func New(ctx context.Context, cfg *config.GoDotEnvProvider, acker acker) Controller {
	configPath := cfg.GetOrDefault("SINK_CONFIG_PATH", "config.yaml")

	b := &bridge{
		stitch:   stitching.New(ctx, cfg),
		validate: validation.New(),
		pipe:     queue.New(ctx, cfg),
		relay:    relay.New(ctx, configPath),
		acker:    acker,
	}

	go b.relayWorker(ctx)

	return b
}

func (b *bridge) ack(msgs ...*sarama.ConsumerMessage) {
	if b.acker == nil {
		return
	}

	for _, m := range msgs {
		if m != nil {
			b.acker.Ack(m)
		}
	}
}

func (b *bridge) Process(ctx context.Context, msg *sarama.ConsumerMessage) {
	event, err := b.validate.Validator(ctx, msg)
	if err != nil {
		// A message that fails validation will fail every time, so reading it again
		// after a restart would only block the partition.
		logger.Errorf("validation failed! error : [%v]", err)
		b.ack(msg)

		return
	}

	stitchedEvent, released := b.stitch.AggregateHolding(ctx, event)

	// Duplicates and expired transactions: nothing is buffered for them any more.
	b.ack(released...)

	if stitchedEvent == nil {
		// Part of an unfinished transaction. Its message stays unacknowledged: the data
		// only exists in memory, so after a crash it has to be read again.
		return
	}

	logger.Infof("stitched event: %v", stitchedEvent)

	// Wait for room rather than drop the event. If shutdown interrupts the wait the
	// event never entered the queue and its messages stay unacknowledged.
	if err := b.pipe.EnqueueContext(ctx, stitchedEvent); err != nil {
		logger.Errorf("stopped while waiting to enqueue tx %.0f: %v", stitchedEvent.TxID, err)
	}
}

// sweepInterval is how often the worker gives up on transactions whose remaining
// tables never arrived, even if no new events come in to trigger the cleanup.
const sweepInterval = time.Second

func (b *bridge) relayWorker(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	lastSweep := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		b.drain(ctx)

		if time.Since(lastSweep) >= sweepInterval {
			b.ack(b.stitch.Sweep()...)

			lastSweep = time.Now()
		}
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

		err := b.relay.Transport(ctx, event)
		if err != nil {
			logger.Errorf("relay failed! error : [%v]", err)
		}

		// Delivered, or failed and recorded in the dead-letter store: either way the
		// outcome is settled. Only an unconfirmed delivery is left for a re-read.
		if errors.Is(err, relay.ErrUnconfirmed) {
			continue
		}

		b.ack(event.Messages...)
	}
}
