package internal

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/d3ltmeshQ/evb/internal/queue"
	"github.com/d3ltmeshQ/evb/internal/relay"
	"github.com/d3ltmeshQ/evb/internal/stitching"
	"github.com/d3ltmeshQ/evb/internal/validation"
	"github.com/d3ltmeshQ/evb/pkg/config"
	"github.com/d3ltmeshQ/evb/pkg/logger"
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

	// if err := ctx.PubSub.Ping(); err != nil {
	// 	logger.Error("ping failed! error : [%v]", err)
	// 	return
	// }

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

	defer func() {
		//TODO: Refactor
		// ctx.PubSub.CommitOffset(pubsub.TopicPartition{
		// 	Topic:     msg.Topic,
		// 	Partition: msg.Partition,
		// 	Offset:    msg.Offset,
		// })
	}()
}

func (b *bridge) relayWorker(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if b.pipe.IsEmpty() {
			return
		}

		b.relay.Transport(ctx, b.pipe.Dequeue())
	}
}
