package relay

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/Amisha0612/evb/internal/models"
	"github.com/Amisha0612/evb/pkg/logger"
)

type Controller interface {
	// Transport delivers the event to every sink of its server. It returns nil only when
	// every sink accepted it. Any other error means the event was not fully delivered;
	// if that error satisfies errors.Is(err, ErrUnconfirmed) the event is not recorded
	// anywhere either, so the caller must not acknowledge the Kafka message it came from.
	Transport(context.Context, *models.StitchedEvent) error
}

// ErrUnconfirmed marks a delivery that neither succeeded nor was safely recorded in
// the dead-letter store: the process is shutting down, or the store itself failed.
// The event can only be recovered by reading its Kafka message again.
var ErrUnconfirmed = errors.New("delivery not confirmed")

type sinkConfig struct {
	Config config `yaml:"config"`
}

type config struct {
	Servers []server `yaml:"servers"`
}

type server struct {
	Name  string `yaml:"name"`
	Sinks sinks  `yaml:"sinks"`
}

type sinks struct {
	Kafka kafka   `yaml:"kafka"`
	HTTPS []https `yaml:"https"`
	Blob  []blob  `yaml:"blob"`
}

type relay struct {
	configs    sinkConfig
	httpSinks  map[string][]httpController
	kafkaSinks map[string]kafkaController
	blobSinks  map[string][]blobController
	retry      retryPolicy
	dead       *deadLetterStore
}

func New(_ context.Context, configPath string) Controller {
	r := &relay{
		configs:    sinkConfig{},
		httpSinks:  make(map[string][]httpController),
		kafkaSinks: make(map[string]kafkaController),
		blobSinks:  make(map[string][]blobController),
		retry:      retryPolicyFromEnv(),
		dead:       newDeadLetter(deadLetterPathFromEnv()),
	}

	// Use default if not provided
	if configPath == "" {
		configPath = os.Getenv("SINK_CONFIG_PATH")
		if configPath == "" {
			configPath = "config.yaml"
		}
	}

	fileBytes, err := os.ReadFile(configPath)
	if err != nil {
		logger.Errorf("error : [%v] in opening config file at path: %s", err.Error(), configPath)
		return nil
	}

	buffer := sinkConfig{}

	if err := yaml.Unmarshal(fileBytes, &buffer); err != nil {
		logger.Errorf("error : [%v] in reading config file", err.Error())
		return nil
	}

	for si := range buffer.Config.Servers {
		s := &buffer.Config.Servers[si]

		httpCtrl := make([]httpController, 0, len(s.Sinks.HTTPS))
		for i := range s.Sinks.HTTPS {
			httpCtrl = append(httpCtrl, newHTTPS(&s.Sinks.HTTPS[i]))
		}

		r.httpSinks[s.Name] = httpCtrl
		r.kafkaSinks[s.Name] = newKafkaProducer(&s.Sinks.Kafka)

		blobCtrl := make([]blobController, 0, len(s.Sinks.Blob))
		for i := range s.Sinks.Blob {
			blobCtrl = append(blobCtrl, newBlob(&s.Sinks.Blob[i]))
		}

		r.blobSinks[s.Name] = blobCtrl
	}

	r.configs = buffer

	return r
}

// sink is what every destination (HTTPS, Kafka, Blob) offers the dispatcher.
type sink interface {
	GetID() string
	Transport(context.Context, any) (any, error)
}

// Transport delivers the event to every sink configured for its server. Sinks are
// independent: one that fails (after its retries) does not stop delivery to the
// others. Failed deliveries are written to the dead-letter store and every
// failure is returned, joined, so the caller knows the event was not fully delivered.
func (r *relay) Transport(ctx context.Context, event *models.StitchedEvent) error {
	targets, configured := r.targets(event.Server)
	if !configured {
		err := fmt.Errorf("no sinks configured for server %q", event.Server)
		logger.Errorf("%v", err)

		return r.recordFailure("none", "", event, 0, err)
	}

	var errs []error

	for _, t := range targets {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("%w: %w", ErrUnconfirmed, err))

			break
		}

		if err := r.deliver(ctx, t.kind, t.sink, event); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

type target struct {
	kind string
	sink sink
}

// targets lists the enabled sinks for a server. configured is false when the
// server is not in the sink file at all; a server whose sinks were all skipped
// (for example a Kafka sink without hosts) is configured, with no targets.
func (r *relay) targets(server string) (targets []target, configured bool) {
	httpSinks, isHTTP := r.httpSinks[server]
	kafkaSink, isKafka := r.kafkaSinks[server]
	blobSinks, isBlob := r.blobSinks[server]

	if !isHTTP && !isKafka && !isBlob {
		return nil, false
	}

	for _, s := range httpSinks {
		if s != nil {
			targets = append(targets, target{kind: "https", sink: s})
		}
	}

	if kafkaSink != nil {
		targets = append(targets, target{kind: "kafka", sink: kafkaSink})
	}

	for _, s := range blobSinks {
		if s != nil {
			targets = append(targets, target{kind: "blob", sink: s})
		}
	}

	return targets, true
}

// deliver sends the event to one sink, retrying transient failures with
// exponential backoff, and dead-letters the event if it cannot be delivered.
func (r *relay) deliver(ctx context.Context, kind string, s sink, event *models.StitchedEvent) error {
	var lastErr error

	attempts := 0

	for attempt := 1; attempt <= r.retry.MaxAttempts; attempt++ {
		attempts = attempt

		_, lastErr = s.Transport(ctx, event)
		if lastErr == nil {
			return nil
		}

		logger.Errorf("attempt %d/%d to deliver to %s sink %s failed: %v", attempt, r.retry.MaxAttempts, kind, s.GetID(), lastErr)

		if isPermanent(lastErr) || attempt == r.retry.MaxAttempts {
			break
		}

		if !sleepContext(ctx, r.retry.delay(attempt)) {
			// Shutdown, not a sink verdict: leave the event for the next start instead
			// of filing it as failed.
			return fmt.Errorf("%s sink %s: %w: %w", kind, s.GetID(), ErrUnconfirmed, errors.Join(lastErr, ctx.Err()))
		}
	}

	return r.recordFailure(kind, s.GetID(), event, attempts, fmt.Errorf("%s sink %s: %w", kind, s.GetID(), lastErr))
}

// recordFailure files a failed delivery in the dead-letter store and returns the
// failure. If the store cannot take it the result also wraps ErrUnconfirmed.
func (r *relay) recordFailure(kind, id string, event *models.StitchedEvent, attempts int, failure error) error {
	if err := r.dead.Write(kind, id, event, attempts, failure); err != nil {
		logger.Errorf("event for tx %.0f was NOT delivered and could not be dead-lettered: %v", event.TxID, err)

		return fmt.Errorf("%w: dead-letter write failed (%w): %w", ErrUnconfirmed, err, failure)
	}

	return failure
}

// sleepContext waits for d and reports whether the full wait elapsed before ctx ended.
func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
