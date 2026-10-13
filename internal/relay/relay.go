package relay

import (
	"context"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/Amisha0612/evb/internal/models"
	"github.com/Amisha0612/evb/pkg/logger"
)

type Controller interface {
	Transport(context.Context, *models.StitchedEvent) error
}

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
}

func New(_ context.Context, configPath string) Controller {
	r := &relay{
		configs:    sinkConfig{},
		httpSinks:  make(map[string][]httpController),
		kafkaSinks: make(map[string]kafkaController),
		blobSinks:  make(map[string][]blobController),
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

func (r *relay) Transport(ctx context.Context, event *models.StitchedEvent) error {
	http, isHTTPSink := r.httpSinks[event.Server]
	if !isHTTPSink {
		logger.Errorf("no https sinks configured for server: %s", event.Server)
	}

	for _, sink := range http {
		response, err := sink.Transport(ctx, event)
		if err != nil {
			logger.Errorf("error in transporting event to sink %s: %v", sink.GetID(), err)
			return err
		}

		logger.Printf("response: %v\n", response)
	}

	kafka, ok := r.kafkaSinks[event.Server]
	if !ok {
		logger.Errorf("no kafka sinks configured for server: %s", event.Server)
	}

	if kafka != nil {
		if _, err := kafka.Transport(ctx, event); err != nil {
			logger.Errorf("error in transporting event to sink %s: %v", kafka.GetID(), err)
			return err
		}
	}

	blobSink, isBlobSink := r.blobSinks[event.Server]
	if !isBlobSink {
		logger.Errorf("no blob sinks configured for server: %s", event.Server)
	}

	for _, sink := range blobSink {
		response, err := sink.Transport(ctx, event)
		if err != nil {
			logger.Errorf("error in transporting event to sink %s: %v", sink.GetID(), err)
			return err
		}

		logger.Printf("response: %v\n", response)
	}

	return nil
}
