package relay

import (
	"os"
	"testing"

	"gopkg.in/yaml.v2"
)

func loadExampleConfig(t *testing.T) sinkConfig {
	t.Helper()

	raw, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatalf("reading example config: %v", err)
	}

	var cfg sinkConfig
	if err := yaml.UnmarshalStrict(raw, &cfg); err != nil {
		t.Fatalf("example config has keys the relay does not understand: %v", err)
	}

	if len(cfg.Config.Servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(cfg.Config.Servers))
	}

	return cfg
}

// config.example.yaml is documentation users copy verbatim, so it must decode
// into the real sink structs with every field populated.
func TestExampleConfigHTTPSSink(t *testing.T) {
	srv := loadExampleConfig(t).Config.Servers[0]

	if srv.Name != "postgres-server" {
		t.Errorf("server name = %q", srv.Name)
	}

	h := srv.Sinks.HTTPS[0]
	if h.URL == "" || h.Path == "" || h.Method == "" || h.Auth.ClientID == "" || h.Auth.TokenURL == "" {
		t.Errorf("https sink not fully decoded: %+v", h)
	}
}

func TestExampleConfigKafkaSink(t *testing.T) {
	k := loadExampleConfig(t).Config.Servers[0].Sinks.Kafka

	if len(k.Hosts) == 0 || len(k.Topics) == 0 || k.MaxRetries == 0 || k.RetryFrequency == 0 {
		t.Errorf("kafka sink not fully decoded: %+v", k)
	}
}

func TestExampleConfigBlobSink(t *testing.T) {
	b := loadExampleConfig(t).Config.Servers[0].Sinks.Blob[0]

	if b.AccountName == "" || b.ContainerName == "" || b.ServiceURL == "" || b.PathPrefix == "" || b.SasToken == "" {
		t.Errorf("blob sink not fully decoded: %+v", b)
	}
}
