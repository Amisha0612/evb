# Event Bridge (evb)

A Change Data Capture (CDC) event processing pipeline written in Go. It consumes database change events from Kafka, validates them, groups the events of one transaction together, and relays the result to HTTP, Kafka and Azure Blob Storage sinks.

## Features

- **Event validation** - Schema and per-operation rules for create, update, delete and read events
- **Transaction stitching** - Collects the events of one transaction (across tables) until a configurable set of tables has arrived, and suppresses duplicates
- **Multi-sink relay** - Per-server routing to HTTPS endpoints (OAuth2 client credentials), Kafka topics and Azure Blob Storage
- **Kafka pub/sub** - Consumer-group subscriber and producer with a connectivity check
- **Buffered queue** - Fixed-size queue between stitching and relay; events are dropped with an error log when it is full
- **HTTP server** - Chi-based API to start the subscription and report health
- **Graceful shutdown** - Signal handling, then context cancellation, then the Kafka client is closed
- **Logger** - Leveled, colour-coded logger with on-demand runtime stats
- **Docker** - Multi-stage Dockerfile and a docker compose stack (Kafka, evb, Kafka UI)
- **CI** - GitHub Actions: tests with the race detector, golangci-lint v2, build

## Architecture

``` bash
Kafka → Subscribe → Validate → Stitch → Queue → Relay → [HTTPS|Kafka|Blob]
```

**Components:**

- `internal/bridge.go` - Orchestrates the pipeline and runs the relay worker
- `internal/validation` - Event validation with go-playground/validator
- `internal/stitching` - Transaction-based aggregation with TTL eviction
- `internal/queue` - Buffered event queue
- `internal/relay` - Multi-sink dispatcher
- `pubsub/` - Kafka consumer/producer client
- `pkg/logger` - Logging
- `pkg/config` - Environment-based configuration

## Getting Started

### Prerequisites

- Go 1.27+
- A Kafka broker (`docker compose up -d kafka` starts one, see below)
- (Optional) Azure Blob Storage account

### Installation

```bash
git clone https://github.com/Amisha0612/evb.git
cd evb
go mod download
```

### Configuration

evb reads two kinds of configuration: environment variables and a sink file.

**Environment variables.** Copy the example and edit it:

```bash
cp configs/.env.example configs/.env
```

Values are loaded from `configs/.<ENV>.env` (default `configs/.local.env`) and then `configs/.env`. A variable that is already set in the process environment wins over both files.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_HOSTS` | required | Comma-separated broker addresses |
| `KAFKA_TOPIC` | required | Comma-separated topics to consume; the first is used by `Publish` |
| `KAFKA_CONSUMER_GROUP_ID` | required | Consumer group ID |
| `KAFKA_MAX_RETRY` | `3` | Producer retries |
| `KAFKA_RETRY_FREQUENCY` | `500` | Producer retry backoff in milliseconds |
| `KAFKA_SASL_USER`, `KAFKA_SASL_PASS` | unset | Enables SASL/PLAIN when the user is set |
| `PORT` | `8080` | HTTP server port |
| `QUEUE_SIZE` | `1000` | Capacity of the event queue |
| `SINK_CONFIG_PATH` | `config.yaml` | Path of the sink configuration file |
| `RULESET_MAP` | unset | JSON map of operation to the tables a transaction needs before it is stitched, e.g. `{"c":"users,orders","u":"users,orders","d":"users"}` |
| `STITCH_TTL` | `10m` | How long a transaction is remembered after its last event (Go duration) |
| `ENV` | `local` | Selects the `configs/.<ENV>.env` override file |
| `ENCRYPTOR_KEY`, `ENCRYPTOR_KEY_ID` | unset | AES-GCM key (16, 24 or 32 bytes) for `logger.Encrypted`; a key of any other length stops the process at startup |

**Sink file.** Copy the example and edit it:

```bash
cp config.example.yaml config.yaml
```

```yaml
config:
  servers:
    - name: "postgres-server"        # must equal source.server in the incoming events
      sinks:
        https:
          - url: "https://api.example.com"
            path: "/webhook/cdc-events"
            method: "POST"           # GET, POST, PATCH or DELETE
            auth:
              clientId: "YOUR_CLIENT_ID"
              clientSecret: "YOUR_CLIENT_SECRET"
              keyProviderURL: "https://auth.example.com/oauth/token"
        kafka:
          hosts: ["localhost:9092"]
          topics: ["processed-cdc-events"]
        blob:
          - storageName: "yourstorageaccount"
            container: "cdc-events"
            serviceUrl: "https://yourstorageaccount.blob.core.windows.net"
            pathPrefix: "events/"
            sasToken: "?sv=..."
```

A Kafka sink with no `hosts` or `topics` is skipped. See [`config.example.yaml`](config.example.yaml) for every supported key; a test checks that the example decodes into the real sink structs.

`configs/.env` and `config.yaml` can contain credentials. Keep them out of version control.

### Running Locally

```bash
# Optional: start a local broker (published on localhost:9094)
docker compose up -d kafka

# Run evb (set KAFKA_HOSTS=localhost:9094 when using the compose broker)
make run          # or: go run main.go

# Start consuming
curl http://localhost:8080/subscribe
```

evb does not consume anything until `/subscribe` has been called once.

### Running with Docker Compose

```bash
# Start Kafka, evb and Kafka UI
docker compose up -d

# View logs
docker compose logs -f evb

# Stop everything
docker compose down
```

| Service | URL |
|---|---|
| evb | http://localhost:8080 |
| Kafka UI | http://localhost:8081 |
| Kafka (from the host) | localhost:9094 |

The evb image ships `config.example.yaml` as its `config.yaml`, so its sinks are placeholders. To use your own, create `config.yaml` and uncomment the `volumes` entry of the `evb` service in `docker-compose.yml`.

## API Endpoints

- `GET /subscribe` - Creates the Kafka client and starts consuming and processing in the background. The first call does the work; later calls do nothing. Returns `500` if the Kafka client cannot be created, and a later call retries.
- `GET /health` - Returns JSON with `status`, `uptime` and `kafka` (`not_initialized` before `/subscribe`, then `connected` or `disconnected`). Responds `503` when the broker cannot be reached.

## Project Structure

``` bash
evb/
├── main.go                 # HTTP server, subscription lifecycle, shutdown
├── internal/
│   ├── bridge.go           # Pipeline orchestration and relay worker
│   ├── models/             # Event types
│   ├── validation/         # Event validation
│   ├── stitching/          # Transaction aggregation
│   ├── queue/              # Event queue
│   └── relay/              # Multi-sink relay
│       ├── relay.go
│       ├── https.go
│       ├── kafka.go
│       └── blob.go
├── pubsub/                 # Kafka client
├── pkg/
│   ├── logger/             # Logger
│   ├── config/             # Configuration
│   └── http/               # HTTP client used by the HTTPS sink
├── examples/               # Producer and consumer examples
├── configs/.env.example    # Environment variable template
├── config.example.yaml     # Sink configuration template
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

## Event Flow

1. **Subscribe** - The Kafka consumer group reads the configured topics.
2. **Validate** - `op` must be `c`, `u`, `d` or `r`; `ts_ms`, `source.txId`, `source.lsn`, `source.table` and `source.server` are required. Creates and reads need `after`, deletes need `before`, and updates need both and they must differ.
3. **Stitch** - Events with the same `txId` are collected until every table that `RULESET_MAP` lists for the operation has arrived. Incomplete transactions are dropped after `STITCH_TTL`; finished ones are remembered for the same time so a redelivered event is ignored.
4. **Queue** - Stitched events wait in a channel-based queue.
5. **Relay** - A worker takes one event from the queue every 30 ms and sends it to each sink configured for the event's server.

## Event Format

Input (Kafka CDC event):

```json
{
  "payload": {
    "op": "c",
    "ts_ms": 1234567890,
    "before": null,
    "after": {"id": 1, "name": "John"},
    "source": {
      "txId": 12345,
      "lsn": 67890,
      "table": "users",
      "server": "postgres-server"
    }
  }
}
```

Output (stitched event, as sent to every sink; `before` and `after` are left out when they are null):

```json
{
  "txId": 12345,
  "server": "postgres-server",
  "isStitched": true,
  "tables": {
    "users": {
      "after": {"id": 1, "name": "John"},
      "payload": {"id": 1, "name": "John"}
    }
  }
}
```

## Development

### Prerequisites

- Go 1.27+
- Docker with Compose v2 (for the compose stack)
- golangci-lint v2 (`make install-tools`)

### Building

```bash
# Build locally
make build

# Build Docker image
make docker-build
```

### Linting

```bash
make lint
make install-tools   # installs golangci-lint v2.14.0
```

### Running Tests

The tests use fakes and Sarama mocks, so they need no Kafka broker.

```bash
# All tests with the race detector
make test

# With an HTML coverage report
make test-coverage

# One package
go test ./internal/stitching -v
```

### Makefile Commands

```bash
make help           # Show all available commands
make build          # Build the application
make test           # Run tests
make test-coverage  # Run tests and open the coverage report
make lint           # Run linter
make run            # Run locally
make docker-build   # Build Docker image
make docker-up      # Start with docker compose
make docker-down    # Stop docker compose
make docker-logs    # Follow docker compose logs
make clean          # Clean build artifacts
make install-tools  # Install golangci-lint
```

## Known Limitations

- The relay worker sends one event per 30 ms tick, roughly 33 events per second per instance.
- A sink failure stops delivery of that event to the sinks after it. There are no retries and no dead-letter queue yet.
- Kafka offsets are marked as soon as a message is handed to the in-memory channel, before it is validated or relayed, so events in flight are lost if the process stops.

## Dependencies

- **Chi** - HTTP router
- **Sarama** - Kafka client
- **go-playground/validator** - Struct validation
- **Azure SDK** - Blob storage integration
- **golang.org/x/oauth2** - OAuth2 client credentials for the HTTPS sink
- **yaml.v2** - Sink configuration
- **godotenv** - Environment configuration

## Contributing

Contributions welcome! Please open an issue or submit a PR.
