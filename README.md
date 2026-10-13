# Event Bridge (evb)

A high-performance Change Data Capture (CDC) event processing pipeline built in Go. Consumes database change events from Kafka, validates, aggregates related events by transaction, and relays them to multiple sinks (HTTP, Kafka, Azure Blob Storage).

## Features

- **Event Validation** - Schema validation with operation-specific rules (create, update, delete)
- **Transaction Stitching** - Aggregates related events from different tables within the same transaction
- **Multi-Sink Relay** - Configurable routing to HTTP endpoints, Kafka topics, and Azure Blob Storage
- **Custom Logger** - Thread-safe logger with color-coded levels and system metrics
- **Kafka Pub/Sub** - Built-in consumer and producer with health checks
- **Queue Management** - Buffered event queue with overflow/underflow protection
- **HTTP Server** - Chi-based REST API for subscription management
- **Health Checks** - `/health` endpoint with Kafka connectivity status
- **Graceful Shutdown** - Proper signal handling and resource cleanup
- **Docker Support** - Multi-stage Dockerfile and docker-compose setup
- **CI/CD Pipeline** - GitHub Actions with automated testing and linting
- **Test Coverage** - Unit tests for critical components (queue, stitching, models)

## Architecture

``` bash
Kafka → Subscribe → Validate → Stitch → Queue → Relay → [HTTP|Kafka|Blob]
```

**Components:**

- `internal/bridge.go` - Main event processor orchestrating the pipeline
- `internal/validation` - Event schema validation with go-playground/validator
- `internal/stitching` - Transaction-based event aggregation
- `internal/queue` - Buffered event queue
- `internal/relay` - Multi-sink event dispatcher
- `pubsub/` - Kafka consumer/producer client
- `pkg/logger` - Custom logging framework
- `pkg/config` - Environment-based configuration

## Getting Started

### Prerequisites

- Go 1.27+
- Kafka cluster
- (Optional) Azure Blob Storage account

### Installation

```bash
git clone https://github.com/Amisha0612/evb.git
cd evb
go mod download
```

### Configuration

Create `configs/.env`:

```env
PORT=8080
KAFKA_HOSTS=localhost:9092
KAFKA_TOPIC=cdc-events
KAFKA_CONSUMER_GROUP_ID=evb-consumer-group
KAFKA_MAX_RETRY=3
KAFKA_RETRY_FREQUENCY=500
QUEUE_SIZE=1000
RULESET_MAP={"c":"users,orders","u":"users,orders","d":"users"}
```

Create `config.yaml` for sink configuration:

```yaml
config:
  servers:
    - name: "postgres-server"
      sinks:
        https:
          - url: "https://api.example.com/webhook"
            timeout: 5000
        kafka:
          brokers: ["localhost:9092"]
          topic: "processed-events"
        blob:
          - container: "events"
            account: "mystorageaccount"
```

### Running Locally

```bash
# Using Go
go run main.go

# Or using Makefile
make run
```

### Running with Docker Compose

```bash
# Start all services (Kafka + evb + Kafka UI)
docker-compose up -d

# View logs
docker-compose logs -f evb

# Stop services
docker-compose down
```

Server starts on `:8080`. Services available:

- **evb**: http://localhost:8080
- **Kafka UI**: http://localhost:8081 (for debugging Kafka topics)

## API Endpoints

- `GET /subscribe` - Initialize event subscription and start background processing
- `GET /health` - Health check endpoint (returns Kafka connectivity status)

## Project Structure

``` bash
evb/
├── internal/
│   ├── bridge.go           # Main event processor
│   ├── models/             # Data models
│   ├── validation/         # Event validation
│   ├── stitching/          # Transaction aggregation
│   ├── queue/              # Event queue
│   └── relay/              # Multi-sink relay
│       ├── https.go
│       ├── kafka.go
│       └── blob.go
├── pubsub/                 # Kafka client
├── pkg/
│   ├── logger/             # Custom logger
│   ├── config/             # Configuration
│   └── http/               # HTTP utilities
├── examples/               # Usage examples
│   ├── producer/
│   └── consumer/
└── main.go
```

## Event Flow

1. **Subscribe** - Kafka consumer subscribes to configured topics
2. **Validate** - Events validated against schema (op, txID, LSN, table)
3. **Stitch** - Events with same txID aggregated based on ruleset
4. **Queue** - Stitched events buffered in channel-based queue
5. **Relay** - Events dispatched to configured sinks every 30ms

## Event Format

Input (Kafka CDC Event):

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

Output (Stitched Event):

```json
{
  "txId": 12345,
  "server": "postgres-server",
  "isStitched": true,
  "tables": {
    "users": {
      "before": null,
      "after": {"id": 1, "name": "John"},
      "payload": {"id": 1, "name": "John"}
    }
  }
}
```

## Development

### Prerequisites

- Go 1.27+
- Docker & Docker Compose (for containerized development)
- golangci-lint (for linting)

### Building

```bash
# Build locally
make build

# Build Docker image
make docker-build
```

### Linting

```bash
# Run linter
make lint

# Install linter
make install-tools
```

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage

# Run specific package tests
go test ./internal/queue -v
```

### Makefile Commands

```bash
make help           # Show all available commands
make build          # Build the application
make test           # Run tests
make lint           # Run linter
make run            # Run locally
make docker-build   # Build Docker image
make docker-up      # Start with docker-compose
make docker-down    # Stop docker-compose
make clean          # Clean build artifacts
```

## Dependencies

- **Chi** - HTTP router
- **Sarama** - Kafka client
- **go-playground/validator** - Struct validation
- **Azure SDK** - Blob storage integration
- **godotenv** - Environment configuration

## License

MIT

## Contributing

Contributions welcome! Please open an issue or submit a PR.

## Roadmap

- [x] Health check endpoints
- [x] Graceful shutdown
- [x] Docker containerization
- [x] CI/CD pipeline (GitHub Actions)
- [x] Unit tests for core components
- [x] golangci-lint integration
- [ ] Prometheus metrics & monitoring
- [ ] Retry mechanisms with exponential backoff
- [ ] Dead letter queue for failed events
- [ ] Event replay capability
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Increase test coverage to 80%+
- [ ] Integration tests with testcontainers
- [ ] Performance benchmarks
