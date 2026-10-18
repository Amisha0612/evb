.PHONY: help build test lint run docker-build docker-up docker-down clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the application
	@echo "Building evb..."
	@go build -o evb .
	@echo "Build complete!"

test: ## Run tests
	@echo "Running tests..."
	@go test ./... -v -race -coverprofile=coverage.txt -covermode=atomic
	@echo "Tests complete!"

test-coverage: test ## Run tests and show coverage
	@go tool cover -html=coverage.txt

lint: ## Run linter
	@echo "Running linter..."
	@golangci-lint run --timeout 5m
	@echo "Linting complete!"

run: ## Run the application locally
	@echo "Starting evb..."
	@go run main.go

docker-build: ## Build Docker image
	@echo "Building Docker image..."
	@docker build -t evb:latest .
	@echo "Docker image built!"

docker-up: ## Start services with docker-compose
	@echo "Starting services..."
	@docker-compose up -d
	@echo "Services started! Access Kafka UI at http://localhost:8081"

docker-down: ## Stop services
	@echo "Stopping services..."
	@docker-compose down
	@echo "Services stopped!"

docker-logs: ## View docker-compose logs
	@docker-compose logs -f

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -f evb coverage.txt
	@rm -rf build/
	@echo "Clean complete!"

install-tools: ## Install development tools
	@echo "Installing tools..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0
	@echo "Tools installed!"

.DEFAULT_GOAL := help
