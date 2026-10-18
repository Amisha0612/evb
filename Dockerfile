# Multi-stage build for smaller final image
FROM golang:1.27-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o evb .

# Final stage - minimal runtime image
FROM alpine:3.22

# Install ca-certificates for HTTPS calls
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 evb && \
    adduser -D -u 1000 -G evb evb

# Set working directory
WORKDIR /home/evb

# Copy binary from builder
COPY --from=builder /app/evb .

# Copy config files
COPY --chown=evb:evb config.example.yaml config.yaml
COPY --chown=evb:evb configs/.env.example configs/.env

# Switch to non-root user
USER evb

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run the application
CMD ["./evb"]
