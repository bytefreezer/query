# Build stage
FROM golang:1.21-bookworm AS builder

WORKDIR /app

# Install DuckDB dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy go mod files
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=1 GOOS=linux go build -o bytefreezer-query .

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/bytefreezer-query .

# Copy UI and prompts
COPY ui/ ./ui/
COPY prompts/ ./prompts/

# Expose port
EXPOSE 8000

# Run the application
CMD ["./bytefreezer-query"]
