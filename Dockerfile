# Build stage
FROM golang:1.23-bookworm AS builder

WORKDIR /app

# Install DuckDB dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Private module access
ARG GITHUB_TOKEN
ENV GOPRIVATE=github.com/bytefreezer/*
RUN if [ -n "$GITHUB_TOKEN" ]; then \
    git config --global url."https://x-access-token:${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi

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
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/bytefreezer-query .

# Copy UI and prompts
COPY ui/ ./ui/
COPY prompts/ ./prompts/

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD wget -q --spider http://127.0.0.1:8000/api/v1/health || exit 1

# Entrypoint
ENTRYPOINT ["./bytefreezer-query"]
