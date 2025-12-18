# ByteFreezer Query

Self-hosted natural language query interface for security log analysis using DuckDB.

## Overview

ByteFreezer Query is a Go-based service that allows you to query your Parquet data stored in S3 using natural language. It uses LLMs (Anthropic, OpenAI, or Ollama) to translate questions into DuckDB SQL, then executes them against your data.

**Key features:**
- Natural language to SQL conversion
- Direct DuckDB queries on S3 Parquet files
- Supports Anthropic, OpenAI, and Ollama
- Runs in your VPC - your data never leaves your environment
- Simple web UI for interactive queries

## Quick Start

### Using Docker

1. Copy `config.yaml` and `.env.example` and configure:
```bash
cp .env.example .env
# Edit .env with your S3 credentials and LLM API key
```

2. Start the service:
```bash
docker compose up -d
```

3. Open http://localhost:8000 in your browser

### Building from Source

```bash
# Build
CGO_ENABLED=1 go build -o bytefreezer-query .

# Configure (edit config.yaml or use environment variables)
export BYTEFREEZER_QUERY_S3_BUCKET=your-bucket
export BYTEFREEZER_QUERY_S3_ACCESS_KEY=your-key
export BYTEFREEZER_QUERY_S3_SECRET_KEY=your-secret
export BYTEFREEZER_QUERY_LLM_PROVIDER=anthropic
export BYTEFREEZER_QUERY_LLM_API_KEY=your-api-key

# Run
./bytefreezer-query
```

## Configuration

Configuration can be provided via `config.yaml` or environment variables with prefix `BYTEFREEZER_QUERY_`.

### config.yaml

```yaml
app:
  name: bytefreezer-query
  version: 1.0.0

logging:
  level: info

server:
  port: 8000

s3:
  region: us-west-2
  access_key: ""
  secret_key: ""
  bucket: ""
  data_path: data/
  ssl: true
  url_style: vhost
  # endpoint: http://minio:9000  # For MinIO

llm:
  provider: anthropic
  api_key: ""
  model: claude-sonnet-4-20250514
  ollama_host: http://localhost:11434
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| BYTEFREEZER_QUERY_SERVER_PORT | HTTP server port | 8000 |
| BYTEFREEZER_QUERY_S3_REGION | AWS region | us-west-2 |
| BYTEFREEZER_QUERY_S3_ACCESS_KEY | AWS access key | required |
| BYTEFREEZER_QUERY_S3_SECRET_KEY | AWS secret key | required |
| BYTEFREEZER_QUERY_S3_BUCKET | Bucket name | required |
| BYTEFREEZER_QUERY_S3_DATA_PATH | Path prefix to parquet files | data/ |
| BYTEFREEZER_QUERY_S3_ENDPOINT | Custom endpoint (for MinIO) | - |
| BYTEFREEZER_QUERY_S3_SSL | Use SSL for S3 | true |
| BYTEFREEZER_QUERY_S3_URL_STYLE | URL style (vhost/path) | vhost |
| BYTEFREEZER_QUERY_LLM_PROVIDER | anthropic, openai, or ollama | anthropic |
| BYTEFREEZER_QUERY_LLM_API_KEY | API key for LLM provider | required* |
| BYTEFREEZER_QUERY_LLM_MODEL | Model to use | claude-sonnet-4-20250514 |
| BYTEFREEZER_QUERY_LLM_OLLAMA_HOST | Ollama server URL | http://localhost:11434 |

*Not required for Ollama

## MinIO Configuration

For MinIO deployments:
```yaml
s3:
  endpoint: http://minio.local:9000
  ssl: false
  url_style: path
```

Or via environment:
```bash
BYTEFREEZER_QUERY_S3_ENDPOINT=http://minio.local:9000
BYTEFREEZER_QUERY_S3_SSL=false
BYTEFREEZER_QUERY_S3_URL_STYLE=path
```

## API Endpoints

### POST /query/natural
Natural language query.

Request:
```json
{"question": "show me failed logins in the last hour"}
```

Response:
```json
{
  "sql": "SELECT * FROM ...",
  "columns": ["timestamp", "user", "status"],
  "rows": [["2025-12-17T10:00:00Z", "john", "failed"]],
  "row_count": 1,
  "execution_time_ms": 234
}
```

### POST /query/sql
Raw SQL query.

Request:
```json
{"sql": "SELECT * FROM read_parquet('s3://...') LIMIT 10"}
```

### GET /schema
Get data schema.

Response:
```json
{
  "columns": [{"name": "timestamp", "type": "TIMESTAMP"}],
  "partitions": ["year", "month", "day"],
  "sample_path": "s3://bucket/data/**/*.parquet"
}
```

### GET /health
Health check.

Response:
```json
{"status": "ok", "duckdb": "connected", "s3": "accessible"}
```

## Command Line Options

```
./bytefreezer-query [options]

Options:
  -config string
        Path to configuration file (default "config.yaml")
  -help
        Show help and exit
  -version
        Show version and exit
```

## Example Queries

- "Show me the last 100 events"
- "Count events by type in the last 24 hours"
- "What are the top 10 source IPs by event count?"
- "Show failed events from yesterday"
- "Find all events from IP 10.0.0.1"

## Security Notes

- This service needs read access to your S3 bucket
- LLM API calls send schema info (not raw data) to generate SQL
- Run within your VPC for production use
- Consider adding authentication for exposed deployments

## License

Licensed under Elastic License 2.0
