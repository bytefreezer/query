# ByteFreezer Query

AI-powered natural language query interface for security log analysis using DuckDB.

## Overview

ByteFreezer Query allows you to query Parquet data stored in S3 using natural language. It uses LLMs (Anthropic, OpenAI, or Ollama) to translate questions into DuckDB SQL, then executes them against your data.

**Key features:**
- Natural language to SQL conversion
- Direct DuckDB queries on S3 Parquet files
- Supports Anthropic, OpenAI, and Ollama
- Runs in your VPC - your data never leaves your environment
- Simple web UI for interactive queries
- Demo limits (configurable): max time range, row limit, ORDER BY restriction

## Deployment Modes

### 1. Standalone Mode

For single-tenant deployments where the service is dedicated to one account. Configure `account_id` in the S3 config to specify which account's data to query.

```yaml
s3:
  bucket: output
  account_id: your-account-id    # Data path: {account_id}/{dataset_id}/data/parquet/
  endpoint: localhost:9000
  ssl: false
```

The service will only see datasets belonging to this account. Suitable for:
- On-premises deployments
- Single-customer installations
- Air-gapped environments

### 2. Shared Mode (with ByteFreezer UI)

For multi-tenant deployments integrated with the ByteFreezer UI. The UI passes the logged-in user's account_id to the query service via HTTP header.

**Configuration (no account_id in config):**
```yaml
s3:
  bucket: output
  # account_id not set - will be provided via header
  endpoint: localhost:9000
  ssl: false
```

**Required HTTP Header:**
```
X-ByteFreezer-Account-ID: customer-account-id
```

The header must be included in all API requests. The UI automatically includes this header from the logged-in user's session.

**Integration points:**
- Query page at `/dashboard/query` in ByteFreezer UI
- UI proxies requests to query service with account header
- Account isolation enforced by session
- Dataset names from control service

See ByteFreezer UI documentation for integration details.

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

All endpoints require either:
- `account_id` configured in config.yaml (standalone mode), OR
- `X-ByteFreezer-Account-ID` header in request (shared mode)

### POST /api/v1/query/natural
Natural language query.

Request:
```json
{"dataset_id": "my-dataset", "question": "show me failed logins in the last hour"}
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

### POST /api/v1/query/sql
Raw SQL query.

Request:
```json
{"sql": "SELECT * FROM read_parquet('s3://...') LIMIT 10"}
```

### GET /api/v1/datasets
List available datasets for the account.

Response:
```json
{
  "account_id": "customer-1",
  "datasets": [{"id": "syslog", "name": "syslog", "file_count": 10}]
}
```

### GET /api/v1/schema?dataset_id=my-dataset
Get data schema for a dataset.

Response:
```json
{
  "dataset_id": "my-dataset",
  "columns": [{"name": "timestamp", "type": "TIMESTAMP"}],
  "partitions": ["year", "month", "day"],
  "sample_path": "s3://bucket/data/**/*.parquet"
}
```

### GET /api/v1/health
Health check.

Response:
```json
{"status": "ok", "duckdb": "connected", "s3": "accessible", "account_id": "customer-1"}
```

### GET /api/v1/limits
Get query limits.

Response:
```json
{"max_time_range_hours": 24, "max_row_limit": 100, "allow_order_by": false}
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
