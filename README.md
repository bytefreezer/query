# ByteFreezer Query

SQL query service for ByteFreezer parquet data with optional AI-powered natural language interface.

## Overview

ByteFreezer Query provides a web UI and REST API for querying parquet files stored in S3/MinIO. Raw SQL queries work out of the box. Optionally connect an LLM (Anthropic, OpenAI, or Ollama) to ask questions in natural language — the LLM translates your question into DuckDB SQL, which executes locally against your data.

**Web UI:** `http://localhost:8000`

**Key features:**
- Web UI with dataset selector, schema sidebar, and query results table
- Raw SQL tab — works immediately with no extra setup
- Ask a Question tab — natural language queries (requires LLM provider)
- DuckDB queries on S3/MinIO parquet files
- Three LLM providers: Anthropic (Claude), OpenAI (GPT), Ollama (local)
- Your data never leaves your environment — LLM only sees column names and types
- Configurable limits: max time range, row limit, ORDER BY restriction

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

### On-Prem (Docker Compose)

The query service is included in the ByteFreezer on-prem Docker Compose stack. After
deploying the full stack, the query web UI is available at:

```
http://<your-host>:8000
```

Raw SQL works immediately. To enable natural language queries, edit `config/query.yaml`
and add an LLM provider (see [Connecting Your Own AI](#connecting-your-own-ai) below),
then restart: `docker compose --profile with-minio restart query`

### Standalone Docker

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

## Connecting Your Own AI

The Query service supports three LLM providers for natural language queries. Without an LLM
configured, the "Ask a Question" tab is disabled in the web UI — raw SQL always works.

**How it works:** When you type a question like "show me failed logins from yesterday", the
service sends your dataset schema (column names and types only) to the LLM, which generates
a DuckDB SQL query. That SQL is then executed locally against your parquet files. Your actual
data rows never leave your environment.

### Anthropic (Claude)

```yaml
llm:
  provider: "anthropic"
  api_key: "sk-ant-api03-your-key-here"
  model: "claude-sonnet-4-20250514"
```

Get an API key at https://console.anthropic.com/settings/keys

Recommended models: `claude-sonnet-4-20250514` (default, fast), `claude-opus-4-20250514` (most capable)

### OpenAI (GPT)

```yaml
llm:
  provider: "openai"
  api_key: "sk-your-key-here"
  model: "gpt-4"
```

Get an API key at https://platform.openai.com/api-keys

Recommended models: `gpt-4` (default), `gpt-4o`, `gpt-4-turbo`

### Ollama (local, free, private)

Run any model locally — no API key needed, no data leaves your network.

```yaml
llm:
  provider: "ollama"
  model: "llama3"
  ollama_host: "http://host.docker.internal:11434"
```

1. Install Ollama: https://ollama.com
2. Pull a model: `ollama pull llama3`
3. Start the server: `ollama serve`

**Docker networking:** Use `host.docker.internal` (Docker Desktop on macOS/Windows) or
`172.17.0.1` (Linux Docker default gateway) to reach Ollama on the host from inside the
query container. If running query outside Docker, use `http://localhost:11434`.

Recommended models: `llama3` (8B, fast), `codellama` (good at SQL), `mixtral` (larger, more capable)

### Environment Variables

LLM settings can also be set via environment variables:

| Variable | Description |
|----------|-------------|
| `BYTEFREEZER_QUERY_LLM_PROVIDER` | `anthropic`, `openai`, or `ollama` |
| `BYTEFREEZER_QUERY_LLM_API_KEY` | API key (not required for Ollama) |
| `BYTEFREEZER_QUERY_LLM_MODEL` | Model name |
| `BYTEFREEZER_QUERY_LLM_OLLAMA_HOST` | Ollama server URL (default: `http://localhost:11434`) |

### After Configuring

Restart the query service to pick up the new config:

```bash
# Docker Compose (on-prem)
docker compose --profile with-minio restart query

# Standalone
systemctl restart bytefreezer-query
```

Then open `http://<your-host>:8000`, select a dataset, and switch to the "Ask a Question" tab.

## Example Queries

Natural language (requires LLM):
- "Show me the last 100 events"
- "Count events by type in the last 24 hours"
- "What are the top 10 source IPs by event count?"
- "Show failed events from yesterday"
- "Find all events from IP 10.0.0.1"

Raw SQL (always works):
```sql
-- Recent events
SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
ORDER BY BfTs DESC LIMIT 100

-- Aggregate by source IP
SELECT source_ip, COUNT(*) as count
FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
GROUP BY source_ip ORDER BY count DESC LIMIT 10

-- Filter by time partition
SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
WHERE year = 2026 AND month = 3 AND day = 12
LIMIT 100
```

## Security Notes

- This service needs read access to your S3/MinIO bucket
- LLM API calls send schema info (column names and types) — never your actual data rows
- Ollama keeps everything local — no external API calls at all
- Run within your VPC for production use
- Consider adding authentication for exposed deployments

## License

Licensed under Elastic License 2.0
