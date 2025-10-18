# FD-Lead-Gen Backend

Python-based data ingestion system for job postings from Indeed via RapidAPI.

## Overview

This backend ingests job data from Indeed (via RapidAPI) into an Aurora PostgreSQL database. The system supports schema discovery, automated table creation, and robust pagination with rate limiting.

## Directory Structure

```
backend/
├── src/
│   └── indeed/
│       ├── api.py           # RapidAPI client with pagination
│       ├── discover.py      # Schema discovery from API responses
│       ├── ddl.py           # Database table creation (Postgres/MySQL)
│       ├── normalize.py     # Data normalization and hashing
│       └── ingest.py        # Orchestration module
├── scripts/
│   └── ingest_indeed.py     # CLI entrypoint
├── aws/
│   └── lambda/
│       └── indeed_ingest/
│           └── README.md    # Future Lambda packaging scaffold
├── .env                     # Environment variables (not committed)
├── .env.sample              # Environment template
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Setup

### 1. Install Python Dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy `.env.sample` to `.env` and fill in your credentials:

```bash
cp .env.sample .env
```

Required variables:
- `RAPIDAPI_KEY` - Your RapidAPI key for Indeed Scraper API
- `DB_HOST` - Aurora database host
- `DB_DATABASE` - Database name
- `DB_USERNAME` - Database username
- `DB_PASSWORD` - Database password

## Usage

### Discovery Mode

Performs schema discovery and creates the `raw_indeed_jobs` table:

```bash
python scripts/ingest_indeed.py --mode discover
```

This will:
1. Fetch a sample page of jobs from the API
2. Infer schema from the response structure
3. Create the `raw_indeed_jobs` table with discovered columns

### Backfill Mode

Load historical data (default: last 30 days):

```bash
python scripts/ingest_indeed.py --mode backfill --fromDays 30 --maxPages 10
```

**Note:** The RapidAPI endpoint may have restrictions on `fromDays`. If you get 400 errors, try smaller values (e.g., `--fromDays 7`).

### Nightly Mode

Default mode for scheduled runs (last 24 hours):

```bash
python scripts/ingest_indeed.py --mode nightly
```

Equivalent to:
```bash
python scripts/ingest_indeed.py --mode backfill --fromDays 1 --maxPages 10
```

### Custom Mode

Run with fully custom parameters:

```bash
python scripts/ingest_indeed.py --mode custom \
  --query "developer" \
  --location "San Francisco, CA" \
  --fromDays 7 \
  --maxPages 5
```

## CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--mode` | Ingestion mode: discover, backfill, nightly, custom | Required |
| `--query` | Job search query | `packaging` |
| `--location` | Location string | `Springfield, OH` |
| `--fromDays` | Days back to search | `30` (backfill), `1` (nightly) |
| `--maxPages` | Max pages to fetch | `10` |
| `--pageSize` | Results per page | `15` |
| `--sampleSize` | Sample size for discovery | `50` |
| `--verbose` | Enable debug logging | `false` |

## Database Schema

The `raw_indeed_jobs` table includes:

### Core Fields
- `id` - Auto-incrementing primary key
- `provider_id` - Indeed job ID (if available)
- `job_hash` - SHA-256 hash for deduplication (UNIQUE)
- `source_payload` - Full JSON response (JSONB)
- `ingested_at` - Ingestion timestamp

### Discovered Fields
Fields are automatically discovered from API responses. Common fields include:
- `title` - Job title
- `company` - Company name
- `location` - Location (JSONB)
- `job_type` - Employment type (JSONB)
- `salary_text` - Salary information (JSONB)
- `description` - Job description
- `job_url` - Application URL
- `remote` - Remote flag (BOOLEAN)
- `posted_at` - Posted date (TIMESTAMPTZ)

### Indexes
- Primary key on `id`
- Unique index on `job_hash`
- Index on `posted_at`, `company`, `ingested_at`
- Conditional unique index on `provider_id` (when available)

## Deduplication

Jobs are deduplicated using `job_hash`, computed from:
```
SHA-256(provider_id || title || company || location || job_url || posted_at)
```

- PostgreSQL: `ON CONFLICT (job_hash) DO NOTHING`
- MySQL: `INSERT IGNORE` with unique key

## Rate Limiting

The API client implements exponential backoff with jitter for 429 (rate limit) responses:
- Retries up to 5 times
- Wait time: 2^attempt + random(0, 1) seconds

## Pagination

Fetches multiple pages automatically:
- Stops when receiving fewer results than `maxRows`
- Stops on empty page
- Configurable via `MAX_PAGES` env var or `--maxPages` CLI flag

## Example Run

```bash
# 1. Create table
$ python scripts/ingest_indeed.py --mode discover

[INFO] === DISCOVERY MODE ===
[INFO] Fetched 15 sample jobs
[INFO] Discovered schema with 10 fields
[INFO] Table raw_indeed_jobs created successfully
[INFO] Discovery complete!

# 2. Load historical data (7 days)
$ python scripts/ingest_indeed.py --mode backfill --fromDays 7 --maxPages 3

[INFO] === BACKFILL MODE ===
[INFO] Total jobs fetched: 45 from 3 pages
[INFO] Backfill complete! Inserted: 24, Skipped: 21, Errors: 0
```

## Logging

Logs are output to stdout with timestamps and log levels:

```
2025-10-18 18:56:16 [INFO] indeed.ingest: Fetched 45 total jobs
2025-10-18 18:56:16 [INFO] indeed.ddl: Connected to postgres database
2025-10-18 18:56:19 [INFO] indeed.ingest: Inserted: 24, Skipped: 21, Errors: 0
```

Use `--verbose` for debug-level logging.

## Future Work

- **AWS Lambda Packaging**: See `backend/aws/lambda/indeed_ingest/README.md`
- **Secrets Manager**: Migrate credentials from `.env` to AWS Secrets Manager
- **EventBridge Scheduling**: Automate nightly runs
- **CloudWatch Monitoring**: Add metrics and alarms
- **Data Quality**: Add validation and quality checks

## Troubleshooting

### 400 Bad Request on Backfill

The API may reject large `fromDays` values. Try smaller intervals:
```bash
python scripts/ingest_indeed.py --mode backfill --fromDays 7 --maxPages 5
```

### Rate Limiting (429)

The client automatically retries with exponential backoff. If you see persistent rate limiting, reduce `--maxPages` or add delays between runs.

### Connection Errors

Verify database credentials and network connectivity:
```bash
psql -h $DB_HOST -U $DB_USERNAME -d $DB_DATABASE
```

## License

Internal use only - FD-Lead-Gen project.
