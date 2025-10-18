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
│       ├── discover.py      # Schema discovery from API responses (legacy)
│       ├── ddl.py           # Database table creation (Postgres/MySQL)
│       ├── normalize.py     # Data normalization, field extraction, and hashing
│       └── ingest.py        # Orchestration module
├── scripts/
│   ├── ingest_indeed.py     # CLI entrypoint
│   └── validate_data.py     # Data quality validation script
├── aws/
│   └── lambda/
│       └── indeed_ingest/
│           └── README.md    # Future Lambda packaging scaffold
├── .env                     # Environment variables (not committed)
├── .env.sample              # Environment template
├── requirements.txt         # Python dependencies (includes beautifulsoup4)
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

Creates the `raw_indeed_jobs` table with a comprehensive schema:

```bash
python scripts/ingest_indeed.py --mode discover
```

This will:
1. Fetch a sample page of jobs from the API to verify connectivity
2. Create the `raw_indeed_jobs` table with a comprehensive predefined schema that captures ALL fields from the Indeed API response
3. Drop any existing table first (fresh start)

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

The `raw_indeed_jobs` table uses a comprehensive predefined schema that captures ALL fields from the Indeed API.

### Core Fields
- `id` - BIGSERIAL primary key
- `job_key` - Indeed job key
- `provider_id` - Indeed job ID
- `job_hash` - SHA-256 hash for deduplication (UNIQUE NOT NULL)
- `source_payload` - Full JSON response (JSONB NOT NULL)
- `ingested_at` - Ingestion timestamp (TIMESTAMPTZ, default NOW())

### Job Details
- `title` - Job title (TEXT)
- `company_name` - Company name (TEXT)
- `description_html` - Full HTML description (TEXT)
- `description_text` - Plain text description (TEXT)
- `job_url` - Application URL (TEXT)
- `date_published` - Posted date (TIMESTAMPTZ)
- `is_remote` - Remote flag (BOOLEAN)

### Job Types and Categories
- `job_types` - Employment types array (JSONB) - e.g., ["Full-time"]
- `job_type_primary` - Primary job type (TEXT)
- `occupations` - Occupation categories (JSONB)
- `attributes` - Additional job attributes (JSONB)

### Location Fields
- `location_city` - City (TEXT)
- `location_state` - State (TEXT)
- `location_country` - Country (TEXT)
- `location_postal_code` - Zip/postal code (TEXT)
- `location_street_address` - Street address (TEXT)
- `location_lat` - Latitude (NUMERIC)
- `location_lng` - Longitude (NUMERIC)

### Salary Fields
- `salary_min` - Minimum salary (NUMERIC)
- `salary_max` - Maximum salary (NUMERIC)
- `salary_currency` - Currency code (TEXT)
- `salary_period` - Pay period (TEXT) - e.g., "YEAR"
- `salary_text` - Display text (TEXT) - e.g., "$50,000 - $60,000 a year"

### Company Details
- `company_rating` - Company rating (NUMERIC)
- `company_rating_count` - Number of ratings (INTEGER)
- `company_reviews_link` - Reviews URL (TEXT)
- `employer_type` - Employer type (TEXT)

### Benefits and Requirements
- `benefits` - Benefits list (JSONB)
- `qualifications` - Required qualifications (JSONB)
- `hiring_event` - Hiring event details (JSONB)

### Metadata
- `urgency` - Urgency tags (JSONB)
- `featured` - Featured job flag (BOOLEAN)
- `easy_apply` - Easy apply flag (BOOLEAN)
- `external_apply_link` - External apply URL (TEXT)

### Indexes
- Primary key on `id`
- Unique constraint on `job_hash`
- Index on `date_published`
- Index on `company_name`
- Index on `ingested_at`
- Conditional unique index on `job_key` (when not null)

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

## Data Validation

Validate data capture quality after ingestion:

```bash
python scripts/validate_data.py
```

This script checks:
- Description field population rate (target: 80%+)
- Critical field completeness (title, company, location, etc.)
- JSON/array field population (job_types, benefits, occupations, etc.)
- Sample record details

Example output:
```
=== DATA VALIDATION REPORT ===

Total jobs in database: 23

>> Description Fields:
  description_html populated: 23/23 (100.0%)
  description_text populated: 23/23 (100.0%)
  Either description populated: 23/23 (100.0%)
  [PASS] 100.0% >= 80% threshold
```

## Future Work

- **AWS Lambda Packaging**: See `backend/aws/lambda/indeed_ingest/README.md`
- **Secrets Manager**: Migrate credentials from `.env` to AWS Secrets Manager
- **EventBridge Scheduling**: Automate nightly runs
- **CloudWatch Monitoring**: Add metrics and alarms

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
