# Indeed Job Qualification Lambda

AWS Lambda scaffold for automated job qualification with LLM scoring and HR contact sourcing.

## Overview

This Lambda function runs the job qualification pipeline on a schedule (nightly):
- Fetches jobs from `raw_indeed_jobs` (last 24 hours)
- Scores with OpenAI LLM for packaging/operator fit
- Hard rejects jobs requiring U.S. citizenship
- Sources HR contacts via Apollo.io
- Inserts qualified jobs (score >= 80) into `qualified_indeed_jobs`

## Deployment (Future Work)

This directory is a scaffold for future Lambda packaging. Actual deployment will be covered in a separate ticket.

### Planned Architecture

```
EventBridge (cron: daily 2am UTC)
  └─> Lambda: indeed_qualify
        ├─> Read from Aurora: raw_indeed_jobs
        ├─> OpenAI API: score jobs
        ├─> Apollo.io API: find HR contacts
        └─> Write to Aurora: qualified_indeed_jobs
```

### Environment Variables (via Secrets Manager)

```
OPENAI_API_KEY
APOLLO_IO_API_KEY
DB_HOST
DB_DATABASE
DB_USERNAME
DB_PASSWORD
QUALIFY_SCORE_THRESHOLD=80
```

### Handler

```python
# lambda_handler.py
from src.indeed.qualify import JobQualifier

def handler(event, context):
    """Lambda handler for nightly job qualification."""
    qualifier = JobQualifier()
    stats = qualifier.run_nightly()

    return {
        'statusCode': 200,
        'body': stats
    }
```

### Packaging

```bash
# Build deployment package
pip install -r requirements.txt -t package/
cp -r src/ package/
cd package && zip -r ../indeed_qualify.zip .
```

### IAM Permissions

- Aurora RDS access (VPC, security groups)
- Secrets Manager read access
- CloudWatch Logs write access

## Local Testing

Run locally before deploying:

```bash
cd backend
python scripts/qualify_indeed_jobs.py --mode nightly --verbose
```

## Monitoring

- CloudWatch Logs: qualification stats, errors
- CloudWatch Metrics: qualified job count, score distribution
- CloudWatch Alarms: zero qualified jobs, high error rate
