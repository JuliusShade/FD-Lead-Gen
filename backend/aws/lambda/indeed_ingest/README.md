# Indeed Ingest Lambda Function

**Status:** Scaffold only - Not yet implemented

## Purpose

This directory is reserved for packaging the Indeed job ingestion system as an AWS Lambda function for scheduled execution.

## Planned Implementation

The Lambda function will:

1. **Package** the code from `backend/src/indeed/` into a deployable Lambda package
2. **Use AWS Secrets Manager** for secure credential storage (RapidAPI key, database credentials)
3. **Execute on schedule** via EventBridge (CloudWatch Events) for nightly runs
4. **Run in VPC** with proper security groups to access Aurora database
5. **Log to CloudWatch** for monitoring and debugging

## Configuration Requirements

When implementing, the following AWS resources will be needed:

### IAM Role
- Lambda execution role with policies for:
  - VPC access (for Aurora connectivity)
  - Secrets Manager read access
  - CloudWatch Logs write access

### Secrets Manager
Store secrets in AWS Secrets Manager:
- `indeed/rapidapi-credentials` - RapidAPI key and host
- `indeed/database-credentials` - Aurora database connection string

### VPC Configuration
- Deploy Lambda in same VPC as Aurora
- Configure security groups to allow Lambda → Aurora communication
- Use private subnets with NAT gateway for external API calls

### EventBridge Rule
Schedule expression examples:
- Nightly at 2 AM UTC: `cron(0 2 * * ? *)`
- Every 6 hours: `rate(6 hours)`

## Handler Function

The Lambda handler will invoke the orchestrator:

```python
import os
import json
from indeed.ingest import IndeedIngestionOrchestrator

def lambda_handler(event, context):
    """
    Lambda handler for Indeed job ingestion.

    Event can specify:
    - mode: discover | backfill | nightly (default)
    - query: job search query
    - location: location string
    - fromDays: days back to search
    - maxPages: max pages to fetch
    """

    # Load secrets from Secrets Manager
    # ... (implementation TBD)

    # Run ingestion
    orchestrator = IndeedIngestionOrchestrator()

    mode = event.get('mode', 'nightly')

    if mode == 'nightly':
        result = orchestrator.run_nightly()
    elif mode == 'backfill':
        result = orchestrator.run_backfill(
            from_days=event.get('fromDays', 30),
            max_pages=event.get('maxPages', 10)
        )
    # ... etc

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
```

## Deployment

Future deployment will use:
- **AWS SAM** or **Serverless Framework** for IaC
- **Docker image** for Lambda (Python 3.11+ runtime)
- **Layer** for dependencies (psycopg2, requests, etc.)

## Next Steps

To implement this Lambda function:

1. Create `handler.py` with Lambda handler function
2. Set up AWS SAM or CDK template for infrastructure
3. Configure Secrets Manager and IAM roles
4. Package dependencies into Lambda layer
5. Deploy and configure EventBridge schedule
6. Set up CloudWatch alarms for monitoring

---

**Note:** This is a placeholder for future work. Current implementation runs locally only.
