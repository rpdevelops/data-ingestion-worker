# Data Ingestion Worker

Asynchronous worker service for processing CSV file imports.

## Overview

The worker consumes messages from AWS SQS queue and processes CSV files stored in S3. It handles both initial processing and reprocessing flows:

- **Initial Processing**: First time processing a CSV file - reads from S3, validates rows, creates staging records
- **Reprocessing**: When user resolves issues and triggers reprocessing - validates existing staging records without re-reading CSV

## Architecture

The worker follows the architecture patterns defined in `AGENT.md`:

- Event-driven processing via SQS
- Idempotent operations using row hashes
- Stateless worker design
- Human-in-the-loop validation support

## Processing Flows

### Initial Processing Flow

1. Consume message from SQS
2. Check if job has staging records
3. If no staging records exist:
   - Read CSV file from S3
   - Pre-process: identify duplicate emails within CSV
   - Pre-load existing emails from contacts table
   - For each row:
     - Generate deterministic row hash
     - Check idempotency (skip if already processed)
     - Create staging record
     - Validate row
     - Create issues if validation fails
     - Mark staging as READY if valid
4. If issues found → set job status to NEEDS_REVIEW
5. If no issues → proceed to consolidation

### Reprocessing Flow

1. Consume message from SQS
2. Check if job has staging records
3. If staging records exist:
   - Get all staging records for job
   - Pre-load existing emails from contacts table
   - Identify duplicate emails within staging records
   - For each staging record:
     - Skip if status is DISCARD (user decision)
     - Validate using staging record data (no CSV read)
     - Update status: READY if valid, ISSUE if invalid
     - Create/update issues as needed
4. If issues found → set job status to NEEDS_REVIEW
5. If no issues → proceed to consolidation

## Key Features

- **Idempotent Processing**: Uses row hashes to prevent duplicate processing
- **Dual Flow Support**: Same processor handles both initial and reprocessing
- **Logging**: Comprehensive logging with flow type indicators
- **Error Handling**: Graceful error handling with job status updates
- **Database Transactions**: Atomic operations for data consistency

## Environment Variables

```bash
# Database
DATABASE_URL=postgresql://user:password@host:port/database

# AWS S3
CSV_BUCKET_NAME=your-bucket-name
AWS_REGION=us-east-1

# AWS SQS
SQS_QUEUE_URL=https://sqs.region.amazonaws.com/account/queue-name
SQS_MAX_NUMBER_OF_MESSAGES=1
SQS_WAIT_TIME_SECONDS=20
SQS_VISIBILITY_TIMEOUT=300

# Processing
MAX_RETRIES=3
RETRY_DELAY_SECONDS=5

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

## Running Locally

### Prerequisites

- Python 3.11+
- PostgreSQL database (AWS RDS)
- AWS S3 bucket configured
- AWS SQS queue configured
- Environment variables configured

### Installation

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set environment variables (create `.env` file)

4. Run the worker:
```bash
python main.py
```

## Docker

Build the image:
```bash
docker build -t data-ingestion-worker .
```

Run the container:
```bash
docker run --env-file .env data-ingestion-worker
```

## Logging

The worker uses structured JSON logging compatible with CloudWatch Logs. Logs include:

- Flow type (INITIAL_PROCESSING or REPROCESSING)
- Job ID
- Processing statistics
- Error details

Example log entry:
```json
{
  "timestamp": "2024-01-01T12:00:00Z",
  "level": "INFO",
  "logger": "src.processor",
  "message": "Processing job - INITIAL PROCESSING flow",
  "job_id": 123,
  "flow_type": "INITIAL_PROCESSING"
}
```

## Database Models

The worker uses the following models:

- `Job`: Job lifecycle and metadata
- `Staging`: Imported rows before finalization
- `Issue`: Validation issues requiring user input
- `IssueItem`: Links issues to staging records
- `Contact`: Finalized contact records

See `Database_Create.SQL` for complete schema.

## Validation Rules

The worker validates rows according to:

1. **Required Fields**: email, first_name, last_name, company must be present
2. **Email Format**: Valid email format (RFC 5322 compliant)
3. **Duplicate Email**: Same email with different identities in CSV
4. **Existing Email**: Email already exists in contacts table

## Status Flow

Job status transitions:

- `PENDING` → `PROCESSING` → `NEEDS_REVIEW` (if issues) or `COMPLETED` (if no issues)
- `PENDING` → `PROCESSING` → `FAILED` (on error)

Staging status values:

- `READY`: Valid and ready for consolidation
- `ISSUE`: Has validation issues
- `DISCARD`: User decided to discard (skipped in reprocessing)
- `SUCCESS`: Successfully consolidated to contacts
