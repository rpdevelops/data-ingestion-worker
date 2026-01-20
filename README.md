# Data Ingestion Worker

Asynchronous worker service for processing CSV file imports.

## Overview

The worker consumes messages from AWS SQS queue and processes CSV files stored in S3. It handles both initial processing and reprocessing flows:

- **Initial Processing**: First time processing a CSV file - reads from S3, validates rows, creates staging records
- **Reprocessing**: When user resolves issues and triggers reprocessing - validates existing staging records without re-reading CSV

## Key Features

- **Idempotent Processing**: Uses row hashes to prevent duplicate processing
- **Restart-Safe**: Worker can be safely restarted - resumes from where it stopped
- **Dual Flow Support**: Same processor handles both initial and reprocessing
- **Progress Tracking**: Updates `job_processed_rows` periodically during processing
- **Issue Management**: Counts only unresolved issues for decision making
- **User Isolation**: Each user's contacts isolated via `contacts_user_id`
- **CSV Handling**: Multiple encoding support and automatic delimiter detection

## Processing Flows

### Routing Decision Logic

The worker determines which flow to use based on job status and staging records:

1. **Job status = COMPLETED** → Skip processing (job already finalized)
2. **Job status = NEEDS_REVIEW + has staging** → REPROCESSING flow (user resolved issues)
3. **Job status = PROCESSING/PENDING + has staging** → INITIAL PROCESSING flow (resume after restart)
4. **No staging records** → INITIAL PROCESSING flow (first time processing)

### Initial Processing Flow

1. Read CSV file from S3 (with encoding/delimiter detection)
2. Pre-process: identify duplicate emails within CSV
3. Pre-load existing emails from contacts table (filtered by user_id)
4. For each row:
   - Generate deterministic row hash
   - Check idempotency (skip if already processed via hash)
   - Create staging record
   - Validate row
   - Create issues if validation fails
   - Mark staging as READY if valid
   - Update progress periodically (every N rows)
5. Count unresolved issues
6. If unresolved issues found → set job status to NEEDS_REVIEW
7. If no unresolved issues → proceed to consolidation

### Reprocessing Flow

1. Get all staging records for job
2. Pre-load existing emails from contacts table (filtered by user_id)
3. Identify duplicate emails within staging records (excluding DISCARD)
4. For each staging record:
   - Skip if status is DISCARD (user decision)
   - Validate using staging record data (no CSV read)
   - Update status: READY if valid, ISSUE if invalid
   - Mark issues as resolved if all related staging records are resolved
   - Mark issues as unresolved if new problems appear
   - Update progress periodically (every N rows)
5. Count unresolved issues
6. If unresolved issues found → set job status to NEEDS_REVIEW
7. If no unresolved issues → proceed to consolidation

### Consolidation Phase

Consolidates staging records with READY status to the contacts table:

1. Get all staging records with READY status
2. Batch create contacts from staging records (with user_id from job)
3. Update staging records to SUCCESS
4. Update job status to COMPLETED
5. All operations are atomic (transaction)

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
SQS_VISIBILITY_TIMEOUT=300  # 5 minutes - message stays invisible while being processed

# Processing
MAX_RETRIES=3
RETRY_DELAY_SECONDS=5
PROGRESS_UPDATE_INTERVAL=10  # Update job_processed_rows every N rows

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

Build and run:
```bash
docker build -t data-ingestion-worker .
docker run --env-file .env data-ingestion-worker
```

## Important Notes

### Worker Restart and SQS Visibility Timeout

When the worker is restarted while processing a job:
- The previous message may still be invisible in SQS (visibility timeout = 300s = 5 minutes)
- The worker will wait until the message becomes visible again (up to 5 minutes delay)
- Once visible, the worker resumes processing from where it stopped
- Row hashes ensure no duplicate processing occurs

**To reduce restart delay in development:** Set `SQS_VISIBILITY_TIMEOUT=60`

### Progress Tracking

The worker updates `job_processed_rows` every N rows (configurable via `PROGRESS_UPDATE_INTERVAL`):
- Includes both new rows processed and skipped rows (already processed in previous run)
- After completion, `job_processed_rows` = `job_total_rows` (all CSV rows accounted for)

### Issue Counting

The worker counts **unresolved issues** (`issue_resolved = false`) for decision making:
- `job_issue_count` = number of unresolved issues
- If `unresolved_issues > 0` → status set to NEEDS_REVIEW
- If `unresolved_issues = 0` → proceeds to consolidation
- Resolved issues are kept in database for history but don't block consolidation

### CSV Handling

**Multiple encoding support** (tried in order):
- utf-8, latin-1, cp1252, iso-8859-1, windows-1252

**Automatic delimiter detection** (tried in order):
- Semicolon (`;`) - Common in European/Portuguese CSVs (prioritized)
- Comma (`,`) - Standard CSV format
- Tab (`\t`) - TSV format

The first combination that produces valid results (multiple fields with valid field names) is used. Empty fields from trailing delimiters are automatically cleaned.

### User Isolation

- Email validation checks only emails from the same user (`contacts_user_id`)
- Same email can exist for different users
- Prevents cross-user data leakage

## Validation Rules

The worker validates rows according to:

1. **Required Fields**: email, first_name, last_name, company must be present
2. **Email Format**: Valid email format (RFC 5322 compliant)
3. **Duplicate Email**: Same email appears multiple times in CSV (any occurrence > 1)
4. **Existing Email**: Email already exists in contacts table for the same user

## Status Flow

**Job status transitions:**
- `PENDING` → `PROCESSING` → `NEEDS_REVIEW` (if unresolved issues) or `COMPLETED` (if no unresolved issues)
- `PENDING` → `PROCESSING` → `FAILED` (on error)
- `NEEDS_REVIEW` → `PROCESSING` (reprocessing) → `NEEDS_REVIEW` (if still has unresolved issues) or `COMPLETED` (if all resolved)

**Staging status values:**
- `READY`: Valid and ready for consolidation
- `ISSUE`: Has validation issues
- `DISCARD`: User decided to discard (skipped in reprocessing)
- `SUCCESS`: Successfully consolidated to contacts

**Issue resolution:**
- `issue_resolved = false`: Issue needs user attention
- `issue_resolved = true`: Issue resolved, kept in database for history
- Only unresolved issues count toward `job_issue_count`

## Database Models

- `Job`: Job lifecycle and metadata
- `Staging`: Imported rows before finalization (includes `staging_row_hash` for idempotency)
- `Issue`: Validation issues requiring user input (includes `issue_resolved` flag)
- `IssueItem`: Links issues to staging records (many-to-many)
- `Contact`: Finalized contact records (includes `contacts_user_id` for user isolation)


## Logging

The worker uses structured JSON logging compatible with CloudWatch Logs. Logs include:
- Flow type (INITIAL_PROCESSING or REPROCESSING)
- Job ID and processing statistics
- Issue statistics (new_issues_this_run, total_issues, unresolved_issues)
- Progress updates with percentages
- Error details

## Error Handling

- Jobs that fail are marked with status `FAILED`
- Failed messages remain in SQS queue for retry (until DLQ after max retries)
- Worker logs detailed error information for debugging
- Job not found: logs warning and skips (message deleted from queue)

## Design Decisions

### Why Row Hashes for Idempotency?

Deterministic hash based on job_id, row_number, and row content allows safe restarts without duplicate processing. Database constraint enforces uniqueness.

### Why Count Unresolved Issues Instead of All Issues?

Resolved issues should not block consolidation. History of resolved issues is valuable for auditing. Clear separation between current problems and historical issues.

### Why Reprocess Staging Instead of Re-reading CSV?

Faster processing (no S3 download needed). User may have corrected data in staging directly. Respects user's decisions (DISCARD status). Validates current state, not original CSV.

### Why Multiple Encoding Support?

CSV files come from various sources with different encodings. Portuguese/Brazilian systems often use latin-1 or cp1252. Automatic detection prevents "empty fields" errors.

### Why Semicolon Delimiter Priority?

Common in European/Portuguese CSV exports. Excel exports often use semicolon for Portuguese locale. Detecting it first improves user experience.

### Why Progress Updates During Processing?

Users can track long-running jobs in real-time. Helps with UX and user confidence. Allows frontend to show progress bars.

### Why User Isolation in Contacts?

Multi-tenant architecture requirement. Same email can be valid for different users/organizations. Prevents cross-user data leakage.

## Case Studies & Fixes

### Worker Restart During Processing

**Problem**: Worker stopped mid-processing (32/100 rows). After restart, only processed 32 staging records and marked job as COMPLETED.

**Solution**: Implemented status-based routing - `PROCESSING/PENDING` + staging → continue INITIAL PROCESSING (read CSV, skip processed via hash). Hash-based idempotency ensures skipped rows are not reprocessed.

### Incorrect Progress Count After Restart

**Problem**: `job_processed_rows` showed 70/100 (only counted new rows, not skipped ones).

**Solution**: Added `skipped_count` tracking. Progress updates use `processed_count + skipped_count`. Final update uses `len(rows)` to ensure accuracy.

### Incorrect Issue Count

**Problem**: `job_issue_count` counted staging records with ISSUE (or all issues including resolved ones) instead of unresolved issues.

**Solution**: Count issues from `issues` table, filtering by `issue_resolved = false`. An issue can affect multiple staging records (e.g., duplicate emails).

### CSV Importing with Null Fields

**Problem**: CSV with semicolon delimiter was importing all fields as null.

**Solution**: Automatic delimiter detection (tries `;`, `,`, `\t` in order). Validates delimiter produces multiple fields with valid field names. Cleans empty fields from trailing delimiters.


## Known Limitations

- CSV file size limited by memory (not streaming)
- No parallel processing of rows (sequential for simplicity)
