# Data Ingestion Worker

Asynchronous worker service for processing CSV file imports.

> **Main Documentation**: See [data-ingestion-tool](https://github.com/rpdevelops/data-ingestion-tool) for architecture overview and system flow.

---

## Overview

The worker consumes messages from AWS SQS and processes CSV files stored in S3. It handles:

- **Initial Processing**: First-time CSV processing with row validation
- **Reprocessing**: Re-validation after user resolves issues

---

## Quick Start

### Prerequisites

- Python 3.11+
- AWS credentials configured
- Environment variables set

### Run Locally

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```

### Docker

```bash
docker build -t data-ingestion-worker .
docker run --env-file .env data-ingestion-worker
```

---

## Environment Variables

```bash
# Database
DATABASE_URL=postgresql://user:password@host:port/database

# AWS S3
CSV_BUCKET_NAME=your-bucket-name
AWS_REGION=us-east-1

# AWS SQS
SQS_QUEUE_URL=https://sqs.region.amazonaws.com/account/queue-name
SQS_VISIBILITY_TIMEOUT=300

# Processing
PROGRESS_UPDATE_INTERVAL=10

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

---

## Processing Flows

### Routing Decision

| Condition | Flow |
|-----------|------|
| Status = COMPLETED | Skip (already finalized) |
| Status = NEEDS_REVIEW + has staging | Reprocessing |
| Status = PROCESSING/PENDING + has staging | Resume initial processing |
| No staging records | Initial processing |

### Initial Processing

1. Download CSV from S3
2. Detect encoding and delimiter
3. Identify duplicate emails within CSV
4. Pre-load existing contacts for email validation
5. For each row:
   - Generate deterministic row hash
   - Skip if already processed (idempotency)
   - Create staging record
   - Validate row
   - Create issues if validation fails
6. Set job status based on issue count

### Reprocessing

1. Load existing staging records
2. Skip DISCARD rows (user decision)
3. Re-validate remaining rows
4. Update issue resolution status
5. Proceed to consolidation if no issues

### Consolidation

Atomic transaction:
1. Insert READY staging rows into contacts
2. Update staging status to SUCCESS
3. Set job status to COMPLETED

---

## Validation Rules

| Order | Validation | Issue Type |
|-------|------------|------------|
| 1 | Required fields present | MISSING_REQUIRED_FIELD |
| 2 | Email format valid | INVALID_EMAIL |
| 3 | Email not duplicate in CSV | DUPLICATE_EMAIL |
| 4 | Email not in existing contacts | EXISTING_EMAIL |

---

## CSV Handling

**Encodings** (tried in order):
- UTF-8, Latin-1, CP1252, ISO-8859-1, Windows-1252

**Delimiters** (tried in order):
- Semicolon (`;`), Comma (`,`), Tab (`\t`)

---

## Idempotency

- **Row Hash**: `hash(job_id, row_number, row_content)`
- **Database Constraint**: `UNIQUE (staging_job_id, staging_row_hash)`
- **Safe Retries**: Worker can restart without duplicate processing

---

## Architectural Decision Records (ADRs)

### ADR-001: Row Hashes for Idempotency

**Decision**: Use deterministic hashes instead of sequential IDs.

**Rationale**:
- Enables safe restarts and retries
- Database constraint enforces uniqueness
- No duplicate processing on worker crash

---

### ADR-002: Count Unresolved Issues Only

**Decision**: `job_issue_count` counts only unresolved issues.

**Rationale**:
- Resolved issues should not block consolidation
- History preserved for auditing
- Clear separation: current problems vs historical

---

### ADR-003: Reprocess from Staging

**Decision**: Reprocessing uses staging records, not CSV re-read.

**Rationale**:
- Faster (no S3 download)
- Respects user edits to staging data
- Honors DISCARD decisions

---

### ADR-004: Semicolon Delimiter Priority

**Decision**: Try semicolon before comma.

**Rationale**:
- Common in European/Portuguese CSV exports
- Excel uses semicolon for non-English locales

---

### ADR-005: User Isolation

**Decision**: Filter contacts by `contacts_user_id`.

**Rationale**:
- Multi-tenant architecture
- Same email valid for different users
- Prevents cross-user data leakage

---

## Troubleshooting

### Worker Restart Delay

**Symptom**: Worker waits up to 5 minutes after restart.

**Cause**: SQS visibility timeout (message still invisible).

**Solution**: Reduce `SQS_VISIBILITY_TIMEOUT` for development.

### Incorrect Progress Count

**Symptom**: Progress shows fewer rows than total.

**Cause**: Only counting new rows, not skipped.

**Solution**: Fixed - progress includes skipped rows.

### CSV Fields Null

**Symptom**: All fields imported as null.

**Cause**: Wrong delimiter detection.

**Solution**: Fixed - automatic delimiter detection with validation.

---

## Limitations

- CSV loaded into memory (not streaming)
- Sequential row processing (no parallelism)

---

## Related Repositories

- [data-ingestion-tool](https://github.com/rpdevelops/data-ingestion-tool) - Main documentation
- [data-ingestion-backend](https://github.com/rpdevelops/data-ingestion-backend) - FastAPI API
- [data-ingestion-frontend](https://github.com/rpdevelops/data-ingestion-frontend) - Next.js UI
- [data-ingestion-infra](https://github.com/rpdevelops/data-ingestion-infra) - Terraform IaC
