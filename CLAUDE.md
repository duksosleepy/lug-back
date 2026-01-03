# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**lug-back** is a FastAPI-based data processing server for LUG, handling warranty registrations, sales data synchronization from Sapo e-commerce platform, bank statement processing, and CRM integrations. The application processes Excel files containing sales/warranty data and synchronizes information across multiple systems.

## Development Commands

### Running the Application

```bash
# Run the main FastAPI server (default: http://127.0.0.1:8000)
uv run src/main.py

# Run Celery worker for background tasks
uv run celery -A src.tasks.worker:celery_app worker --loglevel=info --concurrency=4

# Run Celery beat scheduler for periodic tasks
uv run celery -A src.tasks.worker:celery_app beat --loglevel=info
```

### Celery Task Management

```bash
# Check scheduled tasks
uv run celery -A src.tasks.worker:celery_app inspect scheduled

# Check active tasks
uv run celery -A src.tasks.worker:celery_app inspect active
```

### Development Tools

```bash
# Type checking with basedpyright
uv run basedpyright

# Code formatting and linting with ruff
uv run ruff check .
uv run ruff format .
```

### Package Management

- **Always use `uv`**, never `pip`
- Install packages: `uv add package-name`
- Install dev dependencies: `uv add --dev package-name`
- Run commands: `uv run command`

## Architecture Overview

### Core Modules

**src/api/server.py**
- Main FastAPI application entry point
- Defines API endpoints for file processing, warranty registration, and Sapo sync
- Handles CORS, logging middleware, and startup events
- Key endpoints:
  - `/process/online` - Process online sales Excel files
  - `/process/offline` - Process offline sales Excel files
  - `/process/mapping` - Map product codes/names using mapping files
  - `/warranty` - Handle warranty registration submissions
  - `/sapo/sync` - Trigger Sapo data synchronization
  - `/accounting/*` - Bank statement processing endpoints

**src/tasks/worker.py**
- Celery task definitions and configuration
- Background job processing with Redis as broker/backend
- Scheduled tasks:
  - `sync_pending_registrations`: Runs daily at 2:00 AM, syncs pending warranty registrations
  - `daily_sapo_sync`: Runs daily at 00:30 AM, syncs Sapo data from Dec 31 of previous year
  - `crm-data-sync`: Runs daily at 15:45, syncs CRM data

### Data Processing Pipeline

**Processors (src/processors/)**
- `online_processor.py` - Processes online sales data with Dask for parallel processing
- `offline_processor.py` - Processes offline sales data
- `product_mapping_processor.py` - Maps old product codes/names to new ones using mapping files

All processors:
1. Read Excel input (uses Dask for large files)
2. Validate and clean data (phone numbers, product codes)
3. Filter out excluded customers/products based on business rules
4. Return valid/invalid data separately
5. Invalid records are emailed to configured recipients

**Phone Number Validation**
- Phone validation is critical throughout the system
- Uses `src/util/phone_utils.py` for standardization
- Frontend and backend use identical validation regex
- Valid formats: Vietnamese mobile numbers with proper country code handling

### External Integrations

**Sapo E-commerce Sync (src/sapo_sync/)**
- Syncs order data from two Sapo domains: mysapo.net and mysapogo.com
- Fetches orders within date ranges and writes to Google Sheets
- Uses Google Sheets API for data storage
- Handles cancel reasons initialization for mysapogo.com

**CRM Integration (src/crm/)**
- `flow.py` - Main CRM data flow orchestration with complex filtering rules
- `tasks.py` - Celery tasks for scheduled CRM sync
- Handles bidirectional sync between internal systems and external CRM
- Supports batch processing via HTTP API with fallback ports
- Product mapping using warranty rules Excel file

**Accounting Module (src/accounting/)**
- Processes bank statements from multiple Vietnamese banks
- Uses SQLite database (`banking_enterprise.db`) for caching and lookups
- Employs vector search (Tantivy) and ML (fastembed) for counterparty matching
- Supports VCB, MBBank, and other banks with different export formats
- Handles HTML, XML, and standard Excel formats

### Settings & Configuration

**Environment-based Configuration (src/settings/)**
- `base.py` - Base settings class with environment variable utilities
- `app.py` - Application settings (CORS, debug mode, data directory)
- `email.py` - SMTP email configuration
- `google.py` - Google API credentials (supports file path or base64 encoding)
- `sapo.py` - Sapo API credentials and spreadsheet configuration
- `sentry.py` - Error tracking configuration

Configuration is loaded from:
1. `.env` file (primary source)
2. Environment variables
3. Default values in code

**Critical Environment Variables:**
- `REDIS_URL` - Redis connection for Celery (default: redis://localhost:6379)
- `XC_TOKEN` - API token for NocoDB/XC-DB integration
- `CRM_BATCH_URL` - CRM batch service endpoint
- `CRM_BATCH_FALLBACK_PORTS` - Comma-separated ports for CRM failover
- `GOOGLE_CREDENTIALS_PATH` or `GOOGLE_CREDENTIALS_B64` - Google API auth
- `SMTP_*` - Email configuration (required for error notifications)

### Warranty Registration Flow

The warranty system has a unique bidirectional matching flow:

**Forward Match (User → System)**:
1. User submits warranty registration via `/warranty` endpoint
2. System searches for order code in main table (`mtvvlryi3xc0gqd`)
3. If found: Copy to warranty table, send to CRM, email notification, delete from main
4. If not found: Save to pending table (`mydoap8edbr206g`)

**Reverse Match (System → User)**:
1. Celery task `sync_pending_registrations` runs daily at 2:00 AM
2. Checks each pending registration against newly arrived order data
3. When match found: Complete warranty registration, send to CRM, cleanup

This allows customers to register before their order data arrives in the system.

### Logging & Error Handling

- Uses **Loguru** for application-wide logging (replaces standard logging)
- `src/util/logging/` - Centralized logging configuration
- Supports JSON logs, file output, and structured logging
- Sentry integration for production error tracking (optional via env vars)
- Non-reported exceptions can be configured via `SENTRY_NON_REPORTED_EXCEPTIONS`

### Data Validation Rules

**Customer Exclusions:**
- Customers containing "BƯU ĐIỆN" are filtered out

**Product Code Exclusions (Online):**
- `PBHDT`, `THUNG`, `DVVC_ONL`, `TUINILONPK`, `THECAO`, `TUI GIAY LON`, etc.

**Product Name Exclusions:**
- Products containing "BAO LÌ XÌ"

**Document Type Exclusions:**
- Document type `TLO` (for online processing)
- Product type `VPP` (văn phòng phẩm)

These rules are defined in both processors and CRM flow modules - keep them synchronized.

## Coding Standards (from SKILL.md)

- Follow PEP8 and write Google-style docstrings
- Use type hints (PEP 585 built-in generics, not `typing` module)
- snake_case for variables/functions/databases, PascalCase for classes
- API JSON uses camelCase (convert via Pydantic aliases)
- One function = one responsibility
- Remove all unused code immediately (no "deprecated" remnants)
- Prefer non-copyleft licenses for new dependencies

## Common Patterns

### Processing Excel Files

```python
# All processors follow this pattern:
from io import BytesIO
from processors.online_processor import DaskExcelProcessor

input_buffer = BytesIO(file_content)
processor = DaskExcelProcessor(input_buffer)
output_buffer = BytesIO()

# Returns: (valid_content, invalid_content, kl_records_json, invalid_count)
result = processor.process_to_buffer(output_buffer)
```

### Async HTTP Calls in Sync Context

```python
# For Celery tasks (sync context) calling async APIs:
import asyncio

async def async_operation():
    async with httpx.AsyncClient() as client:
        return await client.get(url)

# In sync task:
result = asyncio.run(async_operation())
```

### Error Email Notifications

```python
from src.util.mail_client import EmailClient
from src.util.send_email import load_config

config = load_config()
with EmailClient(**config) as client:
    msg = client.create_message(to=recipients, subject=subject, body=body, html=True)
    msg = client.attach_file(msg, file_path)
    client.send(msg)
```

## Database Schema

**SQLite (accounting module):**
- `banking_enterprise.db` - Contains lookup tables for accounts, departments, POS terminals
- Tables: Account, Department, Counterparty, POSTerminal, BankMID

**External APIs (NocoDB/XC-DB):**
- `mtvvlryi3xc0gqd` - Main order data table (temporary storage)
- `mffwo1asni22n9z` - Warranty registrations table (permanent)
- `miyw4f4yeojamv6` - Warranty submission tracking
- `mydoap8edbr206g` - Pending registrations (waiting for order data)

## Important Notes

- **Redis is required** for Celery to function (both worker and beat)
- **Google Sheets API** is used extensively - credentials must be configured
- **Email configuration** is required for error notifications to work
- **Phone number validation** happens on both frontend and backend with identical regex
- **CRM batch service** has dynamic port fallback mechanism (check `CRM_BATCH_FALLBACK_PORTS`)
- **Dask** is used for parallel processing of large Excel files
- **Tantivy + fastembed** provide vector search for bank counterparty matching
- All datetime handling uses Asia/Ho_Chi_Minh timezone
- Task timeouts: hard limit 30min (1800s), soft limit 25min (1500s)
