# LocalDevDriver and Debugger Tools

This directory contains the LocalDevDriver (LDDD) - a SQLite-backed development driver for local scraper development and debugging.

## Overview

The LocalDevDriver provides:
- **Resumability**: Graceful shutdown and restart without losing progress
- **Full HTTP Archive**: WARC-exportable request/response capture
- **Intelligent Compression**: Zstandard with per-continuation trained dictionaries
- **Rate Limiting**: Configurable base delay with randomized jitter
- **Rich Statistics**: Queue depth, throughput, and result analytics
- **Debugging Tools**: CLI and Web UI for inspecting and manipulating runs

## LocalDevDriverDebugger (LDDD)

The `LocalDevDriverDebugger` class in `debugger.py` provides a high-level API for inspecting and manipulating scraper run databases without requiring the full LocalDevDriver runtime.

### Key Features

- **Read-only inspection**: Safe for analyzing running or completed runs
- **Write operations**: Requeue, cancel, and resolve errors
- **Connection management**: Async context manager with read-only mode enforcement
- **Export capabilities**: JSONL and WARC export formats

### Python API Usage

```python
from kent.driver.dev_driver.debugger import LocalDevDriverDebugger

# Read-only inspection
async with LocalDevDriverDebugger.open("run.db", read_only=True) as debugger:
    # Get run metadata and statistics
    metadata = await debugger.get_run_metadata()
    stats = await debugger.get_stats()

    # List requests with filtering
    failed_requests = await debugger.list_requests(status='failed')

    # Inspect errors
    errors = await debugger.list_errors(is_resolved=False)
    error_summary = await debugger.get_error_summary()

# Write operations (manipulation)
async with LocalDevDriverDebugger.open("run.db", read_only=False) as debugger:
    # Requeue a failed request
    new_id = await debugger.requeue_request(request_id=123)

    # Cancel pending requests
    cancelled = await debugger.cancel_request(request_id=456)

    # Resolve or requeue errors
    await debugger.resolve_error(error_id=789, resolution_notes="Fixed XPath")
    new_id = await debugger.requeue_error(error_id=790)
```

## CLI Tool: `ldd-debug`

The `ldd-debug` command-line tool provides a convenient interface for inspecting and manipulating LocalDevDriver run databases.

### Installation

The CLI is automatically available after installing juriscraper:

```bash
pip install juriscraper
ldd-debug --help
```

### Common Commands

#### Show Run Information

```bash
# Display run metadata and statistics
ldd-debug info run.db

# Output as JSON
ldd-debug info run.db --format json
```

#### Inspect Requests

```bash
# List all requests
ldd-debug requests list run.db

# Filter by status
ldd-debug requests list run.db --status failed

# Filter by continuation (step name)
ldd-debug requests list run.db --continuation parse_opinions --limit 50

# Show specific request details
ldd-debug requests show run.db 123

# Show request summary by status and continuation
ldd-debug requests summary run.db
```

#### Inspect Responses

```bash
# List responses
ldd-debug responses list run.db

# Filter by continuation
ldd-debug responses list run.db --continuation parse_opinions

# Show response details
ldd-debug responses show run.db 456

# Get response content (HTML/JSON/etc)
ldd-debug responses content run.db 456
ldd-debug responses content run.db 456 -o response.html
```

#### Inspect Errors

```bash
# List all errors
ldd-debug errors list run.db

# Filter unresolved errors
ldd-debug errors list run.db --unresolved

# Filter by error type
ldd-debug errors list run.db --type xpath

# Show error details
ldd-debug errors show run.db 789

# Show error summary
ldd-debug errors summary run.db

# Resolve an error
ldd-debug errors resolve run.db 789 --notes "Fixed XPath selector"

# Requeue an error (creates new request)
ldd-debug errors requeue run.db 789 --notes "Server issue resolved"
```

#### Inspect Results

```bash
# List all results
ldd-debug results list run.db

# Filter by result type
ldd-debug results list run.db --type CourtOpinion

# Filter valid/invalid results
ldd-debug results list run.db --valid
ldd-debug results list run.db --invalid

# Show result details
ldd-debug results show run.db 101

# Show result summary by type
ldd-debug results summary run.db
```

#### Requeue Operations

```bash
# Requeue a specific request
ldd-debug requeue request run.db 123

# Requeue without clearing downstream data
ldd-debug requeue request run.db 123 --no-clear-downstream

# Requeue all completed requests for a continuation
ldd-debug requeue continuation run.db parse_opinions

# Requeue all failed requests for a continuation
ldd-debug requeue continuation run.db parse_opinions --status failed

# Batch requeue errors by type
ldd-debug requeue errors run.db --type xpath

# Batch requeue errors by continuation
ldd-debug requeue errors run.db --continuation parse_opinions
```

#### Cancel Operations

```bash
# Cancel a pending request
ldd-debug cancel request run.db 123

# Cancel all pending requests for a continuation
ldd-debug cancel continuation run.db parse_opinions
```

#### Compression Management

```bash
# Show compression statistics
ldd-debug compression stats run.db

# Train a new compression dictionary
ldd-debug compression train run.db parse_opinions

# Train with custom sample count
ldd-debug compression train run.db parse_opinions --samples 500

# Recompress responses with latest dictionary
ldd-debug compression recompress run.db parse_opinions

# Recompress with specific dictionary ID
ldd-debug compression recompress run.db parse_opinions --dict-id 5
```

#### Debugging

```bash
# Diagnose an error by re-running XPath observation
ldd-debug diagnose run.db 789

# Output as JSON for further processing
ldd-debug diagnose run.db 789 --format json
```

#### Export Operations

```bash
# Export results to JSONL format
ldd-debug export jsonl run.db results.jsonl

# Export specific result type
ldd-debug export jsonl run.db opinions.jsonl --type CourtOpinion

# Export only valid results
ldd-debug export jsonl run.db valid_results.jsonl --valid

# Export responses to WARC format (compressed by default)
ldd-debug export warc run.db archive.warc.gz

# Export uncompressed WARC
ldd-debug export warc run.db archive.warc --no-compress

# Export specific continuation to WARC
ldd-debug export warc run.db step1.warc.gz --continuation parse_opinions
```

### Output Formats

All inspection commands support three output formats:

- `--format table` (default): Human-readable table format
- `--format json`: Pretty-printed JSON
- `--format jsonl`: Newline-delimited JSON (one object per line)

Example:

```bash
# Table format (default)
ldd-debug requests list run.db

# JSON format
ldd-debug requests list run.db --format json

# JSONL format (useful for streaming/processing)
ldd-debug requests list run.db --format jsonl | jq .
```

## Web UI

The LocalDevDriver also includes a FastAPI-based web UI for browsing runs. See `web/app.py` for details.

To start the web UI:

```bash
# From your scraper code
from kent.driver.dev_driver.web.app import create_app

app = create_app(db_path="run.db")
# Then run with uvicorn, gunicorn, etc.
```

## Architecture

The LocalDevDriver uses SQLite for persistent storage with the following key tables:

- `requests`: HTTP requests and queue management
- `responses`: HTTP responses with compressed content
- `results`: Parsed results from scrapers
- `errors`: Extraction and validation errors
- `compression_dicts`: Zstandard compression dictionaries
- `rate_limiter_state`: Rate limiter state for resumability

See `dev_driver_design.md` for full schema details.

## See Also

- `debugger.py`: LocalDevDriverDebugger class with full Python API
- `cli.py`: Command-line interface implementation
- `dev_driver_design.md`: Full architecture and design documentation
- `DEBUG_WANTS.md`: Wishlist for future debugging features
