# LocalDevDriver Design Specification

## Overview

The LocalDevDriver is a SQLite-backed subclass of AsyncDriver designed for local development and debugging of scrapers. It provides:

1. **Resumability** - Graceful shutdown and restart without losing progress
2. **Full HTTP Archive** - WARC-exportable request/response capture
3. **Intelligent Compression** - Zstandard with per-continuation trained dictionaries
4. **Rate Limiting** - 10s base delay with ±2s randomized jitter
5. **Statistics** - Queue depth, throughput, and result analytics

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        LocalDevDriver                               │
│  (subclass of AsyncDriver)                                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │ Rate Limiter │    │ SQLite DB    │    │ Compression          │  │
│  │ (pyrate)     │    │ (aiosqlite)  │    │ Manager              │  │
│  │              │    │              │    │                      │  │
│  │ • 10s base   │    │ • requests   │    │ • zstd dictionaries  │  │
│  │ • ±2s jitter │    │ • responses  │    │ • per-continuation   │  │
│  │              │    │ • queue      │    │ • versioned          │  │
│  │              │    │ • results    │    │                      │  │
│  └──────────────┘    └──────────────┘    └──────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Database Schema

### Table: `requests`

Stores all HTTP requests, including pending queue items.

```sql
CREATE TABLE requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Queue management
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, in_progress, completed, failed, held
    priority INTEGER NOT NULL DEFAULT 9,
    queue_counter INTEGER NOT NULL,          -- For FIFO within same priority

    -- HTTP Request
    method TEXT NOT NULL,
    url TEXT NOT NULL,
    headers_json TEXT,                       -- JSON-encoded headers
    cookies_json TEXT,                       -- JSON-encoded cookies
    body BLOB,                               -- Request body (if any)

    -- Scraper context
    continuation TEXT NOT NULL,              -- Method name to call with response
    current_location TEXT NOT NULL DEFAULT '',
    accumulated_data_json TEXT,              -- JSON-encoded accumulated data
    aux_data_json TEXT,                      -- JSON-encoded aux data
    permanent_json TEXT,                     -- JSON-encoded permanent data
    deduplication_key TEXT,                  -- For duplicate detection

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,

    -- Retry tracking (exponential backoff)
    retry_count INTEGER NOT NULL DEFAULT 0,
    cumulative_backoff REAL NOT NULL DEFAULT 0.0,  -- Total backoff time accumulated
    next_retry_delay REAL,                   -- Next backoff delay (base_delay * 2^retry_count)
    last_error TEXT,                         -- Last error message if failed

    -- Parent tracking
    parent_request_id INTEGER REFERENCES requests(id),

    -- Indexing
    UNIQUE(deduplication_key) ON CONFLICT IGNORE
);

CREATE INDEX idx_requests_status_priority ON requests(status, priority, queue_counter);
CREATE INDEX idx_requests_continuation ON requests(continuation);
CREATE INDEX idx_requests_deduplication ON requests(deduplication_key);
```

### Table: `responses`

Stores HTTP responses with compressed content.

```sql
CREATE TABLE responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL REFERENCES requests(id),

    -- HTTP Response
    status_code INTEGER NOT NULL,
    headers_json TEXT,                       -- JSON-encoded headers
    url TEXT NOT NULL,                       -- Final URL after redirects

    -- Content (compressed)
    content_compressed BLOB,                 -- Zstd-compressed content
    content_size_original INTEGER,           -- Original size for stats
    content_size_compressed INTEGER,         -- Compressed size

    -- Compression metadata
    compression_dict_id INTEGER REFERENCES compression_dicts(id),
    continuation TEXT NOT NULL,              -- For dictionary training grouping

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- WARC export metadata
    warc_record_id TEXT                      -- UUID for WARC record linking
);

CREATE INDEX idx_responses_request ON responses(request_id);
CREATE INDEX idx_responses_continuation ON responses(continuation);
CREATE INDEX idx_responses_dict ON responses(compression_dict_id);
```

### Table: `compression_dicts`

Stores versioned zstandard compression dictionaries.

```sql
CREATE TABLE compression_dicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    continuation TEXT NOT NULL,              -- Which continuation this dict is for
    version INTEGER NOT NULL,                -- Version number (incrementing)
    dictionary_data BLOB NOT NULL,           -- The zstd dictionary bytes
    sample_count INTEGER NOT NULL,           -- How many samples trained on
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(continuation, version)
);

CREATE INDEX idx_compression_dicts_continuation ON compression_dicts(continuation);
```

### Table: `results`

Stores scraped data results.

```sql
CREATE TABLE results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER REFERENCES requests(id),

    -- Result data
    result_type TEXT NOT NULL,               -- Pydantic model class name
    data_json TEXT NOT NULL,                 -- JSON-encoded result data

    -- Validation status
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_errors_json TEXT,             -- JSON-encoded validation errors if invalid

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_results_type ON results(result_type);
CREATE INDEX idx_results_request ON results(request_id);
```

### Table: `archived_files`

Tracks downloaded files from ArchiveRequests.

```sql
CREATE TABLE archived_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL REFERENCES requests(id),

    -- File info
    file_path TEXT NOT NULL,                 -- Path relative to storage_dir
    original_url TEXT NOT NULL,              -- URL the file was downloaded from
    expected_type TEXT,                      -- "pdf", "audio", etc.
    file_size INTEGER,                       -- Size in bytes
    content_hash TEXT,                       -- SHA256 of file content for dedup

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_archived_files_request ON archived_files(request_id);
CREATE INDEX idx_archived_files_hash ON archived_files(content_hash);
```

### Table: `run_metadata`

Single row storing run configuration and state. One DB = one scraper run.

```sql
CREATE TABLE run_metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),   -- Enforce single row

    -- Scraper identity
    scraper_name TEXT NOT NULL,
    scraper_version TEXT,

    -- Run state
    status TEXT NOT NULL DEFAULT 'created',  -- created, running, completed, error, interrupted
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    error_message TEXT,

    -- Invocation parameters (immutable after creation)
    params_json TEXT,                        -- JSON-encoded ScraperParams filters
    base_delay REAL NOT NULL,
    jitter REAL NOT NULL,
    num_workers INTEGER NOT NULL,
    max_backoff_time REAL NOT NULL           -- Max cumulative backoff before marking failed
);
```

### Table: `errors`

Tracks all errors encountered during scraping.

```sql
CREATE TABLE errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER REFERENCES requests(id),

    -- Error classification
    error_type TEXT NOT NULL,                -- 'structural', 'validation', 'transient'
    error_class TEXT NOT NULL,               -- Full exception class name
    message TEXT NOT NULL,
    request_url TEXT NOT NULL,

    -- Structured error data (type-specific)
    context_json TEXT,                       -- JSON-encoded error context

    -- For structural errors (HTMLStructuralAssumptionException)
    selector TEXT,
    selector_type TEXT,                      -- 'xpath' or 'css'
    expected_min INTEGER,
    expected_max INTEGER,
    actual_count INTEGER,

    -- For validation errors (DataFormatAssumptionException)
    model_name TEXT,
    validation_errors_json TEXT,             -- JSON-encoded Pydantic errors
    failed_doc_json TEXT,                    -- JSON-encoded failed document

    -- For transient errors
    status_code INTEGER,                     -- For HTMLResponseAssumptionException
    timeout_seconds REAL,                    -- For RequestTimeoutException

    -- Resolution tracking
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolution_notes TEXT,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_errors_request ON errors(request_id);
CREATE INDEX idx_errors_type ON errors(error_type);
CREATE INDEX idx_errors_unresolved ON errors(is_resolved) WHERE is_resolved = FALSE;
```

## Core Components

### 1. AioSQLiteBucket Rate Limiter

An async SQLite-backed bucket for pyrate_limiter integration with persistent storage.

```python
class AioSQLiteBucket(AbstractBucket):
    """Async SQLite-backed bucket for pyrate_limiter.

    Implements the AbstractBucket interface using aiosqlite for persistent
    rate limiting state that survives process restarts.

    The bucket stores rate items in the rate_items table, allowing the
    rate limiter to track request timestamps across restarts.

    Args:
        db: Database connection (should be same as LocalDevDriver's).
        rates: List of Rate objects defining rate limits.

    Example:
        rates = [Rate(5, Duration.SECOND)]  # 5 requests per second
        bucket = AioSQLiteBucket(db, rates)
        limiter = Limiter(bucket)
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        rates: list[Rate],
    ) -> None: ...
```

### 2. LocalDevDriver Class

```python
class LocalDevDriver(AsyncDriver[ScraperReturnDatatype]):
    """SQLite-backed async driver for local development.

    Extends AsyncDriver with:
    - Persistent request queue in SQLite
    - Response archival with compression
    - Resumability from graceful shutdown
    - WARC export capability
    - Web interface integration via callbacks and async context manager

    Args:
        scraper: The scraper instance to run.
        db_path: Path to SQLite database file.
        storage_dir: Directory for downloaded files.
        base_delay: Base rate limit delay in seconds (default: 10.0).
        jitter: Rate limit jitter in seconds (default: 2.0).
        num_workers: Number of concurrent workers (default: 1).
        resume: If True, resume from existing queue state (default: True).
        max_backoff_time: Maximum total backoff time before marking failed (default: 3600.0).
            Retries start at base_delay and double each attempt until this limit is reached.

    Example (web interface usage):
        async with LocalDevDriver.open(scraper, db_path) as driver:
            driver.on_progress = lambda e: await websocket.send(e.to_json())
            await driver.run()
    """

    def __init__(
        self,
        scraper: BaseScraper[ScraperReturnDatatype],
        db_path: Path,
        storage_dir: Path | None = None,
        base_delay: float = 10.0,
        jitter: float = 2.0,
        num_workers: int = 1,
        resume: bool = True,
        max_backoff_time: float = 3600.0,
    ) -> None: ...

    # --- Async Context Manager ---

    @classmethod
    async def open(
        cls,
        scraper: BaseScraper[ScraperReturnDatatype],
        db_path: Path,
        **kwargs,
    ) -> AsyncContextManager["LocalDevDriver[ScraperReturnDatatype]"]:
        """Open driver as async context manager.

        Ensures proper cleanup of DB connections and workers on exit.

        Example:
            async with LocalDevDriver.open(scraper, db_path) as driver:
                await driver.run()
        """
        ...

    async def close(self) -> None:
        """Close DB connections and clean up resources.

        Called automatically when using async context manager.
        """
        ...

    # --- Progress Callbacks (for web interface) ---

    on_progress: Callable[[ProgressEvent], Awaitable[None]] | None = None
    """Callback invoked on every progress event.

    Set this to push real-time updates to a web interface.
    Events include: request_started, request_completed, request_failed,
    data_scraped, error_occurred, queue_changed, run_started, run_completed.
    """

    async def run(self) -> None:
        """Run the scraper, resuming from DB state if resume=True."""
        ...

    # --- Compression Dictionary Management ---

    async def train_compression_dict(
        self,
        continuation: str,
        sample_limit: int = 1000,
        dict_size: int = 112640,
    ) -> int:
        """Train a new compression dictionary for a continuation.

        Samples responses for the given continuation and creates a new
        versioned zstandard dictionary.

        Args:
            continuation: The continuation name to train for.
            sample_limit: Maximum responses to sample for training.
            dict_size: Target dictionary size in bytes (default: 110KB).
                       Larger dictionaries can improve compression but
                       use more memory. zstd recommends 100KB as a good
                       starting point.

        Returns:
            The new dictionary version number.
        """
        ...

    async def recompress_responses(
        self,
        continuation: str,
        batch_size: int = 100,
    ) -> int:
        """Recompress responses using the latest dictionary.

        Args:
            continuation: The continuation to recompress responses for.
            batch_size: Number of responses to process per batch.

        Returns:
            Number of responses recompressed.
        """
        ...

    # --- Status and Control ---

    async def status(self) -> Literal["unstarted", "in_progress", "done"]:
        """Check the current state of the scraper run.

        Returns:
            - "unstarted": No requests, responses, results, or errors in DB
            - "in_progress": Pending or in_progress requests exist in queue
            - "done": Queue empty but requests/responses/results exist
        """
        ...

    def stop(self) -> None:
        """Signal workers to stop after completing their current request.

        Sets the stop_event which causes workers to exit after their
        current request completes. Pending requests remain in the DB
        for later resume.

        This is a synchronous method that returns immediately - use
        run() to wait for workers to actually finish.
        """
        if self.stop_event:
            self.stop_event.set()

    # --- Statistics ---

    async def get_stats(self) -> DevDriverStats:
        """Get comprehensive statistics about the current state.

        Returns:
            DevDriverStats with queue depth, throughput, compression ratios, etc.
        """
        ...

    # --- WARC Export ---

    async def export_warc(
        self,
        output_path: Path,
        compress: bool = True,
    ) -> int:
        """Export all captured traffic to a WARC file.

        Args:
            output_path: Path to write WARC file.
            compress: If True, create .warc.gz (default: True).

        Returns:
            Number of records exported.
        """
        ...

    # --- Error Management ---

    async def list_errors(
        self,
        error_type: str | None = None,
        continuation: str | None = None,
        unresolved_only: bool = True,
        offset: int = 0,
        limit: int = 50,
    ) -> Page[ErrorRecord]:
        """List errors with optional filters and pagination.

        Args:
            error_type: Filter by type ('structural', 'validation', 'transient').
            continuation: Filter by continuation method name.
            unresolved_only: If True, only return unresolved errors.
            offset: Number of records to skip.
            limit: Maximum records to return.

        Returns:
            Paginated list of ErrorRecord objects with full error details.
        """
        ...

    async def get_error(self, error_id: int) -> ErrorRecord | None:
        """Get a single error by ID."""
        ...

    async def requeue_request(
        self,
        error_id: int,
        mark_resolved: bool = True,
        resolution_notes: str | None = None,
    ) -> int:
        """Re-queue the request that caused an error.

        Creates a new pending request based on the original request that
        caused the error. Useful after fixing scraper code to retry
        failed requests.

        Args:
            error_id: ID of the error whose request to requeue.
            mark_resolved: If True, mark the error as resolved.
            resolution_notes: Optional notes about the resolution.

        Returns:
            The new request ID.
        """
        ...

    async def requeue_errors_by_type(
        self,
        error_type: str,
        continuation: str | None = None,
        mark_resolved: bool = True,
    ) -> int:
        """Re-queue all requests that caused errors of a specific type.

        Batch operation to retry all requests that failed with a specific
        error type (e.g., all 'structural' errors for a specific continuation
        after fixing the selector).

        Args:
            error_type: Error type to filter ('structural', 'validation', 'transient').
            continuation: Optional filter by continuation method name.
            mark_resolved: If True, mark all requeued errors as resolved.

        Returns:
            Number of requests requeued.
        """
        ...

    async def mark_error_resolved(
        self,
        error_id: int,
        resolution_notes: str | None = None,
    ) -> None:
        """Mark an error as resolved without requeuing.

        Use this when the error doesn't need retry (e.g., expected validation
        failure, or data was manually corrected).

        Args:
            error_id: ID of the error to mark resolved.
            resolution_notes: Optional notes about the resolution.
        """
        ...

    # --- Listing and Query Methods (for web interface) ---

    async def list_requests(
        self,
        status: str | None = None,
        continuation: str | None = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Page[RequestRecord]:
        """List requests with optional filters and pagination.

        Args:
            status: Filter by status (pending, in_progress, completed, failed, held).
            continuation: Filter by continuation method name.
            offset: Number of records to skip.
            limit: Maximum records to return.

        Returns:
            Paginated list of RequestRecord objects.
        """
        ...

    async def list_responses(
        self,
        continuation: str | None = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Page[ResponseRecord]:
        """List responses with optional filters and pagination.

        Args:
            continuation: Filter by continuation method name.
            offset: Number of records to skip.
            limit: Maximum records to return.

        Returns:
            Paginated list of ResponseRecord objects.
        """
        ...

    async def list_results(
        self,
        result_type: str | None = None,
        is_valid: bool | None = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Page[ResultRecord]:
        """List scraped results with optional filters and pagination.

        Args:
            result_type: Filter by Pydantic model class name.
            is_valid: Filter by validation status.
            offset: Number of records to skip.
            limit: Maximum records to return.

        Returns:
            Paginated list of ResultRecord objects.
        """
        ...

    async def get_request(self, request_id: int) -> RequestRecord | None:
        """Get a single request by ID."""
        ...

    async def get_response(self, response_id: int) -> ResponseRecord | None:
        """Get a single response by ID."""
        ...

    async def get_response_content(self, response_id: int) -> bytes | None:
        """Get decompressed response content by ID.

        Returns the full response body, decompressed from zstd storage.
        """
        ...

    async def get_result(self, result_id: int) -> dict | None:
        """Get full result data by ID (not just preview)."""
        ...

    async def cancel_request(self, request_id: int) -> bool:
        """Cancel a pending request (remove from queue).

        Only pending requests can be cancelled. In-progress requests
        must complete or be stopped via stop_workers().

        Args:
            request_id: ID of the request to cancel.

        Returns:
            True if cancelled, False if not found or not pending.
        """
        ...

    async def cancel_requests_by_continuation(
        self,
        continuation: str,
    ) -> int:
        """Cancel all pending requests for a continuation.

        Useful when you know a continuation's logic is broken and
        don't want to waste time on queued requests.

        Args:
            continuation: The continuation method name.

        Returns:
            Number of requests cancelled.
        """
        ...

    # --- Step Control (pause/resume by continuation) ---

    async def pause_step(self, continuation: str) -> int:
        """Pause processing of requests for a specific continuation.

        Marks all pending requests for the given continuation as 'held'.
        Held requests are not picked up by workers but remain in the queue
        for later resume. Useful for temporarily stopping a problematic step
        while continuing to process other parts of the scraper.

        Args:
            continuation: The continuation method name to pause.

        Returns:
            Number of requests marked as held.
        """
        ...

    async def resume_step(self, continuation: str) -> int:
        """Resume processing of held requests for a specific continuation.

        Marks all held requests for the given continuation as 'pending',
        making them available for workers to process again.

        Args:
            continuation: The continuation method name to resume.

        Returns:
            Number of requests restored to pending.
        """
        ...
```

### 3. Statistics and Error Data Structures

```python
@dataclass
class QueueStats:
    """Statistics about the request queue."""
    total_pending: int
    total_in_progress: int
    total_completed: int
    total_failed: int
    total_held: int
    by_continuation: dict[str, int]  # pending count per continuation
    by_priority: dict[int, int]      # pending count per priority level

@dataclass
class ThroughputStats:
    """Throughput and timing statistics."""
    requests_per_minute: float
    average_response_time_ms: float
    total_runtime_seconds: float

@dataclass
class CompressionStats:
    """Compression efficiency statistics."""
    total_original_bytes: int
    total_compressed_bytes: int
    compression_ratio: float
    by_continuation: dict[str, float]  # ratio per continuation

@dataclass
class ResultStats:
    """Statistics about scraped results."""
    total_results: int
    valid_results: int
    invalid_results: int
    by_type: dict[str, int]  # count per result_type

@dataclass
class ErrorStats:
    """Statistics about errors."""
    total_errors: int
    unresolved_errors: int
    by_type: dict[str, int]           # count per error_type
    by_continuation: dict[str, int]   # count per continuation
    structural_errors: int
    validation_errors: int
    transient_errors: int

@dataclass
class DevDriverStats:
    """Comprehensive driver statistics."""
    queue: QueueStats
    throughput: ThroughputStats
    compression: CompressionStats
    results: ResultStats
    errors: ErrorStats
    run_status: str                   # From run_metadata: created, running, completed, error, interrupted

@dataclass
class ErrorRecord:
    """Full error record from the database."""
    id: int
    request_id: int | None
    error_type: str                   # 'structural', 'validation', 'transient'
    error_class: str                  # Full exception class name
    message: str
    request_url: str
    context: dict[str, Any]           # Parsed from context_json
    is_resolved: bool
    resolved_at: datetime | None
    resolution_notes: str | None
    created_at: datetime

    # Type-specific fields (populated based on error_type)
    # Structural errors
    selector: str | None = None
    selector_type: str | None = None
    expected_min: int | None = None
    expected_max: int | None = None
    actual_count: int | None = None

    # Validation errors
    model_name: str | None = None
    validation_errors: list[dict] | None = None
    failed_doc: dict | None = None

    # Transient errors
    status_code: int | None = None
    timeout_seconds: float | None = None

    def to_json(self) -> str:
        """Serialize to JSON for web transport."""
        ...


# --- Progress Events (for web interface) ---

class ProgressEventType(Enum):
    """Types of progress events emitted during scraping."""
    RUN_STARTED = "run_started"
    RUN_COMPLETED = "run_completed"
    REQUEST_ENQUEUED = "request_enqueued"
    REQUEST_STARTED = "request_started"
    REQUEST_COMPLETED = "request_completed"
    REQUEST_FAILED = "request_failed"
    DATA_SCRAPED = "data_scraped"
    ERROR_OCCURRED = "error_occurred"
    QUEUE_CHANGED = "queue_changed"

@dataclass
class ProgressEvent:
    """Event emitted during scraper execution for real-time updates.

    All events include timestamp and event_type. Additional fields
    depend on the event type.
    """
    event_type: ProgressEventType
    timestamp: datetime

    # Contextual data (varies by event type)
    request_id: int | None = None
    url: str | None = None
    continuation: str | None = None
    status_code: int | None = None
    error_id: int | None = None
    error_message: str | None = None
    result_id: int | None = None
    result_type: str | None = None
    queue_size: int | None = None
    run_status: str | None = None       # For RUN_COMPLETED

    def to_json(self) -> str:
        """Serialize to JSON for WebSocket transport."""
        ...


# --- Pagination and Query Types ---

@dataclass
class Page(Generic[T]):
    """Paginated result set."""
    items: list[T]
    total: int
    offset: int
    limit: int
    has_more: bool

@dataclass
class RequestRecord:
    """Request record from database for listing."""
    id: int
    status: str                       # pending, in_progress, completed, failed, held
    priority: int
    method: str
    url: str
    continuation: str
    retry_count: int
    cumulative_backoff: float         # Total backoff time accumulated
    next_retry_delay: float | None    # Next backoff delay (None if not retrying)
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    last_error: str | None

    def to_json(self) -> str:
        """Serialize to JSON for web transport."""
        ...

@dataclass
class ResponseRecord:
    """Response record summary for listing."""
    id: int
    request_id: int
    status_code: int
    url: str
    continuation: str
    content_size_original: int
    content_size_compressed: int
    created_at: datetime

    def to_json(self) -> str:
        """Serialize to JSON for web transport."""
        ...

@dataclass
class ResultRecord:
    """Scraped result record for listing."""
    id: int
    request_id: int | None
    result_type: str
    is_valid: bool
    data_preview: str                 # Truncated JSON for display
    created_at: datetime

    def to_json(self) -> str:
        """Serialize to JSON for web transport."""
        ...
```

## Worker Control and Shutdown

### Reusing AsyncDriver's stop_event

`LocalDevDriver` uses the same `stop_event: asyncio.Event` pattern from `AsyncDriver`, but overrides `run()` to behave differently when the event is set:

**AsyncDriver behavior** (not suitable for resumability):
- Drains the queue (discards pending items)
- Cancels workers immediately

**LocalDevDriver behavior** (preserves state for resume):
- Workers complete their current request, then exit
- Queue is NOT drained - pending items remain in DB
- `in_progress` requests are reset to `pending` in DB
- Run status updated to `interrupted`

### Graceful Shutdown Flow

```python
async def run(self) -> None:
    """Override run() to handle stop_event without draining queue."""
    # ... setup ...

    # Start workers (same as AsyncDriver)
    workers = [asyncio.create_task(self._worker(i)) for i in range(self.num_workers)]

    while True:
        if self.stop_event and self.stop_event.is_set():
            # DIFFERENT FROM AsyncDriver:
            # Don't drain queue - just wait for workers to finish current work
            # Workers check stop_event and exit after completing current request
            break

        try:
            await asyncio.wait_for(
                asyncio.shield(self.request_queue.join()),
                timeout=0.5,
            )
            break  # Queue empty, all work done
        except TimeoutError:
            continue

    # Wait for workers to finish (they'll exit after current request)
    for worker in workers:
        worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    # Reset any in_progress requests back to pending for resume
    await self._reset_in_progress_requests()

    # Update run status
    if self.stop_event and self.stop_event.is_set():
        await self._update_run_status("interrupted")
    else:
        await self._update_run_status("completed")
```

### Worker Loop (inherits from AsyncDriver)

Workers already check `stop_event.is_set()` before getting the next request (line 319 of async_driver.py), so they naturally exit after completing their current work when the event is set.

### Signal Handling

```python
def _setup_signal_handlers(self) -> None:
    """Register SIGINT/SIGTERM handlers."""
    import signal

    def handle_signal(signum: int, frame: Any) -> None:
        if self.stop_event:
            self.stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
```

## WARC Export Format

The WARC export captures request/response pairs in standard WARC format:

1. **Request records** - `WARC-Type: request`
   - Full HTTP request with method, URL, headers, body

2. **Response records** - `WARC-Type: response`
   - Full HTTP response with status, headers, body (decompressed from zstd)
   - Linked to request via `WARC-Concurrent-To`

Note: Scraper context (continuation, accumulated_data) is **not** included in WARC
to maintain standard format compatibility. This data remains in the SQLite database.

## Runner Script

A CLI script to run scrapers with the LocalDevDriver:

```bash
# Basic usage
python -m kent.driver.dev_driver.run \
    --scraper juriscraper.scrapers.example.ExampleScraper \
    --db ./scraper_data.db

# With options
python -m kent.driver.dev_driver.run \
    --scraper juriscraper.scrapers.example.ExampleScraper \
    --db ./scraper_data.db \
    --storage ./downloads \
    --delay 15 \
    --jitter 3 \
    --workers 2 \
    --no-resume

# Check status (unstarted/in_progress/done)
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --status

# Stats only (detailed)
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --stats

# Export WARC
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --export-warc ./archive.warc.gz

# Train compression dictionaries
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --train-dict parse_listing

# Recompress with latest dictionaries
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --recompress

# List errors (unresolved by default)
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --errors

# List errors by type
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --errors --error-type structural

# Requeue a specific error's request
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --requeue-error 42

# Requeue all structural errors for a continuation
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --requeue-errors structural --continuation parse_listing

# Mark error resolved without requeue
python -m kent.driver.dev_driver.run \
    --db ./scraper_data.db \
    --resolve-error 42 --notes "Data manually corrected"
```

---

## Design Decisions (Resolved)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Queue Persistence | Immediate writes | SQLite WAL mode keeps it fast; guarantees no work lost on crash |
| Compression Scope | Per-continuation | Similar HTML structures come from same continuations |
| Response Storage | Compressed only | Storage efficiency matters for dev; decompress on WARC export |
| Failed Requests | Time-based backoff | Exponential backoff from base_delay until max_backoff_time (default 1hr) |
| Multi-scraper | Single scraper per DB | Simpler schema, cleaner separation |
| Archive Files | Filesystem with DB path | Large files stored on disk; DB stores reference path |
| Result Validation | Immediate | Matches current AsyncDriver behavior |
| Graceful Shutdown | Complete in-progress | Wait for current requests to finish; prevents orphaned requests |
| WARC Export Depth | Immediate pair only | Standard WARC format; simpler and more compatible |

---

## BaseScraper Additions

The following additions to `BaseScraper` (in `data_types.py`) support step introspection in the web interface:

```python
@dataclass
class StepInfo:
    """Metadata about a scraper step method.

    Attributes:
        name: The method name (continuation string).
        priority: Priority hint for queue ordering (lower = higher priority).
        encoding: Character encoding for text/HTML decoding.
    """
    name: str
    priority: int
    encoding: str


class BaseScraper(Generic[ScraperReturnType]):
    # ... existing methods ...

    @classmethod
    def list_steps(cls) -> list[StepInfo]:
        """List all step methods defined on this scraper.

        Introspects the class to find all methods decorated with @step
        and returns their metadata.

        This is useful for the web interface to display available steps,
        their priorities, and to populate dropdowns for pause_step/resume_step.

        Returns:
            List of StepInfo objects for each decorated step method.

        Example:
            >>> class MyScraper(BaseScraper[CaseData]):
            ...     @step
            ...     def parse_listing(self, lxml_tree): ...
            ...
            ...     @step(priority=5)
            ...     def parse_detail(self, lxml_tree): ...
            ...
            >>> MyScraper.list_steps()
            [StepInfo(name='parse_listing', priority=9, encoding='utf-8'),
             StepInfo(name='parse_detail', priority=5, encoding='utf-8')]
        """
        from kent.common.decorators import get_step_metadata

        steps = []
        for name in dir(cls):
            if name.startswith('_'):
                continue
            try:
                method = getattr(cls, name)
                metadata = get_step_metadata(method)
                if metadata is not None:
                    steps.append(StepInfo(
                        name=name,
                        priority=metadata.priority,
                        encoding=metadata.encoding,
                    ))
            except Exception:
                continue
        return steps
```

---

## Implementation Phases

### Phase 1: Core Infrastructure
- [ ] SQLite schema and migrations (7 tables)
- [ ] Basic LocalDevDriver with DB-backed queue
- [ ] Request/response persistence (uncompressed)
- [ ] Resumability
- [ ] Scraper run tracking with params

### Phase 2: Rate Limiting
- [ ] AioSQLiteBucket rate limiter
- [ ] Integration with LocalDevDriver

### Phase 3: Error Tracking
- [ ] Error capture for all exception types
- [ ] Error storage with type-specific fields
- [ ] Link errors to requests
- [ ] Requeue functionality (single and batch)
- [ ] Error resolution tracking

### Phase 4: Compression
- [ ] Zstandard compression for responses
- [ ] Dictionary training function
- [ ] Recompression function

### Phase 5: Statistics
- [ ] Stats collection and queries
- [ ] DevDriverStats dataclass with ErrorStats
- [ ] Per-continuation breakdowns

### Phase 6: WARC Export
- [ ] WARC export function
- [ ] Test with warcio library

### Phase 7: CLI Runner
- [ ] Argument parsing
- [ ] Run/stats/export commands
- [ ] Error management commands

---

## FastAPI Web Interface

### Overview

A FastAPI-based web interface provides real-time monitoring, control, and management of scraper runs. The server watches a designated `runs/` directory for active databases and provides REST/WebSocket APIs for all driver functionality.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        FastAPI Web Server                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │ Lifespan     │    │ Run Manager  │    │ WebSocket Hub            │  │
│  │ Manager      │    │              │    │                          │  │
│  │              │    │ • runs/      │    │ • Progress streaming     │  │
│  │ • Startup    │    │   directory  │    │ • Stats updates          │  │
│  │ • Shutdown   │    │ • Driver     │    │ • Error notifications    │  │
│  │ • Hot reload │    │   lifecycle  │    │                          │  │
│  └──────────────┘    └──────────────┘    └──────────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                         REST API                                  │  │
│  │  /runs, /runs/{id}/stats, /runs/{id}/errors, /runs/{id}/start    │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
             ┌──────────┐    ┌──────────┐    ┌──────────┐
             │ run_1.db │    │ run_2.db │    │ run_3.db │
             │(running) │    │ (done)   │    │(unstarted)│
             └──────────┘    └──────────┘    └──────────┘
                        runs/ directory
```

### Lifespan Management with Hot Reload

The server uses FastAPI's `lifespan` context manager to coordinate with driver shutdown:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle with graceful driver shutdown."""
    # Startup
    app.state.run_manager = RunManager(runs_dir=Path("runs"))
    await app.state.run_manager.initialize()

    yield

    # Shutdown (triggered by hot reload or termination)
    # Stop all running drivers gracefully
    await app.state.run_manager.shutdown_all(timeout=30.0)

app = FastAPI(lifespan=lifespan)
```

When uvicorn hot-reloads (file change detected):
1. Lifespan `shutdown` triggers
2. `RunManager.shutdown_all()` calls `driver.stop_workers()` on each active driver
3. Workers complete current requests, mark in_progress → pending
4. DB connections close cleanly
5. New process starts with fresh state, drivers resume from DB

### Run Manager

```python
@dataclass
class RunInfo:
    """Information about a scraper run."""
    id: str                           # Derived from filename: run_1.db → "run_1"
    db_path: Path
    status: Literal["unstarted", "in_progress", "done", "error"]
    scraper_name: str | None
    created_at: datetime | None
    started_at: datetime | None
    ended_at: datetime | None

    # Stats summary (lazy-loaded)
    pending_count: int | None = None
    completed_count: int | None = None
    error_count: int | None = None

class RunManager:
    """Manages multiple scraper runs in a directory.

    Watches the runs/ directory for .db files and provides
    access to their LocalDevDriver instances.
    """

    def __init__(self, runs_dir: Path) -> None:
        self.runs_dir = runs_dir
        self._active_drivers: dict[str, LocalDevDriver] = {}
        self._driver_tasks: dict[str, asyncio.Task] = {}

    async def initialize(self) -> None:
        """Initialize run manager, scan directory for existing runs."""
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        # Scan for existing .db files, populate run info
        ...

    async def list_runs(self) -> list[RunInfo]:
        """List all runs in the directory with their status."""
        ...

    async def get_run(self, run_id: str) -> RunInfo | None:
        """Get detailed info for a specific run."""
        ...

    async def create_run(
        self,
        scraper_module: str,
        params: dict | None = None,
        base_delay: float = 10.0,
        jitter: float = 2.0,
        num_workers: int = 1,
        max_backoff_time: float = 3600.0,
    ) -> RunInfo:
        """Create a new run with specified parameters.

        Creates a new .db file in runs/ with the run_metadata populated.
        Does NOT start the scraper - use start_run() for that.
        """
        ...

    async def start_run(
        self,
        run_id: str,
        on_progress: Callable[[ProgressEvent], Awaitable[None]] | None = None,
    ) -> None:
        """Start or resume a run.

        Opens the LocalDevDriver and begins processing.
        Progress events are routed to WebSocket subscribers.
        """
        ...

    async def stop_run(self, run_id: str) -> None:
        """Stop a running scraper without draining queue."""
        if run_id in self._active_drivers:
            self._active_drivers[run_id].stop()
            # Wait for the driver task to complete
            if run_id in self._driver_tasks:
                await self._driver_tasks[run_id]
            ...

    async def get_driver(self, run_id: str) -> LocalDevDriver | None:
        """Get the driver instance for a run (if active)."""
        return self._active_drivers.get(run_id)

    async def shutdown_all(self, timeout: float = 30.0) -> None:
        """Gracefully shutdown all active drivers.

        Called during lifespan shutdown for hot reload compatibility.
        """
        # Signal all drivers to stop
        for driver in self._active_drivers.values():
            driver.stop()

        # Wait for all driver tasks to complete (with timeout)
        if self._driver_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._driver_tasks.values(), return_exceptions=True),
                    timeout=timeout,
                )
            except TimeoutError:
                # Force cancel any remaining tasks
                for task in self._driver_tasks.values():
                    if not task.done():
                        task.cancel()

        # Close all driver connections
        for driver in self._active_drivers.values():
            await driver.close()

        self._active_drivers.clear()
        self._driver_tasks.clear()
```

### REST API Endpoints

```python
# === Run Management ===

@app.get("/api/runs")
async def list_runs() -> list[RunInfo]:
    """List all runs with status summary."""
    ...

@app.get("/api/runs/{run_id}")
async def get_run(run_id: str) -> RunInfo:
    """Get detailed run information."""
    ...

@app.post("/api/runs")
async def create_run(config: RunConfig) -> RunInfo:
    """Create a new run (does not start it)."""
    ...

@app.post("/api/runs/{run_id}/start")
async def start_run(run_id: str) -> RunInfo:
    """Start or resume a run."""
    ...

@app.post("/api/runs/{run_id}/stop")
async def stop_run(run_id: str, timeout: float = 30.0) -> RunInfo:
    """Stop a running scraper."""
    ...

@app.delete("/api/runs/{run_id}")
async def delete_run(run_id: str) -> dict:
    """Delete a run and its database (must be stopped first)."""
    ...

# === Statistics ===

@app.get("/api/runs/{run_id}/stats")
async def get_stats(run_id: str) -> DevDriverStats:
    """Get comprehensive statistics for a run."""
    ...

# === Requests ===

@app.get("/api/runs/{run_id}/requests")
async def list_requests(
    run_id: str,
    status: str | None = None,
    continuation: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> Page[RequestRecord]:
    """List requests with pagination."""
    ...

@app.get("/api/runs/{run_id}/requests/{request_id}")
async def get_request(run_id: str, request_id: int) -> RequestRecord:
    """Get a specific request."""
    ...

@app.delete("/api/runs/{run_id}/requests/{request_id}")
async def cancel_request(run_id: str, request_id: int) -> dict:
    """Cancel a pending request."""
    ...

@app.delete("/api/runs/{run_id}/requests")
async def cancel_requests_by_continuation(
    run_id: str,
    continuation: str,
) -> dict:
    """Cancel all pending requests for a continuation."""
    ...

# === Step Control ===

@app.get("/api/runs/{run_id}/steps")
async def list_steps(run_id: str) -> list[StepInfo]:
    """List all step methods defined on the scraper for this run."""
    ...

@app.post("/api/runs/{run_id}/steps/{continuation}/pause")
async def pause_step(run_id: str, continuation: str) -> dict:
    """Pause processing of requests for a specific step.

    Marks all pending requests for this continuation as 'held'.
    """
    ...

@app.post("/api/runs/{run_id}/steps/{continuation}/resume")
async def resume_step(run_id: str, continuation: str) -> dict:
    """Resume processing of held requests for a specific step.

    Marks all held requests for this continuation as 'pending'.
    """
    ...

# === Responses ===

@app.get("/api/runs/{run_id}/responses")
async def list_responses(
    run_id: str,
    continuation: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> Page[ResponseRecord]:
    """List responses with pagination."""
    ...

@app.get("/api/runs/{run_id}/responses/{response_id}")
async def get_response(run_id: str, response_id: int) -> ResponseRecord:
    """Get response metadata."""
    ...

@app.get("/api/runs/{run_id}/responses/{response_id}/content")
async def get_response_content(run_id: str, response_id: int) -> Response:
    """Get decompressed response body."""
    ...

# === Results ===

@app.get("/api/runs/{run_id}/results")
async def list_results(
    run_id: str,
    result_type: str | None = None,
    is_valid: bool | None = None,
    offset: int = 0,
    limit: int = 50,
) -> Page[ResultRecord]:
    """List scraped results with pagination."""
    ...

@app.get("/api/runs/{run_id}/results/{result_id}")
async def get_result(run_id: str, result_id: int) -> dict:
    """Get full result data."""
    ...

# === Errors ===

@app.get("/api/runs/{run_id}/errors")
async def list_errors(
    run_id: str,
    error_type: str | None = None,
    continuation: str | None = None,
    unresolved_only: bool = True,
    offset: int = 0,
    limit: int = 50,
) -> Page[ErrorRecord]:
    """List errors with pagination."""
    ...

@app.get("/api/runs/{run_id}/errors/{error_id}")
async def get_error(run_id: str, error_id: int) -> ErrorRecord:
    """Get a specific error."""
    ...

@app.post("/api/runs/{run_id}/errors/{error_id}/requeue")
async def requeue_error(
    run_id: str,
    error_id: int,
    mark_resolved: bool = True,
    resolution_notes: str | None = None,
) -> dict:
    """Requeue the request that caused an error."""
    ...

@app.post("/api/runs/{run_id}/errors/requeue")
async def requeue_errors_by_type(
    run_id: str,
    error_type: str,
    continuation: str | None = None,
    mark_resolved: bool = True,
) -> dict:
    """Batch requeue all errors of a type."""
    ...

@app.post("/api/runs/{run_id}/errors/{error_id}/resolve")
async def resolve_error(
    run_id: str,
    error_id: int,
    resolution_notes: str | None = None,
) -> dict:
    """Mark an error as resolved without requeuing."""
    ...

# === Compression ===

@app.post("/api/runs/{run_id}/compression/train")
async def train_compression_dict(
    run_id: str,
    continuation: str,
    sample_limit: int = 1000,
    dict_size: int = 112640,
) -> dict:
    """Train a new compression dictionary."""
    ...

@app.post("/api/runs/{run_id}/compression/recompress")
async def recompress_responses(
    run_id: str,
    continuation: str | None = None,
) -> dict:
    """Recompress responses with latest dictionaries."""
    ...

# === Export ===

@app.post("/api/runs/{run_id}/export/warc")
async def export_warc(
    run_id: str,
    compress: bool = True,
) -> FileResponse:
    """Export run to WARC file."""
    ...
```

### WebSocket API

```python
@app.websocket("/ws/runs/{run_id}")
async def run_websocket(websocket: WebSocket, run_id: str):
    """WebSocket endpoint for real-time run updates.

    Streams ProgressEvents as JSON to connected clients.

    Client messages:
        {"type": "subscribe", "events": ["all"]}  # Subscribe to all events
        {"type": "subscribe", "events": ["error", "data_scraped"]}  # Selective
        {"type": "unsubscribe"}

    Server messages:
        ProgressEvent serialized as JSON
    """
    await websocket.accept()

    # Register this websocket with the run's progress callback
    subscription = await run_manager.subscribe(run_id, websocket)

    try:
        while True:
            # Handle client messages (subscribe/unsubscribe)
            data = await websocket.receive_json()
            await subscription.handle_message(data)
    except WebSocketDisconnect:
        await run_manager.unsubscribe(run_id, websocket)
```

### Rate Limiting with pyrate_limiter

Custom async bucket using the shared aiosqlite connection, integrated with the driver's database:

```python
from pyrate_limiter import AbstractBucket, Rate, RateItem
import aiosqlite

class AioSQLiteBucket(AbstractBucket):
    """Async SQLite bucket using shared aiosqlite connection.

    Integrates with LocalDevDriver's existing database connection
    for unified rate limiting and request tracking.
    """

    def __init__(
        self,
        rates: list[Rate],
        conn: aiosqlite.Connection,
        table: str = "rate_bucket",
    ) -> None:
        self.rates = rates
        self.conn = conn
        self.table = table
        self._lock = asyncio.Lock()

    async def put(self, item: RateItem) -> bool:
        """Put an item in the bucket, return True if within rate limit."""
        async with self._lock:
            # Check if we have space for this item
            for rate in self.rates:
                query = f"""
                    SELECT COUNT(*) FROM {self.table}
                    WHERE timestamp > (strftime('%s', 'now') * 1000 - ?)
                """
                async with self.conn.execute(query, (rate.interval,)) as cursor:
                    count = (await cursor.fetchone())[0]

                if count + item.weight > rate.limit:
                    self.failing_rate = rate
                    return False

            # Insert item(s)
            query = f"INSERT INTO {self.table} (name, timestamp) VALUES (?, ?)"
            for _ in range(item.weight):
                await self.conn.execute(query, (item.name, item.timestamp))
            await self.conn.commit()
            return True

    async def leak(self, current_timestamp: int | None = None) -> int:
        """Remove outdated items from bucket."""
        async with self._lock:
            if current_timestamp is None:
                current_timestamp = int(time.time() * 1000)

            oldest_allowed = current_timestamp - self.rates[-1].interval
            query = f"DELETE FROM {self.table} WHERE timestamp < ?"
            cursor = await self.conn.execute(query, (oldest_allowed,))
            count = cursor.rowcount
            await self.conn.commit()
            return count

    async def flush(self) -> None:
        """Clear all items from bucket."""
        async with self._lock:
            await self.conn.execute(f"DELETE FROM {self.table}")
            await self.conn.commit()
            self.failing_rate = None

    async def count(self) -> int:
        """Count items in bucket."""
        async with self._lock:
            query = f"SELECT COUNT(*) FROM {self.table}"
            async with self.conn.execute(query) as cursor:
                return (await cursor.fetchone())[0]

    async def peek(self, index: int) -> RateItem | None:
        """Peek at item at index (latest-to-earliest)."""
        async with self._lock:
            query = f"""
                SELECT name, timestamp FROM {self.table}
                ORDER BY timestamp DESC
                LIMIT 1 OFFSET ?
            """
            async with self.conn.execute(query, (index,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return RateItem(row[0], row[1])
                return None
```

### Request Models

```python
from pydantic import BaseModel

class RunConfig(BaseModel):
    """Configuration for creating a new run."""
    scraper_module: str               # e.g., "juriscraper.scrapers.ca.courts"
    run_id: str | None = None         # Optional custom ID, auto-generated if None
    params: dict | None = None        # ScraperParams as dict
    base_delay: float = 10.0
    jitter: float = 2.0
    num_workers: int = 1
    max_backoff_time: float = 3600.0  # 1 hour default

class RequeueConfig(BaseModel):
    """Configuration for batch requeue."""
    error_type: str
    continuation: str | None = None
    mark_resolved: bool = True

class ResolveErrorConfig(BaseModel):
    """Configuration for resolving an error."""
    resolution_notes: str | None = None
```

### Index Page

The index page (`/`) should render an HTML dashboard showing:

1. **Active Runs** - Currently running scrapers with live progress
2. **Completed Runs** - Finished runs with summary stats
3. **Unstarted Runs** - Created but not yet started
4. **New Run Form** - Configure and create new runs

```python
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")

@app.get("/")
async def index(request: Request):
    """Render the dashboard index page."""
    runs = await run_manager.list_runs()

    active = [r for r in runs if r.status == "in_progress"]
    completed = [r for r in runs if r.status == "done"]
    unstarted = [r for r in runs if r.status == "unstarted"]
    errored = [r for r in runs if r.status == "error"]

    # Get list of available scrapers for the new run form
    scrapers = await get_available_scrapers()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "active_runs": active,
            "completed_runs": completed,
            "unstarted_runs": unstarted,
            "errored_runs": errored,
            "scrapers": scrapers,
        },
    )

@app.get("/runs/{run_id}")
async def run_detail(request: Request, run_id: str):
    """Render detailed view for a specific run."""
    run = await run_manager.get_run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    driver = await run_manager.get_driver(run_id)
    stats = await driver.get_stats() if driver else None

    return templates.TemplateResponse(
        "run_detail.html",
        {
            "request": request,
            "run": run,
            "stats": stats,
        },
    )
```

---

## Rate Limiter Table Schema

Add to the SQLite schema for integrated rate limiting:

```sql
CREATE TABLE rate_bucket (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    timestamp INTEGER NOT NULL    -- Unix timestamp in milliseconds
);

CREATE INDEX idx_rate_bucket_timestamp ON rate_bucket(timestamp);
```

---

## Updated File Structure

```
juriscraper/scraper_driver/driver/dev_driver/
├── __init__.py
├── dev_driver.py          # LocalDevDriver class
├── schema.py              # SQLite schema (now 7 tables with rate_bucket)
├── compression.py         # Zstd dictionary management
├── stats.py               # Statistics dataclasses
├── errors.py              # Error tracking and requeue
├── warc_export.py         # WARC export
├── rate_limiter.py        # AioSQLiteBucket rate limiter
├── run.py                 # CLI runner
└── web/
    ├── __init__.py
    ├── app.py             # FastAPI application with lifespan
    ├── run_manager.py     # RunManager class
    ├── routes/
    │   ├── __init__.py
    │   ├── runs.py        # Run management endpoints
    │   ├── requests.py    # Request listing/cancellation
    │   ├── responses.py   # Response listing
    │   ├── results.py     # Result listing
    │   ├── errors.py      # Error management endpoints
    │   ├── compression.py # Compression endpoints
    │   └── export.py      # WARC export endpoint
    ├── websocket.py       # WebSocket handling
    ├── templates/
    │   ├── base.html
    │   ├── index.html     # Dashboard
    │   └── run_detail.html
    └── static/
        ├── css/
        └── js/
```

---

## Testing Strategy

1. **Unit tests**: Individual components (compression, rate limiter, etc.)
2. **Integration tests**: Full driver with mock scraper
3. **WARC validation**: Export and validate WARC format
4. **Resumability tests**: Interrupt and resume scenarios
5. **Compression tests**: Dictionary training, ratio verification
6. **Web interface tests**: FastAPI TestClient for REST endpoints
7. **WebSocket tests**: Connection, subscription, event streaming
8. **Hot reload tests**: Verify graceful shutdown preserves state
