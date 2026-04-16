=================
Persistent Driver
=================

Location: ``kent/driver/persistent_driver/``

The persistent driver is Kent's **production-grade driver** and the default
for ``kent run``. It extends ``AsyncDriver`` with SQLite-backed persistence,
providing full request/response archival, error tracking, and a web UI for
monitoring runs.


Mixin Architecture
==================

The persistent driver is composed of specialized mixins. Each mixin owns one
concern:

- **PersistentDriver** (``persistent_driver.py``): Main class, extends ``AsyncDriver``. Orchestrates the run lifecycle, signal handling, and shutdown.
- **QueueMixin** (``_queue.py``): Request serialization/deserialization and SQLite-backed enqueue/dequeue with deduplication.
- **SpeculationMixin** (``_speculation.py``): Speculation state tracking with database persistence and gap-based extension.
- **StorageMixin** (``_storage.py``): Response compression and storage, result storage (valid and invalid), retry logic with exponential backoff.
- **WorkerMixin** (``_workers.py``): Async worker coroutines, dynamic scaling, and the worker monitor.
- **APIMixin** (``_api.py``): Public API for the web UI, step pause/resume, and diagnosis (re-running continuations against stored responses).


Run Lifecycle
=============

The ``run()`` method is the main entry point. The overall flow:

.. md-mermaid::
    :class: align-center

    flowchart TB
        Start["run()"] --> Signals["Register signal handlers<br/>(SIGINT/SIGTERM → stop_event)"]
        Signals --> Status["Set run status = 'running'"]
        Status --> Check{"Queue empty?"}
        Check -->|Yes| Seed["Seed queue from<br/>seed_params"]
        Check -->|No| Resume["Resume from<br/>existing queue"]
        Seed --> Spec["Discover speculative entries<br/>Load persisted state<br/>Seed speculative queue"]
        Resume --> Spec
        Spec --> Spawn["Spawn initial workers<br/>Start monitor task"]
        Spawn --> Wait["Main waiting loop<br/>(asyncio.wait FIRST_COMPLETED)"]
        Wait --> Done{"All workers<br/>+ monitor done?"}
        Done -->|No| Wait
        Done -->|Yes| Finalize["Persist speculation state<br/>Update run status<br/>Dispose engine"]

**Initialization sequence:**

1. ``_init_db()`` creates the async SQLAlchemy engine, session factory, and ``SQLManager``
2. Run metadata (scraper name, version) is stored in the ``run_metadata`` table
3. If ``resume=True`` and the queue has pending requests, they are preserved
4. Signal handlers (SIGINT/SIGTERM) are registered, calling ``stop()`` which sets ``stop_event``

**Queue seeding:**

- If the queue is empty, ``seed_params`` are loaded (from the caller or from the DB, where the web UI can set them)
- Non-speculative entries are dispatched via ``initial_seed()`` and serialized into ``pending`` request rows
- Speculative entries are discovered, persisted state is loaded (for resumption), and the speculative queue is seeded

**Main waiting loop:**

.. code-block:: python

    while self._worker_tasks or (self._monitor_task and not self._monitor_task.done()):
        done, _ = await asyncio.wait(tasks_to_wait, return_when=FIRST_COMPLETED)
        for task in done:
            if task.exception() is not None and task is not self._monitor_task:
                raise task.exception()

``FIRST_COMPLETED`` ensures the loop is responsive to worker crashes, monitor
exits, and signal-driven shutdowns.


Worker Coroutine
================

Each worker runs ``_db_worker(worker_id)``, the core request processing loop.

.. md-mermaid::
    :class: align-center

    flowchart TB
        Start["Worker start"] --> CheckStop{"stop_event<br/>set?"}
        CheckStop -->|Yes| Exit["Exit worker"]
        CheckStop -->|No| Dequeue["Atomic dequeue<br/>(UPDATE...RETURNING)"]
        Dequeue --> Got{"Got request?"}
        Got -->|No| Retry{"Scheduled<br/>retry pending?"}
        Retry -->|Yes| Sleep["Sleep until<br/>retry ready"]
        Sleep --> Dequeue
        Retry -->|No| Poll["Poll 100x at 100ms<br/>Check in_progress + pending"]
        Poll --> Idle{"Still idle?"}
        Idle -->|Yes| Exit
        Idle -->|No| Dequeue
        Got -->|Yes| Rate["Rate limiter<br/>(if enabled)"]
        Rate --> Process["Process request<br/>(HTTP fetch)"]
        Process --> Spec{"Speculative?"}
        Spec -->|Yes| Track["Track speculation<br/>outcome"]
        Spec -->|No| Store["Store response<br/>+ run continuation"]
        Track --> Store
        Store --> Yields["Pattern-match yields:<br/>ParsedData → store result<br/>Request → enqueue<br/>EstimateData → store estimate"]
        Yields --> Complete["Mark completed"]
        Complete --> CheckStop

        Process -->|TransientException| HandleRetry{"Retry budget<br/>remaining?"}
        HandleRetry -->|Yes| Schedule["Schedule retry<br/>(exponential backoff)"]
        Schedule --> CheckStop
        HandleRetry -->|No| Fail["Mark failed<br/>+ store error"]
        Fail --> CheckStop
        Process -->|Other exception| Fail

**Atomic dequeue** uses a single SQL statement to prevent race conditions
between concurrent workers:

.. code-block:: sql

    UPDATE requests
    SET status = 'in_progress', started_at_ns = ?
    WHERE id = (
        SELECT id FROM requests
        WHERE status = 'pending'
          AND (started_at IS NULL OR started_at <= datetime('now'))
        ORDER BY priority ASC, queue_counter ASC
        LIMIT 1
    )
    RETURNING id, ...

SQLite's serializable isolation ensures only one worker gets each request.

**Worker exit conditions:**

- ``stop_event`` is set (graceful shutdown)
- Polling finds no pending requests AND no in-progress requests for ~3 seconds
- Exception propagated to the main waiting loop

**Rate limiting** is applied before request processing. After the rate limiter
releases, ``restamp_request_start()`` resets the timer so rate limiter wait
time is excluded from request duration metrics.


Worker Monitor
==============

The monitor runs ``_worker_monitor()`` on a 60-second cycle alongside the
workers.

.. md-mermaid::
    :class: align-center

    flowchart TB
        Start["Monitor start"] --> Wait["Sleep 60s<br/>(or stop_event)"]
        Wait --> Check{"Workers alive<br/>or pending > 0?"}
        Check -->|No| Exit["Exit monitor"]
        Check -->|Yes| Scale{"pending > 0 AND<br/>active < max_workers?"}
        Scale -->|Yes| Spawn["Spawn worker<br/>(based on rate limit<br/>+ avg duration)"]
        Scale -->|No| Compress{"Continuations with<br/>1000+ responses?"}
        Spawn --> Compress
        Compress -->|Yes| Train["Train compression<br/>dict + recompress"]
        Compress -->|No| Wait
        Train --> Wait

**Dynamic scaling formula:**

- If no rate limits: ``workers_needed = max_workers``
- If no timing data yet: ``workers_needed = active + 1``
- Otherwise: ``workers_needed = ceil(max_rate_per_sec * avg_request_duration)``

The monitor also auto-trains zstd compression dictionaries when a
continuation accumulates 1000+ uncompressed responses.


Request State Machine
=====================

::

    pending ──→ in_progress ──→ completed
       ↑            │
       │            │ (transient error)
       │            ↓
       └──── pending (retry)
                    │
                    │ (max backoff exceeded)
                    ↓
                  failed

Additional states:

- ``held``: Manually paused via the API (``pause_step(continuation)``)

**Retry logic** uses exponential backoff: delays of 1s, 2s, 4s, 8s, ...
capped at ``max_backoff_time / 4`` per retry. A cumulative backoff counter
tracks total wait time; when it exceeds ``max_backoff_time``, the request
is marked ``failed``.

Retries work by setting the request back to ``pending`` with ``started_at``
set to a future timestamp. The dequeue query's ``started_at <= datetime('now')``
clause causes workers to skip it until the delay expires.


Queue Serialization
===================

``QueueMixin`` handles converting between in-memory ``Request`` objects and
database rows.

**Serialization** (``_serialize_request``): Converts a ``Request`` into a flat
dict of database columns. JSON-encodes headers, cookies, accumulated_data,
permanent, speculation_id, and via. Encodes query params into the URL.

**Deserialization** (``_deserialize_request``): Reverses the process --
parses JSON fields, reconstructs ``HTTPRequestParams``, and creates the
appropriate ``Request`` type based on ``request_type`` (navigating,
non_navigating, archive).

**Deduplication** happens at enqueue time: the request's ``deduplication_key``
is checked against existing rows. If found, the request is silently dropped.
Requests with ``SkipDeduplicationCheck`` bypass this.


Speculation Persistence
=======================

``SpeculationMixin`` extends the sync driver's two-phase speculation with
database-backed state so speculation can resume across process restarts.

**State is persisted** to the ``speculation_tracking`` table after each
outcome (success or failure). The table stores: ``func_name``,
``highest_successful_id``, ``consecutive_failures``, ``current_ceiling``,
``stopped``, ``param_index``, and the serialized template.

**On resume**, ``_load_speculation_state_from_db()`` restores the state dict
from the database, allowing speculation to continue from where it left off.

**Outcome tracking** (``_track_speculation_outcome``) is called after each
speculative response. It uses an ``asyncio.Lock`` to prevent race conditions
when multiple workers process speculative requests for the same entry
concurrently.

**Extension** (``_extend_speculation``) fires when a success is detected
near the current ceiling, seeding another ``max_gap()`` requests into the
queue and advancing the ceiling.


Response Storage and Compression
================================

``StorageMixin`` handles persisting HTTP responses and scraped results.

**Responses** are compressed with **zstd** before storage. The compression
module supports per-continuation trained dictionaries for significantly better
ratios on similar HTML pages (10-20x typical).

**Inline storage:** Response data is stored directly in the ``requests`` table
(not a separate responses table) to avoid join overhead during dequeue. Fields
include ``content_compressed``, ``content_size_original``,
``content_size_compressed``, and ``compression_dict_id``.

**Archive responses** (file downloads) store file metadata in the
``archived_files`` table (path, URL, expected type, size, SHA256 hash) but
do not store content in the database -- the file is already on disk.

**Results** (``ParsedData`` yields) are stored in the ``results`` table with
``data_json``, ``is_valid``, and ``validation_errors_json``. Both valid and
invalid data is preserved for post-hoc inspection.


Database Schema
===============

The schema is versioned via ``migrations/`` and managed by ``SQLManager``.

**Core tables:**

``requests``
    The central table. Holds queue state, HTTP request params, response data
    (inline), compression metadata, speculation tracking, retry state, and
    timing. Key indexes: ``(status, priority, queue_counter)`` for dequeue
    performance, ``(deduplication_key)`` for dedup checks,
    ``(parent_request_id)`` for request tree traversal.

``results``
    Parsed data yields. FK to ``requests``. Stores ``result_type``,
    ``data_json``, ``is_valid``, ``validation_errors_json``.

``errors``
    Structured error records. FK to ``requests``. Stores error class, message,
    traceback, and structured fields for HTML structural errors (selector,
    expected/actual counts) and validation errors (model name, failed doc).
    Supports resolution tracking.

``compression_dicts``
    Trained zstd dictionaries keyed by ``(continuation, version)``.

``archived_files``
    File download metadata. FK to ``requests``.

``speculation_tracking``
    Per-template speculation state for resumption.

``run_metadata``
    Single-row table with scraper name, status, timestamps, and seed params.


The SQLManager Layer
====================

Location: ``sql_manager/``

``SQLManager`` is itself composed of mixins, providing all database operations
without direct ORM usage by the driver:

- **RequestQueueMixin**: ``insert_request()``, ``dequeue_next_request()``
- **ResponseStorageMixin**: ``store_response()``, ``get_response_content()``
- **ResultStorageMixin**: ``store_result()``
- **SpeculationMixin**: ``save_speculation_state()``, ``load_speculation_state()``
- **RunMetadataMixin**: ``update_run_status()``, ``get_run_metadata()``
- **ListingMixin**: Paginated queries with filtering
- **ValidationMixin**: Error storage and retrieval
- **EstimateStorageMixin**: ``EstimateData`` persistence
- **IncidentalRequestStorageMixin**: Browser network activity capture (used by the Playwright driver)


The API Mixin
=============

``APIMixin`` provides the public interface consumed by both the ``pdd`` CLI
and the web UI:

**Step control:** ``pause_step(continuation)`` marks all pending requests for
a continuation as ``held``; ``resume_step(continuation)`` reverses it.

**Diagnosis:** ``diagnose(request_id)`` re-runs a continuation against a
stored response, capturing all yields and selector queries via
``SelectorObserver``. Returns a ``DiagnoseResult`` with the yield list,
a human-readable selector tree, and JSON for UI highlighting.

**Listing and export:** Paginated queries for requests, responses, results,
and errors. WARC export for captured traffic.


Async Coordination Patterns
============================

**Worker isolation:** Each worker is an independent ``asyncio.Task``. Workers
coordinate only through the database (atomic dequeue) and the shared
``stop_event``. There is no in-memory queue or message passing between workers.

**Speculation locking:** An ``asyncio.Lock`` (``_speculation_lock``) protects
speculation state updates. Multiple workers may simultaneously process
speculative requests for the same entry; the lock serializes their outcome
tracking.

**Main loop responsiveness:** ``asyncio.wait(FIRST_COMPLETED)`` ensures the
main loop reacts immediately to worker crashes, monitor exits, or
signal-driven shutdowns rather than blocking until all tasks complete.

**Graceful shutdown:** ``stop()`` sets ``stop_event``. Workers poll this at the
top of each loop iteration and during idle polling. The monitor checks it
every 60-second cycle. After all tasks exit, ``run()`` persists speculation
state and updates the run status.
