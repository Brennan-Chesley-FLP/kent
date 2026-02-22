"""LocalDevDriver - SQLite-backed async driver for local development.

This driver extends AsyncDriver with persistent storage for:
- Request queue with resumability
- Response archival with compression
- Error tracking with requeue capability
- Progress events for web interface integration
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from kent.data_types import (
    BaseScraper,
)
from kent.driver.async_driver import AsyncDriver
from kent.driver.persistent_driver._api import APIMixin, DiagnoseResult
from kent.driver.persistent_driver._queue import QueueMixin
from kent.driver.persistent_driver._speculation import SpeculationMixin
from kent.driver.persistent_driver._storage import StorageMixin
from kent.driver.persistent_driver._workers import WorkerMixin
from kent.driver.persistent_driver.database import (
    init_database,
)
from kent.driver.persistent_driver.sql_manager import (
    Page,
    RequestRecord,
    ResponseRecord,
    ResultRecord,
    SQLManager,
)
from kent.driver.sync_driver import SpeculationState

# Re-export for public API
__all__ = [
    "PersistentDriver",
    "ProgressEvent",
    "DiagnoseResult",
    "Page",
    "RequestRecord",
    "ResponseRecord",
    "ResultRecord",
    "SQLManager",
]

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

logger = logging.getLogger(__name__)

ScraperReturnDatatype = TypeVar("ScraperReturnDatatype")


@dataclass
class ProgressEvent:
    """Event emitted during driver execution for real-time updates.

    Attributes:
        event_type: Type of event (request_started, request_completed, etc.)
        timestamp: When the event occurred.
        data: Event-specific data.
    """

    event_type: str
    timestamp: datetime
    data: dict[str, Any]

    def to_json(self) -> str:
        """Serialize to JSON for WebSocket transport."""
        return json.dumps(
            {
                "event_type": self.event_type,
                "timestamp": self.timestamp.isoformat(),
                "data": self.data,
            }
        )


class PersistentDriver(
    SpeculationMixin,
    QueueMixin,
    StorageMixin,
    WorkerMixin,
    APIMixin,
    AsyncDriver[ScraperReturnDatatype],
    Generic[ScraperReturnDatatype],
):
    """SQLite-backed async driver for local development.

    Extends AsyncDriver with:
    - Persistent request queue in SQLite
    - Response archival with compression
    - Resumability from graceful shutdown
    - Progress events for web interface integration
    - Adaptive Token Bucket (ATB) rate limiting

    Args:
        scraper: The scraper instance to run.
        db_path: Path to SQLite database file.
        storage_dir: Directory for downloaded files.
        initial_rate: Initial rate limit in requests/second (default: 0.1 = 6 req/min).
        bucket_size: Maximum tokens in the rate limiter bucket (default: 4.0).
        num_workers: Number of initial concurrent workers (default: 1).
        max_workers: Maximum workers for dynamic scaling (default: 10).
        resume: If True, resume from existing queue state (default: True).
        max_backoff_time: Maximum total backoff time before marking failed (default: 3600.0).

    Example:
        async with LocalDevDriver.open(scraper, db_path) as driver:
            driver.on_progress = lambda e: print(e.to_json())
            await driver.run()
    """

    def __init__(
        self,
        scraper: BaseScraper[ScraperReturnDatatype],
        db: SQLManager,
        storage_dir: Path | None = None,
        num_workers: int = 1,
        max_workers: int = 10,
        resume: bool = True,
        max_backoff_time: float = 3600.0,
        request_manager: Any | None = None,
        enable_monitor: bool = True,
    ) -> None:
        """Initialize the driver.

        Note: Use LocalDevDriver.open() for proper async initialization.

        Args:
            scraper: The scraper instance to run.
            db: SQLManager for database operations.
            storage_dir: Directory for downloaded files.
            num_workers: Number of initial concurrent workers.
            max_workers: Maximum workers for dynamic scaling.
            resume: If True, resume from existing queue state.
            max_backoff_time: Maximum total backoff time before marking failed.
            request_manager: AsyncRequestManager for handling HTTP requests.
            enable_monitor: If True (default), start the worker monitor for dynamic scaling.
                Set to False for tests that need the driver to exit quickly.
        """
        # Initialize parent with the request manager
        super().__init__(
            scraper=scraper,
            storage_dir=storage_dir,
            num_workers=num_workers,
            request_manager=request_manager,
        )

        self.resume = resume
        self.max_backoff_time = max_backoff_time
        self.max_workers = max_workers
        self.enable_monitor = enable_monitor

        self.db = db
        # Progress callback for web interface
        self.on_progress: Callable[[ProgressEvent], Awaitable[None]] | None = (
            None
        )

        # Stop event for graceful shutdown (always set, not optional like in parent)
        self.stop_event: asyncio.Event = asyncio.Event()

        # Worker management for dynamic scaling
        self._worker_tasks: dict[int, asyncio.Task[None]] = {}
        self._next_worker_id: int = 0
        self._monitor_task: asyncio.Task[None] | None = None

        # Speculation state - populated by _discover_speculate_functions (new @speculate pattern)
        self._speculation_state: dict[str, SpeculationState] = {}
        # Lock for speculation state updates from concurrent workers
        self._speculation_lock = asyncio.Lock()

    @classmethod
    @asynccontextmanager
    async def open(
        cls,
        scraper: BaseScraper[ScraperReturnDatatype],
        db_path: Path,
        **kwargs: Any,
    ) -> AsyncIterator[PersistentDriver[ScraperReturnDatatype]]:
        """Open driver as async context manager.

        Ensures proper initialization and cleanup of DB connections.

        Args:
            scraper: The scraper instance to run.
            db_path: Path to SQLite database file.
            **kwargs: Additional arguments passed to __init__.

        Yields:
            Initialized LocalDevDriver instance.

        Example:
            async with LocalDevDriver.open(scraper, db_path) as driver:
                await driver.run()
        """
        # Extract driver-specific kwargs for SQLManager initialization
        num_workers = kwargs.pop("num_workers", 1)
        max_workers = kwargs.pop("max_workers", 10)
        max_backoff_time = kwargs.pop("max_backoff_time", 3600.0)
        resume = kwargs.pop("resume", True)
        timeout = kwargs.pop("timeout", None)  # Request timeout in seconds
        custom_request_manager = kwargs.pop("request_manager", None)
        seed_params = kwargs.pop("seed_params", None)

        # Initialize database and SQLManager
        engine, session_factory = await init_database(db_path)
        sql_manager = SQLManager(engine, session_factory)

        # Initialize run metadata
        # Store full path as module:class_name format for registry lookup
        # e.g., "juriscraper.sd.state.connecticut.jud_ct_gov.scraper:ConnScraper"
        scraper_name = (
            f"{scraper.__class__.__module__}:{scraper.__class__.__name__}"
        )
        scraper_version = getattr(scraper, "__version__", None)
        await sql_manager.init_run_metadata(
            scraper_name=scraper_name,
            scraper_version=scraper_version,
            num_workers=num_workers,
            max_backoff_time=max_backoff_time,
            seed_params=seed_params,
        )

        # Restore queue if resuming
        if resume:
            pending_count = await sql_manager.restore_queue()
            if pending_count > 0:
                logger.info(
                    f"Restored {pending_count} pending requests from database"
                )

        # Use custom request manager if provided (e.g., for testing)
        # Otherwise, set up rate-limited request manager
        if custom_request_manager is not None:
            request_manager = custom_request_manager
        else:
            from kent.driver.persistent_driver.atb_rate_limiter import (
                RateLimitedRequestManager,
            )

            request_manager = RateLimitedRequestManager(
                sql_manager=sql_manager,
                rates=scraper.rate_limits,
                ssl_context=scraper.get_ssl_context(),
                timeout=timeout,
            )
            await request_manager.initialize()

        driver = cls(
            scraper,
            sql_manager,
            request_manager=request_manager,
            num_workers=num_workers,
            max_workers=max_workers,
            max_backoff_time=max_backoff_time,
            resume=resume,
            **kwargs,
        )

        try:
            yield driver
        finally:
            await driver.close()

    async def close(self) -> None:
        """Close DB connections and clean up resources.

        On close, if there are any in_progress requests, reset them to pending
        so they can be resumed on next startup. Also mark run as interrupted
        if it was running.
        """
        # Persist speculation state before closing
        for func_name, spec_state in self._speculation_state.items():
            await self.db.save_speculation_state(
                func_name=func_name,
                highest_successful_id=spec_state.highest_successful_id,
                consecutive_failures=spec_state.consecutive_failures,
                current_ceiling=spec_state.current_ceiling,
                stopped=spec_state.stopped,
            )

        if self.db:
            await self.db.close_run()
            await self.db.engine.dispose()

    # --- Progress Events ---

    async def _emit_progress(
        self, event_type: str, data: dict[str, Any]
    ) -> None:
        """Emit a progress event if callback is registered.

        Args:
            event_type: Type of event.
            data: Event-specific data.
        """
        if self.on_progress:
            event = ProgressEvent(
                event_type=event_type,
                timestamp=datetime.now(timezone.utc),
                data=data,
            )
            await self.on_progress(event)

    # --- Signal Handlers ---

    def _setup_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown.

        Registers handlers for SIGINT (Ctrl+C) and SIGTERM that will
        set the stop_event, causing workers to finish their current
        request and exit gracefully.

        Note: Only works on Unix-like systems. On Windows, only SIGINT
        is supported.
        """
        import signal

        def handle_signal(signum: int, frame: Any) -> None:
            sig_name = signal.Signals(signum).name
            logger.info(
                f"Received {sig_name}, initiating graceful shutdown..."
            )
            self.stop()

        # Register handlers
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

    def _restore_signal_handlers(self) -> None:
        """Restore default signal handlers.

        Should be called after run() completes to avoid leaving
        custom handlers in place.
        """
        import signal

        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    # --- Run ---

    async def run(self, setup_signal_handlers: bool = True) -> None:
        """Run the scraper, using DB-backed queue.

        Overrides AsyncDriver.run() to use database queue operations.

        Args:
            setup_signal_handlers: If True, register SIGINT/SIGTERM handlers
                for graceful shutdown. Set to False when running in a context
                that manages its own signal handling (e.g., FastAPI).
        """
        if setup_signal_handlers:
            self._setup_signal_handlers()

        # Update run status to running
        await self.db.update_run_status("running")

        # Apply any speculative start IDs from the database to the scraper params
        # This is used by the restart-speculative feature
        await self._apply_speculative_start_ids()

        await self._emit_progress(
            "run_started",
            {
                "scraper_name": self.scraper.__class__.__name__,
            },
        )

        status = "completed"
        error: Exception | None = None

        try:
            # Check for early stop before doing any work
            if self.stop_event.is_set():
                return

            # Check if we need to seed the queue with entry point
            has_requests = await self.db.has_any_requests()

            # Load seed_params once (used for both entry and speculation filtering)
            seed_params = await self.db.get_seed_params()

            if not has_requests:
                # Seed queue with entry points.
                # If seed_params were stored (from web UI selection),
                # use them to run only the selected entries.
                if seed_params is not None:
                    # Filter out speculative entries — they're handled
                    # separately by the speculation system below.
                    non_spec = [
                        inv
                        for inv in seed_params
                        if not any(
                            e.speculative
                            for e in self.scraper.list_entries()
                            if e.name in inv
                        )
                    ]
                    if non_spec:
                        entry_requests = self.scraper.initial_seed(non_spec)
                    else:
                        # Only speculative entries selected; skip
                        # initial_seed() which requires ≥1 invocation.
                        entry_requests = iter(())  # type: ignore[assignment]
                else:
                    entry_requests = self._get_entry_requests()
                for entry_request in entry_requests:
                    request_data = self._serialize_request(entry_request)
                    dedup_key = (
                        entry_request.deduplication_key
                        if isinstance(entry_request.deduplication_key, str)
                        else None
                    )

                    await self.db.insert_entry_request(
                        priority=entry_request.priority,
                        method=request_data["method"],
                        url=request_data["url"],
                        headers_json=request_data["headers_json"],
                        cookies_json=request_data["cookies_json"],
                        body=request_data["body"],
                        continuation=request_data["continuation"],
                        current_location=request_data["current_location"],
                        accumulated_data_json=request_data[
                            "accumulated_data_json"
                        ],
                        aux_data_json=request_data["aux_data_json"],
                        permanent_json=request_data["permanent_json"],
                        dedup_key=dedup_key,
                    )

            # Discover @speculate functions and seed the queue
            self._speculation_state = self._discover_speculate_functions()

            # If seed_params specified, remove speculative entries
            # that weren't selected in the web UI.
            if seed_params is not None and self._speculation_state:
                selected_entries = {
                    name for inv in seed_params for name in inv
                }
                to_remove = [
                    key
                    for key, state in self._speculation_state.items()
                    if state.base_func_name not in selected_entries
                ]
                for key in to_remove:
                    del self._speculation_state[key]

            if self._speculation_state:
                # Load any persisted state from previous run
                await self._load_speculation_state_from_db()
                # Seed the queue with speculative requests
                await self._seed_speculative_queue()

            # Start initial workers
            logger.info(
                f"Starting {self.num_workers} initial workers (max: {self.max_workers})"
            )
            for _ in range(self.num_workers):
                self._spawn_worker()

            # Start the worker monitor for dynamic scaling (if enabled)
            if self.enable_monitor:
                self._monitor_task = asyncio.create_task(
                    self._worker_monitor()
                )

            # Wait for all workers and monitor to complete
            # Workers exit when queue is empty or stop_event is set
            # Monitor exits when no workers remain and no pending work
            while self._worker_tasks or (
                self._monitor_task and not self._monitor_task.done()
            ):
                # Gather current tasks (workers + monitor if still running)
                tasks_to_wait: list[asyncio.Task[None]] = list(
                    self._worker_tasks.values()
                )
                if self._monitor_task and not self._monitor_task.done():
                    tasks_to_wait.append(self._monitor_task)

                if not tasks_to_wait:
                    break

                # Wait for any task to complete
                done, _ = await asyncio.wait(
                    tasks_to_wait,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Check for exceptions in completed tasks
                for task in done:
                    if (
                        task.exception() is not None
                        and task is not self._monitor_task
                    ):
                        # Re-raise worker exceptions
                        raise task.exception()  # type: ignore[misc]

        except Exception as e:
            status = "error"
            error = e
            raise
        finally:
            # Cancel monitor if still running
            if self._monitor_task and not self._monitor_task.done():
                self._monitor_task.cancel()
                try:
                    await self._monitor_task
                except asyncio.CancelledError:
                    pass

            # Restore signal handlers if we set them up
            if setup_signal_handlers:
                self._restore_signal_handlers()

            # Update run metadata
            final_status = (
                "interrupted" if self.stop_event.is_set() else status
            )
            await self.db.finalize_run(
                final_status, str(error) if error else None
            )

            await self._emit_progress(
                "run_completed",
                {
                    "scraper_name": self.scraper.__class__.__name__,
                    "status": final_status,
                    "error": str(error) if error else None,
                },
            )

    # --- Status ---

    async def status(self) -> Literal["unstarted", "in_progress", "done"]:
        """Check the current state of the scraper run.

        Returns:
            - "unstarted": No requests in DB
            - "in_progress": Pending or in_progress requests exist
            - "done": No pending/in_progress but completed requests exist
        """
        return await self.db.get_run_status()

    def stop(self) -> None:
        """Signal workers to stop after completing their current request."""
        self.stop_event.set()
