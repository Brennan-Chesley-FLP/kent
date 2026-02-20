"""FastAPI application for LocalDevDriver web interface.

This module provides the main FastAPI application with:
- Lifespan context manager for startup/shutdown
- RunManager for tracking active scraper runs
- Graceful shutdown support for hot reload
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from kent.driver.persistent_driver.debugger import (
        LocalDevDriverDebugger,
    )
    from kent.driver.persistent_driver.persistent_driver import (
        PersistentDriver,
    )
    from kent.driver.persistent_driver.sql_manager import (
        SQLManager,
    )

# Configure logging for driver module to show worker logs
# This ensures logs appear even when running uvicorn directly
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("kent.driver.persistent_driver").setLevel(logging.INFO)

logger = logging.getLogger(__name__)


@dataclass
class RunInfo:
    """Information about a scraper run.

    Attributes:
        run_id: Unique identifier for the run (database filename without .db).
        db_path: Path to the SQLite database file.
        driver: The LocalDevDriver instance (if loaded).
        task: The asyncio task running the driver (if running).
        status: Current status (unloaded, loaded, running, stopping, stopped).
        created_at: When this run info was created.
        started_at: When the run was started (if running).
    """

    run_id: str
    db_path: Path
    driver: PersistentDriver[Any] | None = None
    task: asyncio.Task[None] | None = None
    status: str = "unloaded"
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    started_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "run_id": self.run_id,
            "db_path": str(self.db_path),
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat()
            if self.started_at
            else None,
        }


class RunManager:
    """Manager for tracking and controlling scraper runs.

    Watches a runs directory for database files and tracks active
    driver instances. Supports graceful shutdown for hot reload.

    Attributes:
        runs_dir: Directory containing run database files.
        runs: Dictionary mapping run_id to RunInfo.
    """

    def __init__(self, runs_dir: Path) -> None:
        """Initialize the run manager.

        Args:
            runs_dir: Directory to watch for run database files.
        """
        self.runs_dir = runs_dir
        self.runs: dict[str, RunInfo] = {}
        self._lock = asyncio.Lock()

    async def scan_runs(self) -> list[str]:
        """Scan runs directory for database files.

        Returns:
            List of discovered run_ids.
        """
        async with self._lock:
            discovered: list[str] = []

            if not self.runs_dir.exists():
                self.runs_dir.mkdir(parents=True, exist_ok=True)
                return discovered

            for db_file in self.runs_dir.glob("*.db"):
                run_id = db_file.stem
                if run_id not in self.runs:
                    self.runs[run_id] = RunInfo(
                        run_id=run_id,
                        db_path=db_file,
                        status="unloaded",
                    )
                discovered.append(run_id)

            return discovered

    async def list_runs(self) -> list[RunInfo]:
        """List all known runs.

        Returns:
            List of RunInfo objects.
        """
        async with self._lock:
            return list(self.runs.values())

    async def get_run(self, run_id: str) -> RunInfo | None:
        """Get info for a specific run.

        Args:
            run_id: The run identifier.

        Returns:
            RunInfo or None if not found.
        """
        async with self._lock:
            return self.runs.get(run_id)

    async def create_run(
        self,
        run_id: str,
        scraper: Any,
        **driver_kwargs: Any,
    ) -> RunInfo:
        """Create a new run with a fresh database.

        Args:
            run_id: Unique identifier for the run.
            scraper: The scraper instance to run.
            **driver_kwargs: Additional arguments for LocalDevDriver.

        Returns:
            RunInfo for the new run.

        Raises:
            ValueError: If run_id already exists.
        """
        from kent.driver.persistent_driver.atb_rate_limiter import (
            RateLimitedRequestManager,
        )
        from kent.driver.persistent_driver.database import (
            init_database,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )
        from kent.driver.persistent_driver.sql_manager import (
            SQLManager,
        )
        from kent.driver.persistent_driver.web.archive import (
            get_storage_dir_for_run,
            uuid_archive_callback,
        )

        async with self._lock:
            if run_id in self.runs:
                raise ValueError(f"Run '{run_id}' already exists")

            db_path = self.runs_dir / f"{run_id}.db"

            # Set up storage directory for archived files
            storage_dir = get_storage_dir_for_run(self.runs_dir, run_id)

            # Extract config from driver_kwargs
            num_workers = driver_kwargs.get("num_workers", 1)
            max_backoff_time = driver_kwargs.get("max_backoff_time", 3600.0)
            speculation_config = driver_kwargs.pop("speculation_config", None)

            # Initialize database and SQLManager
            engine, session_factory = await init_database(db_path)
            sql_manager = SQLManager(engine, session_factory)

            # Initialize run metadata
            # Use __module__ to get full path (e.g., juriscraper.opinions...ca1)
            # This is needed for the debugger's compare command to import the scraper
            scraper_name = scraper.__class__.__module__
            scraper_version = getattr(scraper, "__version__", None)
            await sql_manager.init_run_metadata(
                scraper_name=scraper_name,
                scraper_version=scraper_version,
                num_workers=num_workers,
                max_backoff_time=max_backoff_time,
                speculation_config=speculation_config,
            )

            # Set up rate-limited request manager
            request_manager = RateLimitedRequestManager(
                sql_manager=sql_manager,
                rates=scraper.rate_limits,
                ssl_context=scraper.get_ssl_context(),
            )
            await request_manager.initialize()

            # Create driver with SQLManager and request manager
            driver = PersistentDriver(
                scraper=scraper,
                db=sql_manager,
                storage_dir=storage_dir,
                request_manager=request_manager,
                **driver_kwargs,
            )

            # Set the custom archive callback
            driver.on_archive = uuid_archive_callback

            run_info = RunInfo(
                run_id=run_id,
                db_path=db_path,
                driver=driver,
                status="loaded",
            )
            self.runs[run_id] = run_info

            logger.info(f"Created run '{run_id}' at {db_path}")
            return run_info

    async def load_run(
        self, run_id: str, scraper: Any, **driver_kwargs: Any
    ) -> RunInfo:
        """Load an existing run from database.

        Args:
            run_id: The run identifier.
            scraper: The scraper instance to use.
            **driver_kwargs: Additional arguments for LocalDevDriver.

        Returns:
            Updated RunInfo.

        Raises:
            ValueError: If run_id not found or already loaded.
        """
        from kent.driver.persistent_driver.atb_rate_limiter import (
            RateLimitedRequestManager,
        )
        from kent.driver.persistent_driver.database import (
            init_database,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )
        from kent.driver.persistent_driver.sql_manager import (
            SQLManager,
        )
        from kent.driver.persistent_driver.web.archive import (
            get_storage_dir_for_run,
            uuid_archive_callback,
        )

        # Close any read-only connection before loading (driver will take over)
        await close_readonly_connection(run_id)

        async with self._lock:
            if run_id not in self.runs:
                raise ValueError(f"Run '{run_id}' not found")

            run_info = self.runs[run_id]
            if run_info.driver is not None:
                raise ValueError(f"Run '{run_id}' is already loaded")

            # Set up storage directory for archived files
            storage_dir = get_storage_dir_for_run(self.runs_dir, run_id)

            # Extract config from driver_kwargs
            num_workers = driver_kwargs.get("num_workers", 1)
            max_backoff_time = driver_kwargs.get("max_backoff_time", 3600.0)
            speculation_config = driver_kwargs.pop("speculation_config", None)

            # Initialize database and SQLManager
            engine, session_factory = await init_database(run_info.db_path)
            sql_manager = SQLManager(engine, session_factory)

            # Initialize run metadata (for existing runs, this updates status)
            # Use __module__ to get full path (e.g., juriscraper.opinions...ca1)
            # This is needed for the debugger's compare command to import the scraper
            scraper_name = scraper.__class__.__module__
            scraper_version = getattr(scraper, "__version__", None)
            await sql_manager.init_run_metadata(
                scraper_name=scraper_name,
                scraper_version=scraper_version,
                num_workers=num_workers,
                max_backoff_time=max_backoff_time,
            )

            # Get speculation config - prefer provided, fall back to stored
            if speculation_config is None:
                speculation_config = await sql_manager.get_speculation_config()

            # Restore queue since we're resuming
            pending_count = await sql_manager.restore_queue()
            if pending_count > 0:
                logger.info(
                    f"Restored {pending_count} pending requests from database"
                )

            # Set up rate-limited request manager
            request_manager = RateLimitedRequestManager(
                sql_manager=sql_manager,
                rates=scraper.rate_limits,
                ssl_context=scraper.get_ssl_context(),
            )
            await request_manager.initialize()

            # Load driver with resume=True and custom archive handler
            driver = PersistentDriver(
                scraper=scraper,
                db=sql_manager,
                storage_dir=storage_dir,
                resume=True,
                request_manager=request_manager,
                **driver_kwargs,
            )

            # Set the custom archive callback
            driver.on_archive = uuid_archive_callback

            run_info.driver = driver
            run_info.status = "loaded"

            logger.info(f"Loaded run '{run_id}'")
            return run_info

    async def start_run(self, run_id: str) -> RunInfo:
        """Start running a loaded driver.

        Args:
            run_id: The run identifier.

        Returns:
            Updated RunInfo.

        Raises:
            ValueError: If run not loaded or already running.
        """
        async with self._lock:
            if run_id not in self.runs:
                raise ValueError(f"Run '{run_id}' not found")

            run_info = self.runs[run_id]
            if run_info.driver is None:
                raise ValueError(f"Run '{run_id}' is not loaded")
            if run_info.task is not None and not run_info.task.done():
                raise ValueError(f"Run '{run_id}' is already running")

            # Create task to run the driver
            async def run_driver() -> None:
                assert run_info.driver is not None
                try:
                    # Don't set up signal handlers - FastAPI manages those
                    await run_info.driver.run(setup_signal_handlers=False)
                except asyncio.CancelledError:
                    logger.info(f"Run '{run_id}' was cancelled")
                except Exception as e:
                    logger.exception(f"Run '{run_id}' failed: {e}")
                finally:
                    async with self._lock:
                        run_info.status = "stopped"

            run_info.task = asyncio.create_task(run_driver())
            run_info.status = "running"
            run_info.started_at = datetime.now(timezone.utc)

            logger.info(f"Started run '{run_id}'")
            return run_info

    async def resume_run(
        self, run_id: str, scraper: Any, **driver_kwargs: Any
    ) -> RunInfo:
        """Load and immediately start a run (combined operation).

        This is the preferred way to start an existing run. It combines
        load_run and start_run into a single atomic operation.

        Args:
            run_id: The run identifier.
            scraper: The scraper instance to use.
            **driver_kwargs: Additional arguments for LocalDevDriver.

        Returns:
            RunInfo with status="running".

        Raises:
            ValueError: If run not found.
        """
        # Check if already running
        run_info = await self.get_run(run_id)
        if run_info is not None:
            if run_info.task is not None and not run_info.task.done():
                # Already running, return current state
                return run_info

            if run_info.driver is not None:
                # Already loaded but not running, just start it
                return await self.start_run(run_id)

        # Load and then start
        await self.load_run(run_id, scraper, **driver_kwargs)
        return await self.start_run(run_id)

    async def stop_run(self, run_id: str, timeout: float = 30.0) -> RunInfo:
        """Stop a running driver gracefully.

        Args:
            run_id: The run identifier.
            timeout: Timeout in seconds to wait for graceful stop.

        Returns:
            Updated RunInfo.

        Raises:
            ValueError: If run not found or not running.
        """
        async with self._lock:
            if run_id not in self.runs:
                raise ValueError(f"Run '{run_id}' not found")

            run_info = self.runs[run_id]
            if run_info.task is None or run_info.task.done():
                raise ValueError(f"Run '{run_id}' is not running")

            run_info.status = "stopping"

        # Signal stop and wait (outside lock)
        assert run_info.driver is not None
        run_info.driver.stop()

        try:
            await asyncio.wait_for(run_info.task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                f"Run '{run_id}' did not stop gracefully, cancelling"
            )
            run_info.task.cancel()
            try:
                await run_info.task
            except asyncio.CancelledError:
                pass

        async with self._lock:
            run_info.status = "stopped"

        logger.info(f"Stopped run '{run_id}'")
        return run_info

    async def unload_run(self, run_id: str) -> None:
        """Unload a run, closing its driver connection.

        Args:
            run_id: The run identifier.

        Raises:
            ValueError: If run not found or still running.
        """
        async with self._lock:
            if run_id not in self.runs:
                raise ValueError(f"Run '{run_id}' not found")

            run_info = self.runs[run_id]
            if run_info.task is not None and not run_info.task.done():
                raise ValueError(f"Run '{run_id}' is still running")

            if run_info.driver is not None:
                await run_info.driver.close()
                run_info.driver = None

            run_info.status = "unloaded"

        logger.info(f"Unloaded run '{run_id}'")

    async def delete_run(self, run_id: str) -> None:
        """Delete a run and its database file.

        Args:
            run_id: The run identifier.

        Raises:
            ValueError: If run not found or still running.
        """
        async with self._lock:
            if run_id not in self.runs:
                raise ValueError(f"Run '{run_id}' not found")

            run_info = self.runs[run_id]
            if run_info.task is not None and not run_info.task.done():
                raise ValueError(f"Run '{run_id}' is still running")

            if run_info.driver is not None:
                await run_info.driver.close()

            # Delete database file
            if run_info.db_path.exists():
                run_info.db_path.unlink()
                # Also delete WAL and SHM files if they exist
                for suffix in ["-wal", "-shm"]:
                    wal_path = run_info.db_path.with_suffix(f".db{suffix}")
                    if wal_path.exists():
                        wal_path.unlink()

            del self.runs[run_id]

        logger.info(f"Deleted run '{run_id}'")

    async def shutdown_all(self, timeout: float = 30.0) -> None:
        """Stop all running drivers and close connections.

        Used for graceful shutdown during hot reload.

        Args:
            timeout: Timeout in seconds to wait for each driver.
        """
        logger.info("Shutting down all runs...")

        # Get list of running runs
        async with self._lock:
            running_runs = [
                run_id
                for run_id, run_info in self.runs.items()
                if run_info.task is not None and not run_info.task.done()
            ]

        # Stop running runs
        for run_id in running_runs:
            try:
                await self.stop_run(run_id, timeout=timeout)
            except Exception as e:
                logger.warning(f"Error stopping run '{run_id}': {e}")

        # Unload all runs
        async with self._lock:
            for run_id, run_info in self.runs.items():
                if run_info.driver is not None:
                    try:
                        await run_info.driver.close()
                        run_info.driver = None
                        run_info.status = "unloaded"
                    except Exception as e:
                        logger.warning(f"Error closing run '{run_id}': {e}")

        logger.info("All runs shut down")


# Global run manager instance (set during lifespan)
_run_manager: RunManager | None = None


# --- Read-only connection cache for unloaded runs ---


@dataclass
class CachedConnection:
    """A cached read-only SQLManager connection.

    Attributes:
        manager: The SQLManager instance.
        engine: The AsyncEngine instance for disposal.
        last_used: Unix timestamp of last access.
    """

    manager: SQLManager
    engine: Any  # AsyncEngine
    last_used: float


# Cache for read-only SQLManager connections
_readonly_connections: dict[str, CachedConnection] = {}
_readonly_lock = asyncio.Lock()
_cleanup_task: asyncio.Task[None] | None = None
READONLY_TIMEOUT = 30.0  # seconds


async def _cleanup_stale_connections() -> None:
    """Background task to close idle read-only connections."""
    while True:
        await asyncio.sleep(10)  # Check every 10s
        async with _readonly_lock:
            now = time.time()
            stale = [
                run_id
                for run_id, cached in _readonly_connections.items()
                if now - cached.last_used > READONLY_TIMEOUT
            ]
            for run_id in stale:
                try:
                    await _readonly_connections[run_id].engine.dispose()
                    logger.debug(
                        f"Closed stale read-only connection for '{run_id}'"
                    )
                except Exception as e:
                    logger.warning(
                        f"Error closing read-only connection for '{run_id}': {e}"
                    )
                del _readonly_connections[run_id]


async def get_readonly_sql_manager(run_id: str, db_path: Path) -> SQLManager:
    """Get or create a read-only SQLManager for a run.

    These connections are cached and automatically closed after 30s of
    inactivity to avoid resource leaks while still providing efficient
    repeated access.

    Args:
        run_id: The run identifier.
        db_path: Path to the database file.

    Returns:
        SQLManager instance for read-only access.
    """
    from kent.driver.persistent_driver.database import (
        init_database,
    )
    from kent.driver.persistent_driver.sql_manager import (
        SQLManager,
    )

    global _cleanup_task
    async with _readonly_lock:
        # Start cleanup task if not running
        if _cleanup_task is None or _cleanup_task.done():
            _cleanup_task = asyncio.create_task(_cleanup_stale_connections())

        # Return cached connection if available
        if run_id in _readonly_connections:
            _readonly_connections[run_id].last_used = time.time()
            return _readonly_connections[run_id].manager

        # Create new connection
        engine, session_factory = await init_database(db_path)
        manager = SQLManager(engine, session_factory)
        _readonly_connections[run_id] = CachedConnection(
            manager=manager, engine=engine, last_used=time.time()
        )
        logger.debug(f"Opened read-only connection for '{run_id}'")
        return manager


async def close_readonly_connection(run_id: str) -> None:
    """Close a read-only connection if it exists.

    Called when a run is loaded (driver takes over) or deleted.

    Args:
        run_id: The run identifier.
    """
    async with _readonly_lock:
        if run_id in _readonly_connections:
            try:
                await _readonly_connections[run_id].engine.dispose()
            except Exception as e:
                logger.warning(
                    f"Error closing read-only connection for '{run_id}': {e}"
                )
            del _readonly_connections[run_id]


async def shutdown_readonly_connections() -> None:
    """Close all read-only connections. Called during app shutdown."""
    global _cleanup_task
    async with _readonly_lock:
        for run_id, cached in list(_readonly_connections.items()):
            try:
                await cached.engine.dispose()
            except Exception as e:
                logger.warning(
                    f"Error closing read-only connection for '{run_id}': {e}"
                )
        _readonly_connections.clear()

    # Cancel cleanup task
    if _cleanup_task is not None and not _cleanup_task.done():
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
        _cleanup_task = None


def get_run_manager() -> RunManager:
    """Get the global run manager instance.

    Returns:
        The RunManager instance.

    Raises:
        RuntimeError: If run manager not initialized.
    """
    if _run_manager is None:
        raise RuntimeError("Run manager not initialized")
    return _run_manager


async def get_sql_manager_for_run(
    run_id: str, manager: RunManager
) -> SQLManager:
    """Get SQLManager for a run, opening DB if not already loaded.

    This function provides database access for runs without requiring
    the full driver to be loaded. For loaded runs, it uses the driver's
    existing database connection. For unloaded runs, it uses a cached
    read-only connection that auto-closes after 30s of inactivity.

    Args:
        run_id: The run identifier.
        manager: The run manager.

    Returns:
        SQLManager instance for the run.

    Raises:
        ValueError: If run not found.
    """
    run_info = await manager.get_run(run_id)
    if run_info is None:
        raise ValueError(f"Run '{run_id}' not found")

    # If driver is loaded, use its SQLManager
    if run_info.driver is not None:
        return run_info.driver.db

    # Otherwise, use the read-only connection cache
    return await get_readonly_sql_manager(run_id, run_info.db_path)


async def get_debugger_for_run(
    run_id: str, manager: RunManager, read_only: bool = True
) -> LocalDevDriverDebugger:
    """Get LocalDevDriverDebugger instance for a run.

    This provides a high-level API for inspecting and manipulating run databases
    without requiring the full driver to be loaded. Uses the same connection
    pooling as get_sql_manager_for_run.

    Args:
        run_id: The run identifier.
        manager: The run manager.
        read_only: If True, open in read-only mode (prevents writes).
                   Set to False for operations like requeue, cancel, etc.

    Returns:
        LocalDevDriverDebugger instance wrapping the SQLManager.

    Raises:
        ValueError: If run not found.
    """
    from kent.driver.persistent_driver.debugger import (
        LocalDevDriverDebugger,
    )

    sql_manager = await get_sql_manager_for_run(run_id, manager)

    # Wrap the SQL manager with LDDD
    # Note: We use the read_only parameter to control whether write operations
    # are allowed. The actual database connection mode is determined by
    # whether the driver is loaded (writeable) or using readonly cache.
    run_info = await manager.get_run(run_id)
    is_loaded = run_info is not None and run_info.driver is not None

    # If driver is loaded, allow writes even if read_only=True is requested
    # (the driver owns the connection and can write)
    # If driver is not loaded, respect the read_only parameter
    effective_read_only = read_only if not is_loaded else False

    return LocalDevDriverDebugger(
        sql_manager,
        sql_manager._session_factory,
        read_only=effective_read_only,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Lifespan context manager for FastAPI app.

    Handles startup (scan runs directory, scan scrapers) and shutdown (stop all runs).
    """
    global _run_manager

    # Get runs directory from app state or use default
    runs_dir = getattr(app.state, "runs_dir", Path("runs"))

    # Initialize run manager
    _run_manager = RunManager(runs_dir)

    # Scan for existing runs
    discovered = await _run_manager.scan_runs()
    logger.info(f"Discovered {len(discovered)} existing runs")

    # Initialize scraper registry
    from kent.driver.persistent_driver.web.scraper_registry import (
        init_registry,
    )

    sd_dir = getattr(app.state, "sd_dir", None)
    registry = init_registry(sd_dir)
    logger.info(f"Discovered {len(registry.list_scrapers())} scrapers")

    yield

    # Shutdown all runs
    await _run_manager.shutdown_all()
    _run_manager = None

    # Shutdown read-only connections
    await shutdown_readonly_connections()


def create_app(
    runs_dir: Path | None = None, sd_dir: Path | None = None
) -> FastAPI:
    """Create a new FastAPI application.

    Args:
        runs_dir: Directory for run database files. Defaults to "runs".
        sd_dir: Directory containing scrapers. Defaults to juriscraper/sd.

    Returns:
        Configured FastAPI application.
    """
    from fastapi.staticfiles import StaticFiles

    from kent.driver.persistent_driver.web.routes import (
        archived_files_router,
        compression_router,
        debug_router,
        errors_router,
        export_router,
        rate_limiter_router,
        requests_router,
        responses_router,
        results_router,
        runs_router,
        scrapers_router,
        views_router,
        websocket_router,
    )

    app = FastAPI(
        title="LocalDevDriver Web Interface",
        description="Web interface for managing scraper runs",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Store config in app state for lifespan access
    app.state.runs_dir = runs_dir or Path("runs")
    app.state.sd_dir = sd_dir

    # Mount static files
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount(
            "/static", StaticFiles(directory=str(static_dir)), name="static"
        )

    # Include API routers
    app.include_router(scrapers_router)
    app.include_router(runs_router)
    app.include_router(requests_router)
    app.include_router(responses_router)
    app.include_router(results_router)
    app.include_router(errors_router)
    app.include_router(compression_router)
    app.include_router(export_router)
    app.include_router(debug_router)
    app.include_router(archived_files_router)
    app.include_router(rate_limiter_router)
    app.include_router(websocket_router)

    # Include view routers (HTML pages) - must be last to avoid route conflicts
    app.include_router(views_router)

    return app


# Default app instance
app = create_app()
