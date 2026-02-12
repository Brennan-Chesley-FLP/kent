"""SQLManagerBase - Core initialization and connection management."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from typing_extensions import Self

from kent.driver.dev_driver.database import init_database
from kent.driver.dev_driver.models import Request

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class SQLManagerBase:
    """Core database connection and initialization for SQLManager.

    Provides the shared engine, session factory, and lock that all
    mixin classes depend on.

    Example::

        # Standalone usage for inspection
        async with SQLManager.open(db_path) as manager:
            stats = await manager.get_stats()
            requests = await manager.list_requests(status="pending")

        # With existing engine/session factory (for driver integration)
        manager = SQLManager(engine, session_factory)
        await manager.store_response(request_id, response, continuation)
    """

    def __init__(
        self,
        engine: AsyncEngine,
        session_factory: async_sessionmaker,
    ) -> None:
        """Initialize with an engine and session factory.

        Args:
            engine: An async SQLAlchemy engine.
            session_factory: An async session factory bound to the engine.
        """
        self._engine = engine
        self._session_factory = session_factory
        self._lock = asyncio.Lock()

    @classmethod
    @asynccontextmanager
    async def open(cls, db_path: Path) -> AsyncIterator[Self]:
        """Open a database and create a SQLManager.

        This is the preferred way to create a SQLManager for standalone usage.
        Ensures proper initialization and cleanup.

        Args:
            db_path: Path to the SQLite database file.

        Yields:
            SQLManager instance.

        Example::

            async with SQLManager.open(db_path) as manager:
                stats = await manager.get_stats()
        """
        engine, session_factory = await init_database(db_path)
        try:
            yield cls(engine, session_factory)
        finally:
            await engine.dispose()

    @property
    def engine(self) -> AsyncEngine:
        """Get the underlying async engine."""
        return self._engine

    async def _get_next_queue_counter(self) -> int:
        """Get the next queue counter value for FIFO ordering."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(func.max(Request.queue_counter))
            )
            max_val = result.scalar()
            return (max_val or 0) + 1
