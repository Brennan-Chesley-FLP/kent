"""Database engine and session management for the persistent_driver.

This module replaces schema.py for connection management, providing
async SQLAlchemy engine creation and session factory configuration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, select

from kent.driver.persistent_driver.models import *  # noqa: F401, F403
from kent.driver.persistent_driver.models import Request, SchemaInfo

SCHEMA_VERSION = 13


async def create_engine_and_init(
    db_path: Path,
    echo: bool = False,
) -> AsyncEngine:
    """Create an async engine and initialize the database schema.

    Creates all tables if they don't exist. Configures WAL mode and
    foreign keys via connection event listeners.

    Args:
        db_path: Path to the SQLite database file.
        echo: Whether to echo SQL statements (for debugging).

    Returns:
        An initialized AsyncEngine.
    """
    url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(
        url,
        echo=echo,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_conn: Any, connection_record: Any) -> None:
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    return engine


def get_session_factory(
    engine: AsyncEngine,
) -> async_sessionmaker:
    """Create a session factory bound to the engine.

    Args:
        engine: The async engine to bind sessions to.

    Returns:
        An async session factory.
    """
    return async_sessionmaker(engine, expire_on_commit=False)


async def init_database(
    db_path: Path,
    echo: bool = False,
) -> tuple[AsyncEngine, async_sessionmaker]:
    """Initialize database and return engine + session factory.

    This is the main entry point, replacing schema.init_database().

    Args:
        db_path: Path to the SQLite database file.
        echo: Whether to echo SQL statements.

    Returns:
        Tuple of (engine, session_factory).
    """
    engine = await create_engine_and_init(db_path, echo=echo)
    session_factory = get_session_factory(engine)
    return engine, session_factory


async def get_schema_version(session: AsyncSession) -> int:
    """Get the current schema version from the database.

    Args:
        session: An async database session.

    Returns:
        The current schema version number, or 0 if not initialized.
    """
    try:
        result = await session.execute(
            select(SchemaInfo.version)
            .order_by(SchemaInfo.version.desc())  # type: ignore[attr-defined]
            .limit(1)
        )
        row = result.first()
        return row[0] if row else 0
    except Exception:
        return 0


async def get_next_queue_counter(session: AsyncSession) -> int:
    """Get the next queue counter value for FIFO ordering.

    Args:
        session: An async database session.

    Returns:
        The next queue_counter value (max + 1, or 1 if empty).
    """
    result = await session.execute(select(sa.func.max(Request.queue_counter)))
    row = result.first()
    return (row[0] or 0) + 1
