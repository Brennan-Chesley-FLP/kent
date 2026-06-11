"""Statistics dataclasses and queries for a run database.

``get_stats`` aggregates queue/throughput/result/error statistics for a run;
en_banc polls it for periodic progress logging.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlmodel import col, select

from jkent.driver.database_engine.models import (
    Error,
    Request,
    Result,
    RunMetadata,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@dataclass
class QueueStats:
    """Statistics about the request queue.

    Attributes:
        pending: Number of pending requests.
        in_progress: Number of requests currently being processed.
        completed: Number of successfully completed requests.
        failed: Number of failed requests.
        held: Number of held (paused) requests.
        total: Total number of requests.
        by_continuation: Counts by continuation method name.
    """

    pending: int = 0
    in_progress: int = 0
    completed: int = 0
    failed: int = 0
    held: int = 0
    total: int = 0
    by_continuation: dict[str, dict[str, int]] = field(default_factory=dict)


@dataclass
class ThroughputStats:
    """Statistics about request throughput.

    Attributes:
        total_completed: Total requests completed.
        total_duration_seconds: Total duration from first to last request.
        requests_per_minute: Average requests completed per minute.
        average_response_time_seconds: Average time between start and completion.
    """

    total_completed: int = 0
    total_duration_seconds: float = 0.0
    requests_per_minute: float = 0.0
    average_response_time_seconds: float = 0.0


@dataclass
class ResultStats:
    """Statistics about scraped results.

    Attributes:
        total: Total number of results.
        valid: Number of valid results.
        invalid: Number of invalid results.
        by_type: Counts by result type (Pydantic model name).
    """

    total: int = 0
    valid: int = 0
    invalid: int = 0
    by_type: dict[str, int] = field(default_factory=dict)


@dataclass
class ErrorStats:
    """Statistics about errors.

    Attributes:
        total: Total number of errors.
        unresolved: Number of unresolved errors.
        resolved: Number of resolved errors.
        by_type: Counts by error type (structural, validation, transient).
        by_continuation: Counts by continuation method name.
    """

    total: int = 0
    unresolved: int = 0
    resolved: int = 0
    by_type: dict[str, int] = field(default_factory=dict)
    by_continuation: dict[str, int] = field(default_factory=dict)


@dataclass
class DevDriverStats:
    """Combined statistics for a run.

    Attributes:
        queue: Queue statistics.
        throughput: Throughput statistics.
        results: Result statistics.
        errors: Error statistics.
        run_status: Current run status.
        scraper_name: Name of the scraper.
    """

    queue: QueueStats
    throughput: ThroughputStats
    results: ResultStats
    errors: ErrorStats
    run_status: str = "unknown"
    scraper_name: str = ""


async def _queue_stats(session: AsyncSession) -> QueueStats:
    """Compute queue stats on an existing session."""
    # Get counts by status
    result = await session.execute(
        select(col(Request.status), sa.func.count()).group_by(
            col(Request.status)
        )
    )
    rows = result.all()

    stats = QueueStats()
    for status, count in rows:
        if status == "pending":
            stats.pending = count
        elif status == "in_progress":
            stats.in_progress = count
        elif status == "completed":
            stats.completed = count
        elif status == "failed":
            stats.failed = count
        elif status == "held":
            stats.held = count

    stats.total = (
        stats.pending
        + stats.in_progress
        + stats.completed
        + stats.failed
        + stats.held
    )

    # Get counts by continuation
    result = await session.execute(  # type: ignore[assignment]
        select(
            col(Request.continuation),
            col(Request.status),
            sa.func.count(),
        ).group_by(col(Request.continuation), col(Request.status))
    )
    rows = result.all()

    for continuation, status, count in rows:
        if continuation not in stats.by_continuation:
            stats.by_continuation[continuation] = {}
        stats.by_continuation[continuation][status] = count

    return stats


async def _throughput_stats(session: AsyncSession) -> ThroughputStats:
    """Compute throughput stats on an existing session."""
    result = await session.execute(
        select(
            sa.func.count(),
            sa.func.min(col(Request.started_at)),
            sa.func.max(col(Request.completed_at)),
            sa.func.avg(
                (
                    sa.func.julianday(col(Request.completed_at))
                    - sa.func.julianday(col(Request.started_at))
                )
                * 86400
            ),
        ).where(
            col(Request.status) == "completed",
            col(Request.started_at).isnot(None),
            col(Request.completed_at).isnot(None),
        )
    )
    row = result.first()

    stats = ThroughputStats()
    if row and row[0] > 0:
        stats.total_completed = row[0]

        # Calculate duration from first to last request
        if row[1] and row[2]:
            duration_result = await session.execute(
                select(
                    (
                        sa.func.julianday(sa.literal(row[2]))
                        - sa.func.julianday(sa.literal(row[1]))
                    )
                    * 86400
                )
            )
            duration_row = duration_result.first()
            if duration_row and duration_row[0] is not None:
                stats.total_duration_seconds = duration_row[0]
                if stats.total_duration_seconds > 0:
                    stats.requests_per_minute = (
                        stats.total_completed / stats.total_duration_seconds
                    ) * 60

        if row[3]:
            stats.average_response_time_seconds = row[3]

    return stats


async def _result_stats(session: AsyncSession) -> ResultStats:
    """Compute result stats on an existing session."""
    result = await session.execute(
        select(
            sa.func.count(),
            sa.func.sum(
                sa.case((col(Result.is_valid) == sa.true(), 1), else_=0)
            ),
            sa.func.sum(
                sa.case((col(Result.is_valid) == sa.false(), 1), else_=0)
            ),
        )
    )
    row = result.first()

    stats = ResultStats()
    if row:
        stats.total = row[0]
        stats.valid = row[1] or 0
        stats.invalid = row[2] or 0

    # Get counts by type
    result = await session.execute(  # type: ignore[assignment]
        select(col(Result.result_type), sa.func.count()).group_by(
            col(Result.result_type)
        )
    )
    rows = result.all()
    for result_type, count in rows:
        stats.by_type[result_type] = count

    return stats


async def _error_stats(session: AsyncSession) -> ErrorStats:
    """Compute error stats on an existing session."""
    result = await session.execute(
        select(
            sa.func.count(),
            sa.func.sum(
                sa.case((col(Error.is_resolved) == sa.false(), 1), else_=0)
            ),
            sa.func.sum(
                sa.case((col(Error.is_resolved) == sa.true(), 1), else_=0)
            ),
        )
    )
    row = result.first()

    stats = ErrorStats()
    if row:
        stats.total = row[0]
        stats.unresolved = row[1] or 0
        stats.resolved = row[2] or 0

    # Get counts by type
    result = await session.execute(  # type: ignore[assignment]
        select(col(Error.error_type), sa.func.count()).group_by(
            col(Error.error_type)
        )
    )
    rows = result.all()
    for error_type, count in rows:
        stats.by_type[error_type] = count

    # Get counts by continuation (via joined requests)
    result = await session.execute(  # type: ignore[assignment]
        select(col(Request.continuation), sa.func.count(col(Error.id)))
        .join(Request, col(Error.request_id) == col(Request.id))
        .group_by(col(Request.continuation))
    )
    rows = result.all()
    for continuation, count in rows:
        stats.by_continuation[continuation] = count

    return stats


async def get_stats(
    session_factory: async_sessionmaker,
) -> DevDriverStats:
    """Get all statistics for the LocalDevDriver.

    Runs every sub-query on a single shared session/connection instead of
    opening one per sub-stat.

    Args:
        session_factory: Async session factory.

    Returns:
        DevDriverStats instance with all statistics.
    """
    async with session_factory() as session:
        # Get run metadata
        result = await session.execute(
            select(
                col(RunMetadata.scraper_name), col(RunMetadata.status)
            ).where(col(RunMetadata.id) == 1)
        )
        row = result.first()
        scraper_name = row[0] if row else ""
        run_status = row[1] if row else "unknown"

        return DevDriverStats(
            queue=await _queue_stats(session),
            throughput=await _throughput_stats(session),
            results=await _result_stats(session),
            errors=await _error_stats(session),
            run_status=run_status,
            scraper_name=scraper_name,
        )
