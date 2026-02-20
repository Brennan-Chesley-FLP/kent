"""REST API endpoints for rate limiter monitoring.

This module provides endpoints for:
- Getting current rate limiter state
- Viewing rate limiter statistics
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from kent.driver.persistent_driver.debugger import (
    LocalDevDriverDebugger,
)
from kent.driver.persistent_driver.web.app import (
    RunManager,
    get_debugger_for_run,
    get_run_manager,
)

router = APIRouter(
    prefix="/api/runs/{run_id}/rate-limiter", tags=["rate-limiter"]
)


async def _get_debugger(
    run_id: str, manager: RunManager, read_only: bool = True
) -> LocalDevDriverDebugger:
    """Get LocalDevDriverDebugger for a run.

    Args:
        run_id: The run identifier.
        manager: The run manager.
        read_only: If True, open in read-only mode (prevents writes).

    Returns:
        LocalDevDriverDebugger instance.

    Raises:
        HTTPException: 404 if run not found, 400 if error.
    """
    try:
        return await get_debugger_for_run(run_id, manager, read_only=read_only)
    except ValueError as e:
        if "not found" in str(e):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e),
            ) from e
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e


class RateLimiterStateResponse(BaseModel):
    """Response model for rate limiter state."""

    tokens: float = Field(..., description="Current token count in bucket")
    rate: float = Field(..., description="Current rate in tokens per second")
    bucket_size: float = Field(..., description="Maximum tokens in bucket")
    last_congestion_rate: float = Field(
        ..., description="Rate at last congestion event"
    )
    jitter: float = Field(default=0.0, description="Deprecated: always 0.0")
    approximate_requests_per_minute: float = Field(
        ..., description="Approximate requests per minute (rate * 60)"
    )
    total_requests: int = Field(..., description="Total requests made")
    total_successes: int = Field(..., description="Total successful requests")
    total_rate_limited: int = Field(
        ..., description="Total rate-limited requests"
    )
    success_rate: float = Field(
        ..., description="Success rate percentage (0-100)"
    )
    status: str = Field(
        ..., description="Status: 'healthy', 'throttled', or 'recovering'"
    )
    last_used_at: float = Field(
        ..., description="Unix timestamp of last token acquisition"
    )
    created_at: str | None = Field(
        None, description="When rate limiter was created"
    )
    updated_at: str | None = Field(
        None, description="When state was last updated"
    )


@router.get("", response_model=RateLimiterStateResponse)
async def get_rate_limiter_state(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RateLimiterStateResponse:
    """Get current rate limiter state for a run.

    Returns the current state of the Adaptive Token Bucket rate limiter,
    including token count, rate, statistics, and status.

    Args:
        run_id: The unique identifier of the run.

    Returns:
        Current rate limiter state.

    Raises:
        HTTPException: 404 if run not found or rate limiter not initialized.
    """
    debugger = await _get_debugger(run_id, manager, read_only=True)

    # Get rate limiter state from database using LDDD method
    state = await debugger.get_rate_limiter_state()

    if state is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rate limiter not initialized for run '{run_id}'",
        )

    # Compute derived fields
    total_requests = state["total_requests"]
    total_successes = state["total_successes"]
    total_rate_limited = state["total_rate_limited"]
    rate = state["rate"]

    success_rate = (
        total_successes / total_requests * 100 if total_requests > 0 else 100.0
    )

    # Compute status
    if total_rate_limited == 0:
        status_str = "healthy"
    elif rate < state["last_congestion_rate"]:
        status_str = "recovering"
    else:
        status_str = "throttled"

    return RateLimiterStateResponse(
        tokens=state["tokens"],
        rate=rate,
        bucket_size=state["bucket_size"],
        last_congestion_rate=state["last_congestion_rate"],
        jitter=state["jitter"],
        approximate_requests_per_minute=rate * 60,
        total_requests=total_requests,
        total_successes=total_successes,
        total_rate_limited=total_rate_limited,
        success_rate=round(success_rate, 2),
        status=status_str,
        last_used_at=state["last_used_at"],
        created_at=state.get("created_at"),
        updated_at=state.get("updated_at"),
    )


class ThroughputStatsResponse(BaseModel):
    """Response model for throughput statistics."""

    active_workers: int = Field(
        ..., description="Number of requests currently in progress"
    )
    rate_5m: float = Field(
        ..., description="Requests per minute over last 5 minutes"
    )
    rate_15m: float = Field(
        ..., description="Requests per minute over last 15 minutes"
    )
    rate_1h: float = Field(
        ..., description="Requests per minute over last hour"
    )
    rate_1d: float = Field(
        ..., description="Requests per minute over last day"
    )


@router.get("/throughput", response_model=ThroughputStatsResponse)
async def get_throughput_stats(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> ThroughputStatsResponse:
    """Get throughput statistics for a run.

    Computes actual request rates over various time windows by counting
    completed requests in those periods.

    Args:
        run_id: The unique identifier of the run.

    Returns:
        Throughput statistics including active workers and request rates.

    Raises:
        HTTPException: 404 if run not found.
    """
    debugger = await _get_debugger(run_id, manager, read_only=True)

    import sqlalchemy as sa
    from sqlmodel import select

    from kent.driver.persistent_driver.models import Request as RequestModel

    async with debugger._session_factory() as session:
        # Get active workers (in_progress requests)
        result = await session.execute(
            select(sa.func.count())
            .select_from(RequestModel)
            .where(RequestModel.status == "in_progress")
        )
        active_workers = result.scalar_one()

        # Compute request rates over different time windows
        time_windows = [
            ("5m", 5),
            ("15m", 15),
            ("1h", 60),
            ("1d", 1440),
        ]

        rates = {}
        for name, minutes in time_windows:
            result = await session.execute(
                select(sa.func.count())
                .select_from(RequestModel)
                .where(
                    RequestModel.status == "completed",
                    RequestModel.completed_at
                    >= sa.func.datetime("now", f"-{minutes} minutes"),
                )
            )
            count = result.scalar_one()
            # Convert to requests per minute
            rates[name] = count / minutes if minutes > 0 else 0.0

    return ThroughputStatsResponse(
        active_workers=active_workers,
        rate_5m=round(rates["5m"], 2),
        rate_15m=round(rates["15m"], 2),
        rate_1h=round(rates["1h"], 2),
        rate_1d=round(rates["1d"], 2),
    )
