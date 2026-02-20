"""REST API endpoints for rate limiter monitoring.

This module provides endpoints for:
- Getting current rate limiter state
- Viewing throughput statistics
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


class RateLimitConfig(BaseModel):
    """A single rate limit configuration."""

    limit: int = Field(..., description="Max requests allowed in the window")
    interval_ms: int = Field(..., description="Time window in milliseconds")


class RateLimiterStateResponse(BaseModel):
    """Response model for rate limiter state."""

    rates: list[RateLimitConfig] = Field(
        ..., description="Configured rate limits"
    )
    total_requests: int = Field(..., description="Total requests made")
    total_successes: int = Field(..., description="Total successful requests")
    success_rate: float = Field(
        ..., description="Success rate percentage (0-100)"
    )


@router.get("", response_model=RateLimiterStateResponse)
async def get_rate_limiter_state(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RateLimiterStateResponse:
    """Get current rate limiter state for a run.

    Returns the configured rate limits and basic request statistics.

    Args:
        run_id: The unique identifier of the run.

    Returns:
        Current rate limiter state.

    Raises:
        HTTPException: 404 if run not found.
    """
    # Check if the run has an active driver with state
    run_info = await manager.get_run(run_id)
    if run_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found",
        )

    driver = run_info.driver
    if driver is None or driver.request_manager is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rate limiter not available for run '{run_id}' "
            "(driver not loaded)",
        )

    # Get state from the request manager if it has one
    state = getattr(driver.request_manager, "state", None)
    if state is None:
        return RateLimiterStateResponse(
            rates=[],
            total_requests=0,
            total_successes=0,
            success_rate=100.0,
        )

    rates = [
        RateLimitConfig(limit=r["limit"], interval_ms=r["interval_ms"])
        for r in state.get("rates", [])
    ]

    return RateLimiterStateResponse(
        rates=rates,
        total_requests=state.get("total_requests", 0),
        total_successes=state.get("total_successes", 0),
        success_rate=state.get("success_rate", 100.0),
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
