"""REST API endpoints for managing requests within a run.

This module provides endpoints for:
- Listing requests with filters
- Getting request details
- Cancelling individual requests
- Batch cancelling requests by continuation
- Batch requeuing requests by continuation
"""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from kent.driver.persistent_driver.web.app import (
    RunManager,
    get_run_manager,
)
from kent.driver.persistent_driver.web.routes._helpers import get_debugger

router = APIRouter(prefix="/api/runs/{run_id}/requests", tags=["requests"])


class RequestResponse(BaseModel):
    """Response model for a single request."""

    id: int
    status: str
    priority: int
    queue_counter: int
    method: str
    url: str
    continuation: str
    current_location: str
    created_at: str | None
    started_at: str | None
    completed_at: str | None
    retry_count: int
    cumulative_backoff: float
    last_error: str | None


class RequestListResponse(BaseModel):
    """Response model for listing requests."""

    items: list[RequestResponse]
    total: int
    offset: int
    limit: int
    has_more: bool


class CancelResponse(BaseModel):
    """Response model for cancel operations."""

    cancelled_count: int
    message: str


class CancelByContinuationRequest(BaseModel):
    """Request model for batch cancellation."""

    continuation: str = Field(..., description="Continuation to filter by")


class RequestSummaryItem(BaseModel):
    """Summary counts for a single continuation."""

    continuation: str
    pending: int = 0
    in_progress: int = 0
    completed: int = 0
    failed: int = 0
    held: int = 0
    cancelled: int = 0
    total: int = 0


class RequestSummaryResponse(BaseModel):
    """Response model for request summary endpoint."""

    items: list[RequestSummaryItem]
    grand_total: int


@router.get("", response_model=RequestListResponse)
async def list_requests(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
    status_filter: Literal[
        "pending", "in_progress", "completed", "failed", "held"
    ]
    | None = Query(None, alias="status", description="Filter by status"),
    continuation: str | None = Query(
        None, description="Filter by continuation"
    ),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(50, ge=1, le=500, description="Pagination limit"),
    sort: Literal["queue", "id_asc", "id_desc"] = Query(
        "queue", description="Sort order: queue (priority), id_asc, id_desc"
    ),
) -> RequestListResponse:
    """List requests for a run with optional filters.

    Args:
        run_id: The run identifier.
        status_filter: Optional status filter (pending, in_progress, completed, failed, held).
        continuation: Optional continuation name filter.
        offset: Pagination offset.
        limit: Maximum number of results.

    Returns:
        Paginated list of requests.
    """
    debugger = await get_debugger(run_id, manager, read_only=True)

    # Use LDDD's list_requests method
    page = await debugger.list_requests(
        status=status_filter,
        continuation=continuation,
        offset=offset,
        limit=limit,
        sort=sort,
    )

    items = [
        RequestResponse(
            id=r.id,
            status=r.status,
            priority=r.priority,
            queue_counter=r.queue_counter,
            method=r.method,
            url=r.url,
            continuation=r.continuation,
            current_location=r.current_location,
            created_at=r.created_at,
            started_at=r.started_at,
            completed_at=r.completed_at,
            retry_count=r.retry_count,
            cumulative_backoff=r.cumulative_backoff or 0.0,
            last_error=r.last_error,
        )
        for r in page.items
    ]

    return RequestListResponse(
        items=items,
        total=page.total,
        offset=page.offset,
        limit=page.limit,
        has_more=page.has_more,
    )


@router.get("/summary", response_model=RequestSummaryResponse)
async def get_request_summary(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RequestSummaryResponse:
    """Get request counts grouped by continuation and status.

    Returns a pivot table with one row per continuation, showing counts
    for each status (pending, in_progress, completed, failed, held, cancelled).

    Bookkeeping requests (those without URLs) are excluded from the summary.

    Args:
        run_id: The run identifier.

    Returns:
        Summary of request counts by continuation and status.
    """
    debugger = await get_debugger(run_id, manager, read_only=True)

    # Use LDDD's get_request_summary method
    summary = await debugger.get_request_summary()

    # Convert to response model format
    summaries: dict[str, RequestSummaryItem] = {}
    grand_total = 0

    for continuation, status_counts in summary.items():
        if continuation == "all":
            # Skip the "all" key as we calculate grand_total separately
            continue

        summaries[continuation] = RequestSummaryItem(
            continuation=continuation,
            pending=status_counts.get("pending", 0),
            in_progress=status_counts.get("in_progress", 0),
            completed=status_counts.get("completed", 0),
            failed=status_counts.get("failed", 0),
            held=status_counts.get("held", 0),
            cancelled=status_counts.get("cancelled", 0),
            total=sum(status_counts.values()),
        )
        grand_total += summaries[continuation].total

    return RequestSummaryResponse(
        items=list(summaries.values()),
        grand_total=grand_total,
    )


class SpeculativeStepInfo(BaseModel):
    """Info about a speculative step."""

    name: str
    default_starting_id: int = 1
    largest_observed_gap: int = 10
    last_successful_id: int | None = None


class SpeculativeStepsResponse(BaseModel):
    """Response model for listing speculative steps."""

    items: list[SpeculativeStepInfo]
    run_loaded: bool


@router.get("/speculative-steps", response_model=SpeculativeStepsResponse)
async def get_speculative_steps(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> SpeculativeStepsResponse:
    """Get the speculative steps available for a run.

    Returns the list of @speculate decorated functions from the scraper,
    including the last successful ID from the database and the largest
    observed gap from the decorator metadata.

    Args:
        run_id: The run identifier.

    Returns:
        List of speculative step info with progress data.

    Raises:
        HTTPException: 404 if run not found.
    """
    from kent.driver.persistent_driver.web.app import (
        get_sql_manager_for_run,
    )

    # Get run info
    run_info = await manager.get_run(run_id)
    if run_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found",
        )

    # Get speculative progress from database
    progress: dict[str, int] = {}
    try:
        sql_manager = await get_sql_manager_for_run(run_id, manager)
        progress = await sql_manager.get_all_speculation_progress()
    except Exception:
        pass  # Progress will be empty dict

    # If run is loaded, get steps from the live scraper
    if run_info.driver is not None:
        scraper = run_info.driver.scraper
        entries = scraper.__class__.list_entries()
        items = []
        for entry_info in entries:
            if not entry_info.speculative:
                continue
            items.append(
                SpeculativeStepInfo(
                    name=entry_info.name,
                    default_starting_id=1,
                    last_successful_id=progress.get(entry_info.name),
                )
            )
        return SpeculativeStepsResponse(items=items, run_loaded=True)

    # If not loaded, try to get from registry using scraper_name from DB
    try:
        sql_manager = await get_sql_manager_for_run(run_id, manager)
        run_metadata = await sql_manager.get_run_metadata()
        if run_metadata is None:
            return SpeculativeStepsResponse(items=[], run_loaded=False)

        scraper_name = run_metadata.get("scraper_name")
        if not scraper_name:
            return SpeculativeStepsResponse(items=[], run_loaded=False)

        # speculative_steps were removed from ScraperInfo during @entry migration;
        # speculative info is now in entry_schema
        return SpeculativeStepsResponse(items=[], run_loaded=False)

    except Exception:
        return SpeculativeStepsResponse(items=[], run_loaded=False)


class SeedSpeculativeRequest(BaseModel):
    """Request model for seeding speculative requests."""

    step_name: str = Field(..., description="Name of the @speculate step")
    from_id: int = Field(..., ge=1, description="Starting ID (inclusive)")
    to_id: int = Field(..., ge=1, description="Ending ID (inclusive)")


class SeedSpeculativeResponse(BaseModel):
    """Response model for seeding speculative requests."""

    seeded_count: int
    step_name: str
    from_id: int
    to_id: int
    message: str


@router.post("/seed-speculative", response_model=SeedSpeculativeResponse)
async def seed_speculative_requests(
    run_id: str,
    request: SeedSpeculativeRequest,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> SeedSpeculativeResponse:
    """Seed pending requests for a speculative step ID range.

    Creates new pending requests by invoking the @speculate function
    for each ID in the specified range. This allows manually extending
    speculation or re-running specific ID ranges.

    This endpoint works regardless of whether the run is currently loaded.
    It uses the debugger to instantiate the scraper temporarily from
    the registry and seed requests directly to the database.

    Args:
        run_id: The run identifier.
        request: Contains step_name, from_id, and to_id.

    Returns:
        Count of requests seeded.

    Raises:
        HTTPException: 404 if run not found or step not found.
        HTTPException: 400 if invalid range or scraper not found.
    """
    # Validate range
    if request.from_id > request.to_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"from_id ({request.from_id}) must be <= to_id ({request.to_id})",
        )

    # Get debugger in write mode to seed requests
    debugger = await get_debugger(run_id, manager, read_only=False)

    try:
        seeded_count = await debugger.seed_speculative_requests(
            step_name=request.step_name,
            from_id=request.from_id,
            to_id=request.to_id,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    return SeedSpeculativeResponse(
        seeded_count=seeded_count,
        step_name=request.step_name,
        from_id=request.from_id,
        to_id=request.to_id,
        message=f"Seeded {seeded_count} requests for {request.step_name} IDs {request.from_id}-{request.to_id}",
    )


@router.get("/{request_id}", response_model=RequestResponse)
async def get_request(
    run_id: str,
    request_id: int,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RequestResponse:
    """Get details for a specific request.

    Args:
        run_id: The run identifier.
        request_id: The request ID.

    Returns:
        Request details.

    Raises:
        HTTPException: 404 if request not found.
    """
    debugger = await get_debugger(run_id, manager, read_only=True)

    record = await debugger.get_request(request_id)

    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Request {request_id} not found in run '{run_id}'",
        )

    return RequestResponse(
        id=record.id,
        status=record.status,
        priority=record.priority,
        queue_counter=record.queue_counter,
        method=record.method,
        url=record.url,
        continuation=record.continuation,
        current_location=record.current_location,
        created_at=record.created_at,
        started_at=record.started_at,
        completed_at=record.completed_at,
        retry_count=record.retry_count,
        cumulative_backoff=record.cumulative_backoff or 0.0,
        last_error=record.last_error,
    )


@router.post("/{request_id}/cancel", response_model=CancelResponse)
async def cancel_request(
    run_id: str,
    request_id: int,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> CancelResponse:
    """Cancel a pending or held request.

    Args:
        run_id: The run identifier.
        request_id: The request ID to cancel.

    Returns:
        Cancellation result.

    Raises:
        HTTPException: 404 if request not found.
        HTTPException: 400 if request cannot be cancelled.
    """
    debugger = await get_debugger(run_id, manager, read_only=False)

    cancelled = await debugger.cancel_request(request_id)

    if not cancelled:
        # Check if request exists
        record = await debugger.get_request(request_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Request {request_id} not found",
            )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Request {request_id} cannot be cancelled (status: {record.status})",
        )

    return CancelResponse(
        cancelled_count=1,
        message=f"Request {request_id} cancelled",
    )


@router.post("/cancel-by-continuation", response_model=CancelResponse)
async def cancel_by_continuation(
    run_id: str,
    request: CancelByContinuationRequest,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> CancelResponse:
    """Cancel all pending/held requests for a specific continuation.

    Args:
        run_id: The run identifier.
        request: Contains the continuation name.

    Returns:
        Number of requests cancelled.
    """
    debugger = await get_debugger(run_id, manager, read_only=False)

    cancelled_count = await debugger.cancel_requests_by_continuation(
        request.continuation
    )

    return CancelResponse(
        cancelled_count=cancelled_count,
        message=f"Cancelled {cancelled_count} requests with continuation '{request.continuation}'",
    )
