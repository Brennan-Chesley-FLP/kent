"""REST API endpoints for debugging tools.

This module provides endpoints for:
- Diagnosing responses (re-running continuations with XPath observation)
- Future: XSD validation of responses
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from kent.driver.persistent_driver.debugger import (
    LocalDevDriverDebugger,
)
from kent.driver.persistent_driver.web.app import (
    RunManager,
    get_debugger_for_run,
    get_run_manager,
)

router = APIRouter(prefix="/api/runs/{run_id}/debug", tags=["debug"])


class DiagnoseResponse(BaseModel):
    """Response model for diagnose endpoint."""

    response_id: int
    continuation: str
    yields: list[dict[str, Any]]
    simple_tree: str
    observer_json: list[dict[str, Any]]
    error: str | None = None


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


async def _get_driver_for_run(run_id: str, manager: RunManager):
    """Get driver instance for a loaded run.

    NOTE: This is used for operations that require the full driver machinery,
    such as re-running continuations with XPath observation. For read-only
    database inspection, use _get_debugger instead.

    Args:
        run_id: The run identifier.
        manager: The run manager.

    Returns:
        LocalDevDriver instance.

    Raises:
        HTTPException: 404 if run not found, 400 if not loaded.
    """
    run_info = await manager.get_run(run_id)
    if run_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found",
        )
    if run_info.driver is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Run '{run_id}' is not loaded. Load it first using POST /api/runs/{run_id}/load or POST /api/runs/{run_id}/resume.",
        )
    return run_info.driver


@router.get("/diagnose/{response_id}", response_model=DiagnoseResponse)
async def diagnose_response(
    run_id: str,
    response_id: int,
    manager: Annotated[RunManager, Depends(get_run_manager)],
    speculation_cap: int = Query(
        default=3,
        ge=0,
        le=10,
        description="Maximum speculative requests to follow",
    ),
) -> DiagnoseResponse:
    """Re-run a continuation against a stored response with XPath observation.

    This endpoint retrieves a stored response and re-runs its continuation
    method with an XPathObserver active. Returns information about what
    XPath/CSS queries were made, their match counts, and what was yielded.

    Useful for debugging "zero results" issues where the HTML structure
    may have changed or XPath queries are incorrect.

    IMPORTANT: This endpoint requires the run to be loaded (driver must be active)
    because it needs to re-execute the scraper's continuation methods. Use
    POST /api/runs/{run_id}/load or POST /api/runs/{run_id}/resume first.

    NOTE: This endpoint uses the LocalDevDriver's diagnose() method which takes
    a response_id. This differs from LocalDevDriverDebugger's diagnose() method
    which takes an error_id. The driver method provides full re-execution with
    XPath observation, while LDDD's method provides basic error context only.

    Args:
        run_id: The run identifier.
        response_id: The database ID of the response to diagnose.
        speculation_cap: Maximum speculative requests to follow (default 3).
            Note: This parameter is deprecated in the driver but kept for
            backwards compatibility.

    Returns:
        Diagnosis results including yields and XPath observation data.

    Raises:
        HTTPException: 404 if run or response not found.
        HTTPException: 400 if run not loaded (driver not active).
        HTTPException: 500 if diagnosis fails.
    """
    driver = await _get_driver_for_run(run_id, manager)

    try:
        result = await driver.diagnose(response_id, speculation_cap)
        return DiagnoseResponse(**result.to_dict())
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Diagnosis failed: {e}",
        ) from e
