"""REST API endpoints for managing scraper runs.

This module provides endpoints for:
- Listing all runs
- Getting run details
- Creating new runs
- Starting/stopping runs
- Deleting runs
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlmodel import select

from kent.driver.persistent_driver.web.app import (
    RunInfo,
    RunManager,
    get_run_manager,
)

router = APIRouter(prefix="/api/runs", tags=["runs"])


class RunResponse(BaseModel):
    """Response model for a single run."""

    run_id: str
    db_path: str
    status: str
    created_at: str
    started_at: str | None


class RunListResponse(BaseModel):
    """Response model for listing runs."""

    runs: list[RunResponse]
    total: int


class SpeculationStepConfig(BaseModel):
    """Speculation configuration for a single step."""

    threshold: int = Field(
        default=0,
        description="IDs <= threshold always continue (no speculation stop)",
    )
    speculation: int = Field(
        default=5,
        description="Number of consecutive failures above threshold before stopping",
    )


class CreateRunRequest(BaseModel):
    """Request model for creating a new run."""

    run_id: str = Field(..., description="Unique identifier for the run")
    scraper_path: str = Field(
        ..., description="Full scraper path (module.path:ClassName)"
    )
    seed_params: list[dict[str, dict[str, Any]]] | None = Field(
        default=None,
        description="Parameter invocations for initial_seed() [{entry_name: kwargs}, ...]",
    )
    speculation_config: dict[str, SpeculationStepConfig] | None = Field(
        default=None,
        description="Speculation settings per step (step_name -> {threshold, speculation})",
    )
    num_workers: int = Field(
        default=1, description="Number of concurrent workers"
    )
    max_backoff_time: float = Field(
        default=3600.0,
        description="Maximum total backoff time before marking failed",
    )


class StopRunRequest(BaseModel):
    """Request model for stopping a run."""

    timeout: float = Field(
        default=30.0, description="Timeout for graceful stop in seconds"
    )


class LoadRunRequest(BaseModel):
    """Request model for loading an existing run.

    DEPRECATED: Use POST /api/runs/{run_id}/resume instead.
    """

    scraper_path: str | None = Field(
        default=None,
        description="Full scraper path (module.path:ClassName). If not provided, uses scraper_name from database.",
    )
    num_workers: int = Field(
        default=1, description="Number of concurrent workers"
    )
    max_backoff_time: float = Field(
        default=3600.0,
        description="Maximum total backoff time before marking failed",
    )


class ResumeRunRequest(BaseModel):
    """Request model for resuming an existing run (load + start combined).

    This is the preferred way to start an existing run. It combines
    loading and starting into a single atomic operation.
    """

    scraper_path: str | None = Field(
        default=None,
        description="Full scraper path (module.path:ClassName). If not provided, uses scraper_name from database.",
    )
    num_workers: int = Field(
        default=1, description="Number of concurrent workers"
    )
    max_backoff_time: float = Field(
        default=3600.0,
        description="Maximum total backoff time before marking failed",
    )


def _run_info_to_response(run_info: RunInfo) -> RunResponse:
    """Convert RunInfo to API response model."""
    d = run_info.to_dict()
    return RunResponse(
        run_id=d["run_id"],
        db_path=d["db_path"],
        status=d["status"],
        created_at=d["created_at"],
        started_at=d["started_at"],
    )


@router.get("", response_model=RunListResponse)
async def list_runs(
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RunListResponse:
    """List all known runs.

    Returns runs discovered from the runs directory, including
    their current status (unloaded, loaded, running, etc.).
    """
    runs = await manager.list_runs()
    return RunListResponse(
        runs=[_run_info_to_response(r) for r in runs],
        total=len(runs),
    )


@router.get("/{run_id}", response_model=RunResponse)
async def get_run(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RunResponse:
    """Get details for a specific run.

    Args:
        run_id: The unique identifier of the run.

    Returns:
        Run details including status and timestamps.

    Raises:
        HTTPException: 404 if run not found.
    """
    run_info = await manager.get_run(run_id)
    if run_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found",
        )
    return _run_info_to_response(run_info)


@router.post(
    "", response_model=RunResponse, status_code=status.HTTP_201_CREATED
)
async def create_run(
    request: CreateRunRequest,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RunResponse:
    """Create a new scraper run and start it immediately.

    Creates a new database, initializes the driver with the specified
    configuration, and starts running automatically.

    Args:
        request: Run configuration including scraper path and parameters.

    Returns:
        The created run details with status 'running'.

    Raises:
        HTTPException: 400 if run_id already exists.
        HTTPException: 404 if scraper not found.
    """
    from kent.driver.persistent_driver.web.scraper_registry import (
        get_registry,
    )

    # Get registry and look up scraper
    try:
        registry = get_registry()
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Scraper registry not initialized",
        ) from e

    # Check if scraper exists
    scraper_info = registry.get_scraper(request.scraper_path)
    if scraper_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scraper '{request.scraper_path}' not found",
        )

    # Instantiate scraper (params passed via initial_seed() at runtime)
    scraper = registry.instantiate_scraper(request.scraper_path)
    if scraper is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to instantiate scraper '{request.scraper_path}'",
        )

    # Convert speculation_config to dict format for driver
    speculation_config_dict = None
    if request.speculation_config:
        speculation_config_dict = {
            step_name: {
                "threshold": step_config.threshold,
                "speculation": step_config.speculation,
            }
            for step_name, step_config in request.speculation_config.items()
        }

    # Create run
    try:
        run_info = await manager.create_run(
            run_id=request.run_id,
            scraper=scraper,
            num_workers=request.num_workers,
            max_backoff_time=request.max_backoff_time,
            speculation_config=speculation_config_dict,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e

    # Auto-start the run
    try:
        run_info = await manager.start_run(request.run_id)
        return _run_info_to_response(run_info)
    except ValueError:
        # If start fails, still return the created run info
        return _run_info_to_response(run_info)


@router.post("/{run_id}/load", response_model=RunResponse, deprecated=True)
async def load_run(
    run_id: str,
    request: LoadRunRequest,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RunResponse:
    """Load an existing run from its database.

    DEPRECATED: Use POST /api/runs/{run_id}/resume instead, which combines
    loading and starting into a single atomic operation.

    Opens the database and prepares the driver for running. The scraper
    can be specified explicitly or inferred from the database metadata.

    Args:
        run_id: The unique identifier of the run.
        request: Load configuration including optional scraper path.

    Returns:
        Updated run details with status 'loaded'.

    Raises:
        HTTPException: 404 if run not found or scraper not found.
        HTTPException: 400 if run already loaded.
    """
    import warnings

    warnings.warn(
        "POST /api/runs/{run_id}/load is deprecated. Use POST /api/runs/{run_id}/resume instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    from kent.driver.persistent_driver.database import (
        create_engine_and_init,
        get_session_factory,
    )
    from kent.driver.persistent_driver.models import RunMetadata
    from kent.driver.persistent_driver.web.scraper_registry import (
        get_registry,
    )

    # Get run info
    run_info = await manager.get_run(run_id)
    if run_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found",
        )

    if run_info.driver is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Run '{run_id}' is already loaded",
        )

    # Determine scraper path
    scraper_path = request.scraper_path

    if scraper_path is None:
        # Query the database for the scraper_name
        try:
            engine = await create_engine_and_init(run_info.db_path)
            session_factory = get_session_factory(engine)
            try:
                async with session_factory() as session:
                    result = await session.execute(
                        select(
                            RunMetadata.status, RunMetadata.scraper_name
                        ).where(RunMetadata.id == 1)
                    )
                    row = result.first()
                    if row is None:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Run '{run_id}' has no metadata. Cannot determine scraper.",
                        )
                    scraper_name = row[1]
            finally:
                await engine.dispose()
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to read database: {e}",
            ) from e

        # Find a scraper with this name in the registry
        try:
            registry = get_registry()
        except RuntimeError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Scraper registry not initialized",
            ) from e

        # Search for scraper by full_path first (new format: module:class)
        # Fall back to module_path match (old format: just module)
        matching_scrapers = [
            s for s in registry.list_scrapers() if s.full_path == scraper_name
        ]
        if not matching_scrapers:
            # Try matching by module_path (backward compatibility)
            matching_scrapers = [
                s
                for s in registry.list_scrapers()
                if s.module_path == scraper_name
            ]
        if not matching_scrapers:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Scraper '{scraper_name}' not found in registry. "
                f"Specify scraper_path explicitly.",
            )
        if len(matching_scrapers) > 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Multiple scrapers named '{scraper_name}' found. "
                f"Specify scraper_path explicitly: "
                f"{[s.full_path for s in matching_scrapers]}",
            )
        scraper_path = matching_scrapers[0].full_path

    # Get registry and instantiate scraper
    try:
        registry = get_registry()
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Scraper registry not initialized",
        ) from e

    scraper_info = registry.get_scraper(scraper_path)
    if scraper_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scraper '{scraper_path}' not found",
        )

    # Instantiate scraper (without params for resume - they're in the DB)
    scraper = registry.instantiate_scraper(scraper_path)
    if scraper is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to instantiate scraper '{scraper_path}'",
        )

    # Load run
    try:
        run_info = await manager.load_run(
            run_id=run_id,
            scraper=scraper,
            num_workers=request.num_workers,
            max_backoff_time=request.max_backoff_time,
        )
        return _run_info_to_response(run_info)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e


@router.post("/{run_id}/start", response_model=RunResponse)
async def start_run(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RunResponse:
    """Start or resume a run.

    The run must be in 'loaded' state. If the run completed previously,
    it will resume from where it left off.

    Args:
        run_id: The unique identifier of the run.

    Returns:
        Updated run details with status 'running'.

    Raises:
        HTTPException: 404 if run not found.
        HTTPException: 400 if run not in valid state to start.
    """
    try:
        run_info = await manager.start_run(run_id)
        return _run_info_to_response(run_info)
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


@router.post("/{run_id}/stop", response_model=RunResponse)
async def stop_run(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
    request: StopRunRequest | None = None,
) -> RunResponse:
    """Stop a running run gracefully.

    Signals the driver to stop and waits for in-progress requests
    to complete. If the timeout is exceeded, forces cancellation.

    Args:
        run_id: The unique identifier of the run.
        request: Optional configuration for stop behavior.

    Returns:
        Updated run details with status 'stopped'.

    Raises:
        HTTPException: 404 if run not found.
        HTTPException: 400 if run not currently running.
    """
    timeout = request.timeout if request else 30.0

    try:
        run_info = await manager.stop_run(run_id, timeout=timeout)
        return _run_info_to_response(run_info)
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


@router.post("/{run_id}/resume", response_model=RunResponse)
async def resume_run(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
    request: ResumeRunRequest | None = None,
) -> RunResponse:
    """Resume a run (load + start in one step).

    This is the preferred way to start an existing run. It combines
    loading and starting into a single atomic operation. If the run
    is already running, returns its current status.

    Args:
        run_id: The unique identifier of the run.
        request: Optional configuration for the run.

    Returns:
        Run details with status 'running'.

    Raises:
        HTTPException: 404 if run not found.
        HTTPException: 400 if run cannot be started.
        HTTPException: 500 if scraper cannot be instantiated.
    """
    from kent.driver.persistent_driver.sql_manager import (
        SQLManager,
    )
    from kent.driver.persistent_driver.web.scraper_registry import (
        get_registry,
    )

    request = request or ResumeRunRequest()

    # Get run info
    run_info = await manager.get_run(run_id)
    if run_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found",
        )

    # If already running, return current state
    if run_info.task is not None and not run_info.task.done():
        return _run_info_to_response(run_info)

    # If already loaded but not running, just start it
    if run_info.driver is not None:
        try:
            run_info = await manager.start_run(run_id)
            return _run_info_to_response(run_info)
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e),
            ) from e

    # Need to load first - determine scraper path
    scraper_path = request.scraper_path
    if scraper_path is None:
        # Get scraper name from database
        try:
            async with SQLManager.open(run_info.db_path) as sql_manager:
                metadata = await sql_manager.get_run_metadata()
                if metadata is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Run has no metadata. Specify scraper_path explicitly.",
                    )
                scraper_name = metadata.get("scraper_name")
                if not scraper_name:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Run has no scraper_name in metadata. Specify scraper_path explicitly.",
                    )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to read database: {e}",
            ) from e

        # Find scraper by name in registry
        try:
            registry = get_registry()
        except RuntimeError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Scraper registry not initialized",
            ) from e

        # Search for scraper by full_path first (new format: module:class)
        # Fall back to module_path match (old format: just module)
        matching_scrapers = [
            s for s in registry.list_scrapers() if s.full_path == scraper_name
        ]
        if not matching_scrapers:
            # Try matching by module_path (backward compatibility)
            matching_scrapers = [
                s
                for s in registry.list_scrapers()
                if s.module_path == scraper_name
            ]
        if not matching_scrapers:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Scraper '{scraper_name}' not found in registry. Specify scraper_path explicitly.",
            )
        if len(matching_scrapers) > 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Multiple scrapers named '{scraper_name}' found. Specify scraper_path explicitly: {[s.full_path for s in matching_scrapers]}",
            )
        scraper_path = matching_scrapers[0].full_path

    # Get registry and instantiate scraper
    try:
        registry = get_registry()
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Scraper registry not initialized",
        ) from e

    scraper = registry.instantiate_scraper(scraper_path)
    if scraper is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to instantiate scraper '{scraper_path}'",
        )

    # Resume (load + start)
    try:
        run_info = await manager.resume_run(
            run_id=run_id,
            scraper=scraper,
            num_workers=request.num_workers,
            max_backoff_time=request.max_backoff_time,
        )
        return _run_info_to_response(run_info)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e


@router.delete("/{run_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_run(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> None:
    """Delete a run and its database.

    The run must be stopped before deletion. This permanently
    removes all data associated with the run.

    Args:
        run_id: The unique identifier of the run.

    Raises:
        HTTPException: 404 if run not found.
        HTTPException: 400 if run is still running.
    """
    try:
        await manager.delete_run(run_id)
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


@router.post("/{run_id}/unload", response_model=RunResponse)
async def unload_run(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RunResponse:
    """Unload a run, closing its database connection.

    Frees memory by closing the driver connection while keeping
    the run available for future loading.

    Args:
        run_id: The unique identifier of the run.

    Returns:
        Updated run details with status 'unloaded'.

    Raises:
        HTTPException: 404 if run not found.
        HTTPException: 400 if run is still running.
    """
    try:
        await manager.unload_run(run_id)
        run_info = await manager.get_run(run_id)
        if run_info is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Run '{run_id}' not found after unload",
            )
        return _run_info_to_response(run_info)
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


@router.post("/scan", response_model=RunListResponse)
async def scan_runs(
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> RunListResponse:
    """Rescan the runs directory for new databases.

    Discovers any new .db files that have been added to the
    runs directory since the last scan.

    Returns:
        Updated list of all runs after scanning.
    """
    await manager.scan_runs()
    runs = await manager.list_runs()
    return RunListResponse(
        runs=[_run_info_to_response(r) for r in runs],
        total=len(runs),
    )
