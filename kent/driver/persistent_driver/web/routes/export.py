"""REST API endpoints for data export within a run.

This module provides endpoints for:
- Exporting responses to WARC format
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from kent.driver.persistent_driver.debugger import (
    LocalDevDriverDebugger,
)
from kent.driver.persistent_driver.web.app import (
    RunManager,
    get_debugger_for_run,
    get_run_manager,
)

router = APIRouter(prefix="/api/runs/{run_id}/export", tags=["export"])


class WarcExportRequest(BaseModel):
    """Request model for WARC export."""

    compress: bool = Field(
        default=True, description="Compress the WARC file with gzip"
    )
    continuation: str | None = Field(
        None, description="Filter by continuation"
    )


class WarcExportResponse(BaseModel):
    """Response model for WARC export metadata."""

    record_count: int
    file_size: int
    filename: str
    message: str


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


@router.post("/warc", response_class=FileResponse)
async def export_warc(
    run_id: str,
    request: WarcExportRequest,
    manager: Annotated[RunManager, Depends(get_run_manager)],
) -> FileResponse:
    """Export responses to a WARC file.

    Creates a WARC file containing all request/response pairs for the run,
    suitable for archival or replay with tools like Wayback Machine.

    Args:
        run_id: The run identifier.
        request: Export options.

    Returns:
        WARC file as downloadable attachment.

    Raises:
        HTTPException: 400 if no responses to export.
    """
    debugger = await _get_debugger(run_id, manager, read_only=True)

    # Create temp file for WARC
    suffix = ".warc.gz" if request.compress else ".warc"
    fd, temp_path = tempfile.mkstemp(suffix=suffix, prefix=f"{run_id}_")
    # Close the file descriptor since we'll pass the path to export function
    import os

    os.close(fd)
    warc_path = Path(temp_path)

    try:
        _count = await debugger.export_warc(
            warc_path,
            compress=request.compress,
            continuation=request.continuation,
        )
    except ValueError as e:
        # LDDD raises ValueError when no responses to export
        warc_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except Exception as e:
        warc_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export WARC: {e}",
        ) from e

    filename = f"{run_id}{suffix}"
    media_type = (
        "application/warc+gzip" if request.compress else "application/warc"
    )

    return FileResponse(
        path=warc_path,
        filename=filename,
        media_type=media_type,
        background=None,  # Don't delete file in background, let client download complete
    )


@router.get("/warc/preview", response_model=WarcExportResponse)
async def preview_warc_export(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_run_manager)],
    continuation: str | None = Query(
        None, description="Filter by continuation"
    ),
) -> WarcExportResponse:
    """Preview WARC export without creating the file.

    Returns metadata about what would be exported.

    Args:
        run_id: The run identifier.
        continuation: Optional continuation filter.

    Returns:
        Export preview with record count.
    """
    debugger = await _get_debugger(run_id, manager, read_only=True)

    preview = await debugger.preview_warc_export(continuation=continuation)

    count = preview["record_count"]
    estimated_size = preview["estimated_size"]

    return WarcExportResponse(
        record_count=count,
        file_size=estimated_size,
        filename=f"{run_id}.warc.gz",
        message=f"Would export {count} records (~{estimated_size} bytes uncompressed)",
    )
