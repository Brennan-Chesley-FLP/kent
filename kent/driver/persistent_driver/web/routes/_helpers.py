"""Shared helpers for route modules."""

from __future__ import annotations

from fastapi import HTTPException, status

from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger
from kent.driver.persistent_driver.web.app import (
    RunManager,
    get_debugger_for_run,
)


def convert_run_error(e: ValueError) -> HTTPException:
    """Convert a run-manager ValueError into an appropriate HTTPException.

    Messages containing "not found" become 404; everything else becomes 400.
    """
    if "not found" in str(e):
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=str(e),
    )


async def get_debugger(
    run_id: str, manager: RunManager, read_only: bool = True
) -> LocalDevDriverDebugger:
    """Get LocalDevDriverDebugger for a run, converting errors to HTTPException.

    Raises:
        HTTPException: 404 if the run is not found, 400 for other ValueErrors.
    """
    try:
        return await get_debugger_for_run(run_id, manager, read_only=read_only)
    except ValueError as e:
        raise convert_run_error(e) from e
