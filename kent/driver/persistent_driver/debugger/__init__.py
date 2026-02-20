"""LocalDevDriverDebugger - Inspection and manipulation of scraper run databases.

This module provides a standalone class for inspecting and manipulating
LocalDevDriver run databases without requiring the full driver machinery.

The LocalDevDriverDebugger (LDDD) enables:
- Read-only inspection of completed or running scraper runs
- Safe manipulation operations (requeue, cancel, resolve errors)
- Lightweight CLI and WebUI tooling
- Testing and debugging workflows

Key features:
- Async context manager for connection lifecycle
- Read-only mode enforcement via SQLite connection flags
- High-level API wrapping SQLManager for semantic operations
- Compatible with existing RequestRecord, ResponseRecord, ResultRecord types

Example usage:
    # Read-only inspection
    async with LocalDevDriverDebugger.open(db_path, read_only=True) as debugger:
        metadata = await debugger.get_run_metadata()
        stats = await debugger.get_stats()
        requests = await debugger.list_requests(status='failed')

    # Write operations (requeue, cancel, etc.)
    async with LocalDevDriverDebugger.open(db_path, read_only=False) as debugger:
        await debugger.requeue_error(error_id=123)
        await debugger.cancel_request(request_id=456)
"""

from kent.driver.persistent_driver.debugger._base import DebuggerBase
from kent.driver.persistent_driver.debugger._comparison import ComparisonMixin
from kent.driver.persistent_driver.debugger._export import ExportSearchMixin
from kent.driver.persistent_driver.debugger._inspection import InspectionMixin
from kent.driver.persistent_driver.debugger._integrity import IntegrityMixin
from kent.driver.persistent_driver.debugger._manipulation import (
    ManipulationMixin,
)
from kent.driver.persistent_driver.debugger._validation import ValidationMixin


class LocalDevDriverDebugger(
    InspectionMixin,
    ManipulationMixin,
    IntegrityMixin,
    ValidationMixin,
    ExportSearchMixin,
    ComparisonMixin,
    DebuggerBase,
):
    """Debug and inspect LocalDevDriver run databases.

    This class provides a high-level API for inspecting and manipulating
    scraper run databases without requiring the full LocalDevDriver runtime.

    Supports both read-only inspection (safe for analyzing running/completed runs)
    and write operations (requeue, cancel, resolve errors).

    Attributes:
        sql: The underlying SQLManager instance for database operations.
        read_only: Whether this instance is in read-only mode.
    """

    pass


__all__ = ["LocalDevDriverDebugger"]
