"""Write/mutation operations for LocalDevDriverDebugger."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Literal

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlmodel import select

from kent.driver.persistent_driver.models import (
    Error,
    Request,
)
from kent.driver.persistent_driver.sql_manager import (
    SQLManager,
)

if TYPE_CHECKING:
    pass


class ManipulationMixin:
    """Write operations: cancel, requeue, resolve, seed, train, recompress."""

    sql: SQLManager
    _session_factory: async_sessionmaker
    read_only: bool

    if TYPE_CHECKING:
        # Provided by DebuggerBase at runtime via multiple inheritance.
        def _require_write_mode(self) -> None: ...

    # =========================================================================
    # Request Manipulation
    # =========================================================================

    async def cancel_request(self, request_id: int) -> bool:
        """Cancel a pending or held request.

        Args:
            request_id: The request ID to cancel.

        Returns:
            True if the request was cancelled, False if it was not pending/held.

        Raises:
            PermissionError: If the debugger is in read-only mode.
        """
        self._require_write_mode()
        return await self.sql.cancel_request(request_id)

    async def cancel_requests_by_continuation(self, continuation: str) -> int:
        """Cancel all pending/held requests for a continuation.

        Args:
            continuation: The continuation (step name) to cancel.

        Returns:
            Number of requests cancelled.

        Raises:
            PermissionError: If the debugger is in read-only mode.
        """
        self._require_write_mode()
        return await self.sql.cancel_requests_by_continuation(continuation)

    async def requeue_request(
        self, request_id: int, clear_downstream: bool = True
    ) -> int:
        """Requeue a completed or failed request.

        Args:
            request_id: The request ID to requeue.
            clear_downstream: If True (default), delete all downstream data.

        Returns:
            The new request ID.

        Raises:
            PermissionError: If the debugger is in read-only mode.
            ValueError: If the request doesn't exist.
        """
        self._require_write_mode()

        if clear_downstream:
            result = await self.sql.requeue_requests(
                [request_id], clear_downstream=True
            )
            if not result.requeued_request_ids:
                raise ValueError(f"Request {request_id} not found")
            return result.requeued_request_ids[0]
        else:
            # Just create a new request without clearing
            async with self._session_factory() as session:
                result = await session.execute(
                    select(Request).where(Request.id == request_id)
                )
                req = result.scalars().first()
                if not req:
                    raise ValueError(f"Request {request_id} not found")

            new_id = await self.sql.insert_requeue_request(
                priority=req.priority,
                method=req.method,
                url=req.url,
                headers_json=req.headers_json,
                cookies_json=req.cookies_json,
                body=req.body,
                continuation=req.continuation,
                current_location=req.current_location,
                accumulated_data_json=req.accumulated_data_json,
                aux_data_json=req.aux_data_json,
                permanent_json=req.permanent_json,
                original_request_id=request_id,
                request_type=req.request_type or "navigating",
                expected_type=req.expected_type,
            )
            return new_id

    async def requeue_continuation(
        self,
        continuation: str,
        status: Literal["completed", "failed"] = "completed",
        clear_downstream: bool = True,
    ) -> int:
        """Requeue all requests for a continuation with a given status.

        Args:
            continuation: The continuation (step name) to requeue.
            status: Which requests to requeue ('completed' or 'failed').
            clear_downstream: If True (default), clear downstream data.

        Returns:
            Number of requests requeued.

        Raises:
            PermissionError: If the debugger is in read-only mode.
        """
        self._require_write_mode()
        return await self.sql.requeue_requests_by_continuation(
            continuation=continuation,
            status=status,
        )

    # =========================================================================
    # Error Manipulation
    # =========================================================================

    async def resolve_error(
        self, error_id: int, resolution_notes: str | None = None
    ) -> bool:
        """Mark an error as resolved.

        Args:
            error_id: The error ID to resolve.
            resolution_notes: Optional notes about the resolution.

        Returns:
            True if the error was resolved, False if already resolved or not found.

        Raises:
            PermissionError: If the debugger is in read-only mode.
        """
        self._require_write_mode()

        async with self._session_factory() as session:
            result = await session.execute(
                sa.update(Error)
                .where(Error.id == error_id, Error.is_resolved == sa.false())
                .values(
                    is_resolved=True,
                    resolved_at=sa.func.current_timestamp(),
                    resolution_notes=resolution_notes,
                )
            )
            await session.commit()
            return result.rowcount > 0

    async def requeue_error(
        self, error_id: int, resolution_notes: str | None = None
    ) -> int:
        """Requeue the request that caused an error.

        Args:
            error_id: The error ID to requeue.
            resolution_notes: Optional notes (defaults to "Requeued for retry").

        Returns:
            The new request ID.

        Raises:
            PermissionError: If the debugger is in read-only mode.
            ValueError: If the error doesn't exist.
        """
        self._require_write_mode()

        if resolution_notes is None:
            resolution_notes = "Requeued for retry"

        result = await self.sql.requeue_error(error_id, mark_resolved=True)

        if not result.requeued_request_ids:
            raise ValueError(
                f"Error {error_id} not found or has no associated request"
            )

        new_request_id = result.requeued_request_ids[0]

        async with self._session_factory() as session:
            await session.execute(
                sa.update(Error)
                .where(Error.id == error_id)
                .values(
                    is_resolved=True,
                    resolved_at=sa.func.current_timestamp(),
                    resolution_notes=f"{resolution_notes} (requeued as request {new_request_id})",
                )
            )
            await session.commit()

        return new_request_id

    async def batch_requeue_errors(
        self,
        error_type: str | None = None,
        continuation: str | None = None,
    ) -> int:
        """Requeue multiple errors matching filter criteria.

        Args:
            error_type: Filter by error type.
            continuation: Filter by continuation (step name).

        Returns:
            Number of errors requeued.

        Raises:
            PermissionError: If the debugger is in read-only mode.
        """
        self._require_write_mode()
        new_request_ids = await self.sql.batch_requeue_errors(
            error_type=error_type,
            continuation=continuation,
        )
        return len(new_request_ids)

    # =========================================================================
    # Compression Manipulation
    # =========================================================================

    async def train_compression_dict(
        self, continuation: str, sample_count: int = 1000
    ) -> int:
        """Train a new compression dictionary for a continuation.

        Args:
            continuation: The continuation (step name) to train for.
            sample_count: Number of response samples to use for training.

        Returns:
            The new compression dictionary ID.

        Raises:
            PermissionError: If the debugger is in read-only mode.
            ValueError: If not enough samples available.
        """
        self._require_write_mode()

        from kent.driver.persistent_driver.compression import (
            train_compression_dict,
        )

        dict_id = await train_compression_dict(
            self._session_factory, continuation, sample_count
        )
        return dict_id

    async def recompress_responses(
        self, continuation: str, dict_id: int | None = None
    ) -> dict[str, int]:
        """Recompress responses with a compression dictionary.

        Args:
            continuation: The continuation (step name) to recompress.
            dict_id: Compression dictionary ID. If None, uses latest.

        Returns:
            Dictionary with recompression statistics (total, size_before, size_after, savings).

        Raises:
            PermissionError: If the debugger is in read-only mode.
            ValueError: If no dictionary found.
        """
        self._require_write_mode()

        from kent.driver.persistent_driver.compression import (
            recompress_responses,
        )

        total, size_before, size_after = await recompress_responses(
            self._session_factory, continuation, dict_id=dict_id
        )
        return {
            "total": total,
            "size_before": size_before,
            "size_after": size_after,
            "savings": size_before - size_after,
        }

    # =========================================================================
    # Seed Speculative Requests
    # =========================================================================

    async def seed_speculative_requests(
        self,
        step_name: str,
        from_id: int,
        to_id: int,
    ) -> int:
        """Seed pending requests for a speculative step ID range.

        Creates new pending requests by invoking the @speculate function
        for each ID in the specified range.

        Args:
            step_name: Name of the @speculate decorated function.
            from_id: Starting ID (inclusive).
            to_id: Ending ID (inclusive).

        Returns:
            Number of requests seeded.

        Raises:
            PermissionError: If debugger is in read-only mode.
            ValueError: If scraper not found in registry or step not found.
        """
        from kent.common.decorators import (
            get_entry_metadata,
        )

        self._require_write_mode()

        # Get scraper info from metadata
        metadata = await self.sql.get_run_metadata()
        if metadata is None:
            raise ValueError("No run metadata found in database")

        scraper_name = metadata.get("scraper_name")
        if not scraper_name:
            raise ValueError("No scraper_name in run metadata")

        # Import registry and find scraper
        from kent.driver.persistent_driver.web.scraper_registry import (
            get_registry,
        )

        registry = get_registry()

        # Find scraper by module path (primary), full path, or class name
        matching = [
            s
            for s in registry.list_scrapers()
            if s.module_path == scraper_name
        ]
        if not matching:
            matching = [
                s
                for s in registry.list_scrapers()
                if s.full_path == scraper_name
            ]
        if not matching:
            matching = [
                s
                for s in registry.list_scrapers()
                if s.class_name == scraper_name
            ]

        if not matching:
            raise ValueError(
                f"Scraper '{scraper_name}' not found in registry. "
                "Make sure the web server is running with the correct sd_dir."
            )

        scraper_info = matching[0]

        # Instantiate the scraper
        scraper = registry.instantiate_scraper(scraper_info.full_path)
        if scraper is None:
            raise ValueError(
                f"Failed to instantiate scraper '{scraper_info.full_path}'"
            )

        # Get the speculative entry function
        func = getattr(scraper, step_name, None)
        if func is None:
            raise ValueError(f"Step '{step_name}' not found on scraper")

        # Verify it's a speculative entry function
        entry_meta = get_entry_metadata(func)
        if entry_meta is None or not entry_meta.speculative:
            raise ValueError(
                f"Step '{step_name}' is not a speculative entry function"
            )

        # Seed requests for the range
        seeded_count = 0
        for id_value in range(from_id, to_id + 1):
            request = func(id_value)

            http_request = request.request

            continuation = request.continuation
            if callable(continuation) and not isinstance(continuation, str):
                continuation = continuation.__name__

            if request.archive:
                request_type = "archive"
                expected_type = request.expected_type
            elif request.nonnavigating:
                request_type = "non_navigating"
                expected_type = None
            else:
                request_type = "navigating"
                expected_type = None

            permanent_data = (
                dict(request.permanent) if request.permanent else {}
            )

            speculation_id_json = None
            if request.speculation_id is not None:
                speculation_id_json = json.dumps(list(request.speculation_id))

            await self.sql.insert_request(
                priority=request.priority,
                request_type=request_type,
                method=http_request.method.value,
                url=http_request.url,
                headers_json=json.dumps(http_request.headers)
                if http_request.headers
                else None,
                cookies_json=json.dumps(http_request.cookies)
                if http_request.cookies
                else None,
                body=http_request.data
                if isinstance(http_request.data, bytes)
                else (
                    json.dumps(http_request.data).encode()
                    if http_request.data
                    else None
                ),
                continuation=continuation,
                current_location=request.current_location,
                accumulated_data_json=json.dumps(request.accumulated_data)
                if request.accumulated_data
                else None,
                aux_data_json=json.dumps(request.aux_data)
                if request.aux_data
                else None,
                permanent_json=json.dumps(permanent_data)
                if permanent_data
                else None,
                expected_type=expected_type,
                dedup_key=None,
                parent_id=None,
                is_speculative=request.is_speculative,
                speculation_id=speculation_id_json,
            )
            seeded_count += 1

        return seeded_count
