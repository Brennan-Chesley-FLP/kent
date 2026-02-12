"""Error requeue operations for SQLManager."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from kent.driver.dev_driver.models import Error, Request

if TYPE_CHECKING:
    import asyncio


class ErrorRequeueMixin:
    """Error table database operations."""

    _lock: asyncio.Lock
    _session_factory: async_sessionmaker

    async def get_error_with_request(
        self, error_id: int
    ) -> tuple[Any, ...] | None:
        """Get error and associated request data for requeue.

        Args:
            error_id: The database ID of the error.

        Returns:
            Row tuple with error and request data, or None.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Error.id,
                    Error.request_id,
                    Error.is_resolved,
                    Request.method,
                    Request.url,
                    Request.headers_json,
                    Request.cookies_json,
                    Request.body,
                    Request.continuation,
                    Request.current_location,
                    Request.accumulated_data_json,
                    Request.aux_data_json,
                    Request.permanent_json,
                    Request.priority,
                    Request.request_type,
                    Request.expected_type,
                )
                .outerjoin(Request, Error.request_id == Request.id)
                .where(Error.id == error_id)
            )
            row = result.first()
            return tuple(row) if row else None

    async def get_errors_for_requeue(
        self,
        error_type: str | None = None,
        continuation: str | None = None,
    ) -> list[tuple[Any, ...]]:
        """Get unresolved errors for batch requeue.

        Args:
            error_type: Optional error type filter.
            continuation: Optional continuation filter.

        Returns:
            List of row tuples with error and request data.
        """
        async with self._session_factory() as session:
            stmt = select(
                Error.id,
                Error.request_id,
                Request.method,
                Request.url,
                Request.headers_json,
                Request.cookies_json,
                Request.body,
                Request.continuation,
                Request.current_location,
                Request.accumulated_data_json,
                Request.aux_data_json,
                Request.permanent_json,
                Request.priority,
                Request.request_type,
                Request.expected_type,
            ).join(Request, Error.request_id == Request.id)

            stmt = stmt.where(
                Error.is_resolved == False,  # noqa: E712
                Error.request_id.is_not(None),  # type: ignore[union-attr]
            )

            if error_type:
                stmt = stmt.where(Error.error_type == error_type)
            if continuation:
                stmt = stmt.where(Request.continuation == continuation)

            result = await session.execute(stmt)
            return [tuple(row) for row in result.all()]

    async def get_error_info_for_progress(self, error_id: int) -> dict | None:
        """Get error info for progress events.

        Args:
            error_id: The database ID of the error.

        Returns:
            Dict with url and continuation, or None if not found.
        """
        row = await self.get_error_with_request(error_id)
        if row is None:
            return None
        url = row[4]
        continuation = row[8]
        return {"url": url, "continuation": continuation}

    async def batch_requeue_errors(
        self,
        error_type: str | None = None,
        continuation: str | None = None,
    ) -> list[int]:
        """Batch requeue errors matching the given filters.

        Args:
            error_type: Optional error type filter.
            continuation: Optional continuation filter.

        Returns:
            List of new request IDs created.
        """
        rows = await self.get_errors_for_requeue(error_type, continuation)
        if not rows:
            return []

        new_request_ids = []
        error_ids = []

        for row in rows:
            (
                error_id,
                request_id,
                method,
                url,
                headers_json,
                cookies_json,
                body,
                row_continuation,
                current_location,
                accumulated_data_json,
                aux_data_json,
                permanent_json,
                priority,
                request_type,
                expected_type,
            ) = row

            new_request_id = await self.insert_requeue_request(  # type: ignore[attr-defined]
                priority=priority or 0,
                method=method,
                url=url,
                headers_json=headers_json,
                cookies_json=cookies_json,
                body=body,
                continuation=row_continuation,
                current_location=current_location,
                accumulated_data_json=accumulated_data_json,
                aux_data_json=aux_data_json,
                permanent_json=permanent_json,
                original_request_id=request_id,
                request_type=request_type or "navigating",
                expected_type=expected_type,
            )
            new_request_ids.append(new_request_id)
            error_ids.append(error_id)

        if error_ids:
            async with self._lock, self._session_factory() as session:
                await session.execute(
                    update(Error)
                    .where(Error.id.in_(error_ids))  # type: ignore[union-attr]
                    .values(
                        is_resolved=True,
                        resolved_at=func.current_timestamp(),
                        resolution_notes="Batch requeued",
                    )
                )
                await session.commit()

        return new_request_ids
