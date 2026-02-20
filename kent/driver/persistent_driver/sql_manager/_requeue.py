"""Enhanced requeue operations for SQLManager."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from kent.driver.persistent_driver.models import (
    Error,
    IncidentalRequest,
    Request,
    Result,
)
from kent.driver.persistent_driver.sql_manager._types import RequeueResult

if TYPE_CHECKING:
    import asyncio


class RequeueMixin:
    """Cross-model enhanced requeue operations."""

    _lock: asyncio.Lock
    _session_factory: async_sessionmaker

    # --- Enhanced Requeue Operations ---

    async def requeue_requests(
        self,
        request_ids: list[int],
        *,
        clear_responses: bool = False,
        clear_downstream: bool = False,
        dry_run: bool = False,
    ) -> RequeueResult:
        """Requeue a list of requests with configurable cleanup behavior.

        Args:
            request_ids: List of request IDs to requeue.
            clear_responses: If True, clear response columns for the requeued requests.
            clear_downstream: If True, recursively delete child requests and artifacts.
            dry_run: If True, report what would happen without making changes.

        Returns:
            RequeueResult with lists of affected IDs and dry_run flag.
        """
        requeue_result = RequeueResult(dry_run=dry_run)

        if not request_ids:
            return requeue_result

        async with self._session_factory() as session:
            # Get original request data
            result = await session.execute(
                select(
                    Request.id,
                    Request.method,
                    Request.url,
                    Request.continuation,
                    Request.priority,
                    Request.headers_json,
                    Request.cookies_json,
                    Request.body,
                    Request.current_location,
                    Request.accumulated_data_json,
                    Request.aux_data_json,
                    Request.permanent_json,
                    Request.request_type,
                    Request.expected_type,
                ).where(Request.id.in_(request_ids))  # type: ignore[union-attr]
            )
            rows = result.all()

        if not rows:
            return requeue_result

        # Build set of all request IDs to affect
        all_affected_request_ids = set(request_ids)

        if clear_downstream:
            async with self._session_factory() as session:
                for request_id in request_ids:
                    # Recursive CTE for downstream requests
                    downstream = (
                        select(Request.id)
                        .where(Request.parent_request_id == request_id)
                        .cte(name="downstream", recursive=True)
                    )
                    downstream = downstream.union_all(
                        select(Request.id).where(
                            Request.parent_request_id == downstream.c.id
                        )
                    )
                    result = await session.execute(select(downstream.c.id))
                    downstream_ids = [r[0] for r in result.all()]
                    all_affected_request_ids.update(downstream_ids)

        affected_list = list(all_affected_request_ids)

        async with self._session_factory() as session:
            if clear_responses and affected_list:
                # Identify requests that have responses (for reporting)
                result = await session.execute(
                    select(Request.id).where(
                        Request.id.in_(affected_list),  # type: ignore[union-attr]
                        Request.response_status_code.isnot(None),  # type: ignore[union-attr]
                    )
                )
                requeue_result.cleared_response_ids = [
                    r[0] for r in result.all()
                ]

            if clear_downstream:
                downstream_request_ids = [
                    rid
                    for rid in all_affected_request_ids
                    if rid not in request_ids
                ]
                requeue_result.cleared_downstream_request_ids = (
                    downstream_request_ids
                )

                if affected_list:
                    result = await session.execute(
                        select(Result.id).where(
                            Result.request_id.in_(affected_list)  # type: ignore[union-attr]
                        )
                    )
                    requeue_result.cleared_result_ids = [
                        r[0] for r in result.all()
                    ]

                    result = await session.execute(
                        select(Error.id).where(
                            Error.request_id.in_(affected_list)  # type: ignore[union-attr]
                        )
                    )
                    requeue_result.cleared_error_ids = [
                        r[0] for r in result.all()
                    ]

        if dry_run:
            requeue_result.requeued_request_ids = list(range(1, len(rows) + 1))
            return requeue_result

        # Execute the requeue and cleanup
        async with self._lock:
            new_request_ids = []
            for row in rows:
                (
                    original_id,
                    method,
                    url,
                    continuation,
                    priority,
                    headers_json,
                    cookies_json,
                    body,
                    current_location,
                    accumulated_data_json,
                    aux_data_json,
                    permanent_json,
                    request_type,
                    expected_type,
                ) = row

                new_request_id = await self._insert_requeue_request_unlocked(  # type: ignore[attr-defined]
                    priority=priority or 0,
                    method=method,
                    url=url,
                    headers_json=headers_json,
                    cookies_json=cookies_json,
                    body=body,
                    continuation=continuation,
                    current_location=current_location,
                    accumulated_data_json=accumulated_data_json,
                    aux_data_json=aux_data_json,
                    permanent_json=permanent_json,
                    original_request_id=original_id,
                    request_type=request_type or "navigating",
                    expected_type=expected_type,
                )
                new_request_ids.append(new_request_id)

            requeue_result.requeued_request_ids = new_request_ids

            async with self._session_factory() as session:
                if clear_responses and requeue_result.cleared_response_ids:
                    # NULL out response columns instead of deleting from a separate table
                    await session.execute(
                        update(Request)
                        .where(
                            Request.id.in_(  # type: ignore[union-attr]
                                requeue_result.cleared_response_ids
                            )
                        )
                        .values(
                            response_status_code=None,
                            response_headers_json=None,
                            response_url=None,
                            content_compressed=None,
                            content_size_original=None,
                            content_size_compressed=None,
                            compression_dict_id=None,
                            response_created_at=None,
                            warc_record_id=None,
                            speculation_outcome=None,
                        )
                    )
                    if affected_list:
                        await session.execute(
                            delete(IncidentalRequest).where(
                                IncidentalRequest.parent_request_id.in_(  # type: ignore[attr-defined]
                                    affected_list
                                )
                            )
                        )

                if clear_downstream:
                    if requeue_result.cleared_result_ids:
                        await session.execute(
                            delete(Result).where(
                                Result.id.in_(  # type: ignore[union-attr]
                                    requeue_result.cleared_result_ids
                                )
                            )
                        )
                    if requeue_result.cleared_error_ids:
                        await session.execute(
                            delete(Error).where(
                                Error.id.in_(requeue_result.cleared_error_ids)  # type: ignore[union-attr]
                            )
                        )
                    if requeue_result.cleared_downstream_request_ids:
                        await session.execute(
                            delete(Request).where(
                                Request.id.in_(  # type: ignore[union-attr]
                                    requeue_result.cleared_downstream_request_ids
                                )
                            )
                        )

                await session.commit()

        return requeue_result

    async def requeue_response(
        self,
        request_id: int,
        *,
        clear_responses: bool = False,
        clear_downstream: bool = False,
        dry_run: bool = False,
    ) -> RequeueResult:
        """Requeue a request that has a response.

        Args:
            request_id: The database ID of the request.
            clear_responses: If True, clear response columns to force re-fetch.
            clear_downstream: If True, recursively delete downstream artifacts.
            dry_run: If True, report what would happen without making changes.

        Returns:
            RequeueResult with lists of affected IDs and dry_run flag.
        """
        return await self.requeue_requests(
            [request_id],
            clear_responses=clear_responses,
            clear_downstream=clear_downstream,
            dry_run=dry_run,
        )

    async def requeue_error(
        self,
        error_id: int,
        *,
        mark_resolved: bool = True,
        clear_responses: bool = False,
        clear_downstream: bool = False,
        dry_run: bool = False,
    ) -> RequeueResult:
        """Requeue from an error with optional resolution marking.

        Args:
            error_id: The database ID of the error.
            mark_resolved: If True, mark error as resolved after requeuing.
            clear_responses: If True, clear response columns for the requeued request.
            clear_downstream: If True, recursively delete downstream artifacts.
            dry_run: If True, report what would happen without making changes.

        Returns:
            RequeueResult with lists of affected IDs and dry_run flag.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(Error.id, Error.request_id).where(Error.id == error_id)
            )
            row = result.first()

        if row is None:
            return RequeueResult(dry_run=dry_run)

        _, request_id = row
        if request_id is None:
            return RequeueResult(dry_run=dry_run)

        requeue_result = await self.requeue_requests(
            [request_id],
            clear_responses=clear_responses,
            clear_downstream=clear_downstream,
            dry_run=dry_run,
        )

        if (
            mark_resolved
            and not dry_run
            and requeue_result.requeued_request_ids
        ):
            async with self._lock, self._session_factory() as session:
                new_request_id = requeue_result.requeued_request_ids[0]
                await session.execute(
                    update(Error)
                    .where(
                        Error.id == error_id,
                        Error.is_resolved == False,  # noqa: E712
                    )
                    .values(
                        is_resolved=True,
                        resolved_at=func.current_timestamp(),
                        resolution_notes=f"Requeued as request {new_request_id}",
                    )
                )
                await session.commit()
                requeue_result.resolved_error_ids = [error_id]

        return requeue_result

    async def requeue_continuation(
        self,
        continuation: str,
        *,
        error_type: str | None = None,
        traceback_contains: str | None = None,
        clear_responses: bool = False,
        clear_downstream: bool = False,
        dry_run: bool = False,
    ) -> RequeueResult:
        """Bulk requeue requests by continuation with optional error filtering.

        Args:
            continuation: The continuation method name to filter by.
            error_type: Optional error type filter.
            traceback_contains: Optional substring to match in error tracebacks.
            clear_responses: If True, clear response columns for the requeued requests.
            clear_downstream: If True, recursively delete downstream artifacts.
            dry_run: If True, report what would happen without making changes.

        Returns:
            RequeueResult with lists of affected IDs and dry_run flag.
        """
        async with self._session_factory() as session:
            if error_type or traceback_contains:
                stmt = (
                    select(Request.id)
                    .distinct()
                    .join(Error, Error.request_id == Request.id)
                    .where(
                        Error.is_resolved == False,  # noqa: E712
                        Request.continuation == continuation,
                    )
                )
                if error_type:
                    stmt = stmt.where(Error.error_type == error_type)
                if traceback_contains:
                    stmt = stmt.where(
                        Error.traceback.like(f"%{traceback_contains}%")  # type: ignore[union-attr]
                    )
            else:
                stmt = select(Request.id).where(
                    Request.continuation == continuation,
                    Request.status == "completed",
                )

            result = await session.execute(stmt)
            request_ids = [r[0] for r in result.all()]

        if not request_ids:
            return RequeueResult(dry_run=dry_run)

        requeue_result = await self.requeue_requests(
            request_ids,
            clear_responses=clear_responses,
            clear_downstream=clear_downstream,
            dry_run=dry_run,
        )

        if (error_type or traceback_contains) and not dry_run and request_ids:
            async with self._session_factory() as session:
                result = await session.execute(
                    select(Error.id).where(
                        Error.request_id.in_(request_ids),  # type: ignore[union-attr]
                        Error.is_resolved == False,  # noqa: E712
                    )
                )
                error_ids = [r[0] for r in result.all()]

            if error_ids:
                async with (
                    self._lock,
                    self._session_factory() as session,
                ):
                    await session.execute(
                        update(Error)
                        .where(Error.id.in_(error_ids))  # type: ignore[union-attr]
                        .values(
                            is_resolved=True,
                            resolved_at=func.current_timestamp(),
                            resolution_notes="Bulk requeued via continuation",
                        )
                    )
                    await session.commit()
                    requeue_result.resolved_error_ids = error_ids

        return requeue_result
