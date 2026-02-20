"""Read-only inspection methods for LocalDevDriverDebugger."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlmodel import select

from kent.driver.dev_driver.models import (
    CompressionDict,
    Error,
    IncidentalRequest,
    Request,
    Result,
)
from kent.driver.dev_driver.sql_manager import (
    Page,
    RequestRecord,
    ResponseRecord,
    ResultRecord,
    SQLManager,
)

if TYPE_CHECKING:
    pass


class InspectionMixin:
    """Read-only inspection methods for requests, responses, errors, results, and more."""

    sql: SQLManager
    _session_factory: async_sessionmaker
    read_only: bool

    # =========================================================================
    # Request Inspection
    # =========================================================================

    async def list_requests(
        self,
        status: Literal[
            "pending", "in_progress", "completed", "failed", "held"
        ]
        | None = None,
        continuation: str | None = None,
        limit: int = 100,
        offset: int = 0,
        sort: str = "queue",
    ) -> Page[RequestRecord]:
        """List requests with optional filtering.

        Args:
            status: Filter by request status.
            continuation: Filter by continuation (step name).
            limit: Maximum number of requests to return.
            offset: Number of requests to skip (for pagination).
            sort: Sort order - "queue" (default), "id_asc", or "id_desc".

        Returns:
            Page object containing RequestRecord items.
        """
        return await self.sql.list_requests(
            status=status,
            continuation=continuation,
            limit=limit,
            offset=offset,
            sort=sort,
        )

    async def get_request(self, request_id: int) -> RequestRecord | None:
        """Get a single request by ID.

        Args:
            request_id: The request ID.

        Returns:
            RequestRecord if found, None otherwise.
        """
        return await self.sql.get_request(request_id)

    async def get_request_summary(
        self,
    ) -> dict[str, dict[str, int]]:
        """Get summary of request counts by status and continuation.

        Returns:
            Dictionary mapping continuation -> {status -> count}.
            Includes a special "all" key for totals across all continuations.
        """
        stats = await self.sql.get_stats()
        queue_stats = stats.queue.by_continuation

        summary: dict[str, dict[str, int]] = {"all": {}}
        for continuation, status_dict in queue_stats.items():
            if continuation not in summary:
                summary[continuation] = {}
            for status, count in status_dict.items():
                summary[continuation][status] = count
                summary["all"][status] = summary["all"].get(status, 0) + count

        return summary

    # =========================================================================
    # Response Inspection
    # =========================================================================

    async def list_responses(
        self,
        continuation: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Page[ResponseRecord]:
        """List responses with optional filtering.

        Args:
            continuation: Filter by continuation (step name).
            limit: Maximum number of responses to return.
            offset: Number of responses to skip (for pagination).

        Returns:
            Page object containing ResponseRecord items.
        """
        return await self.sql.list_responses(
            continuation=continuation, limit=limit, offset=offset
        )

    async def get_response(self, request_id: int) -> ResponseRecord | None:
        """Get a single response by request ID.

        Args:
            request_id: The request ID.

        Returns:
            ResponseRecord if found, None otherwise.
        """
        return await self.sql.get_response(request_id)

    async def get_response_content(self, request_id: int) -> bytes | None:
        """Get decompressed response content.

        Args:
            request_id: The request ID.

        Returns:
            Decompressed response content bytes, or None if not found.
        """
        return await self.sql.get_response_content(request_id)

    # =========================================================================
    # Incidental Request Inspection
    # =========================================================================

    async def list_incidental_requests(
        self,
        parent_request_id: int | None = None,
        resource_type: str | None = None,
        from_cache: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Page[dict[str, Any]]:
        """List incidental requests with optional filtering.

        Args:
            parent_request_id: Filter by parent request ID.
            resource_type: Filter by resource type.
            from_cache: Filter by cache status.
            limit: Maximum number of results to return.
            offset: Number of results to skip (for pagination).

        Returns:
            Page object containing incidental request dictionaries.
        """
        # Build WHERE conditions
        conditions = []
        if parent_request_id is not None:
            conditions.append(
                IncidentalRequest.parent_request_id == parent_request_id
            )
        if resource_type is not None:
            conditions.append(IncidentalRequest.resource_type == resource_type)
        if from_cache is not None:
            conditions.append(IncidentalRequest.from_cache == from_cache)

        async with self._session_factory() as session:
            # Get total count
            count_stmt = select(sa.func.count()).select_from(IncidentalRequest)
            for cond in conditions:
                count_stmt = count_stmt.where(cond)

            count_result = await session.execute(count_stmt)
            total = count_result.scalar() or 0

            # Get page of results
            query = select(
                IncidentalRequest.id,
                IncidentalRequest.parent_request_id,
                IncidentalRequest.resource_type,
                IncidentalRequest.method,
                IncidentalRequest.url,
                IncidentalRequest.status_code,
                IncidentalRequest.content_size_original,
                IncidentalRequest.from_cache,
                IncidentalRequest.failure_reason,
                IncidentalRequest.created_at,
            ).order_by(IncidentalRequest.created_at.desc())  # type: ignore[union-attr]

            for cond in conditions:
                query = query.where(cond)

            query = query.limit(limit).offset(offset)
            result = await session.execute(query)
            rows = result.all()

        # Convert to dictionaries
        items = [
            {
                "id": row[0],
                "parent_request_id": row[1],
                "resource_type": row[2],
                "method": row[3],
                "url": row[4],
                "status_code": row[5],
                "content_size_original": row[6],
                "from_cache": bool(row[7]) if row[7] is not None else None,
                "failure_reason": row[8],
                "created_at": row[9],
            }
            for row in rows
        ]

        return Page(
            items=items,
            total=total,
            limit=limit,
            offset=offset,
        )

    async def get_incidental_request(
        self, incidental_id: int
    ) -> dict[str, Any] | None:
        """Get a single incidental request by ID.

        Args:
            incidental_id: The incidental request ID.

        Returns:
            Incidental request dict if found, None otherwise.
        """
        return await self.sql.get_incidental_request_by_id(incidental_id)

    async def get_incidental_request_content(
        self, incidental_id: int
    ) -> bytes | None:
        """Get decompressed incidental request content.

        Args:
            incidental_id: The incidental request ID.

        Returns:
            Decompressed content bytes, or None if not found or no content.
        """
        inc = await self.sql.get_incidental_request_by_id(incidental_id)
        if not inc or not inc.get("content_compressed"):
            return None

        # Decompress content
        import zstandard as zstd

        content_compressed = inc["content_compressed"]
        compression_dict_id = inc.get("compression_dict_id")

        if compression_dict_id:
            # Get compression dictionary
            dict_data = await self.sql.get_compression_dict(
                compression_dict_id
            )
            if dict_data:
                dctx = zstd.ZstdDecompressor(
                    dict_data=zstd.ZstdCompressionDict(dict_data)
                )
            else:
                dctx = zstd.ZstdDecompressor()
        else:
            dctx = zstd.ZstdDecompressor()

        return dctx.decompress(content_compressed)

    # =========================================================================
    # Error Inspection
    # =========================================================================

    async def list_errors(
        self,
        error_type: str | None = None,
        is_resolved: bool | None = None,
        continuation: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Page[dict[str, Any]]:
        """List errors with optional filtering.

        Args:
            error_type: Filter by error type.
            is_resolved: Filter by resolution status.
            continuation: Filter by continuation (step name).
            limit: Maximum number of errors to return.
            offset: Number of errors to skip (for pagination).

        Returns:
            Page object containing error dictionaries.
        """
        # Build WHERE conditions
        conditions = []

        if error_type is not None:
            conditions.append(Error.error_type == error_type)

        if is_resolved is not None:
            conditions.append(Error.is_resolved == is_resolved)

        # Select error columns
        error_columns = [
            Error.id,
            Error.request_id,
            Error.error_type,
            Error.error_class,
            Error.message,
            Error.request_url,
            Error.context_json,
            Error.selector,
            Error.selector_type,
            Error.expected_min,
            Error.expected_max,
            Error.actual_count,
            Error.model_name,
            Error.validation_errors_json,
            Error.failed_doc_json,
            Error.status_code,
            Error.timeout_seconds,
            Error.traceback,
            Error.is_resolved,
            Error.resolved_at,
            Error.resolution_notes,
            Error.created_at,
        ]
        error_column_names = [
            "id",
            "request_id",
            "error_type",
            "error_class",
            "message",
            "request_url",
            "context_json",
            "selector",
            "selector_type",
            "expected_min",
            "expected_max",
            "actual_count",
            "model_name",
            "validation_errors_json",
            "failed_doc_json",
            "status_code",
            "timeout_seconds",
            "traceback",
            "is_resolved",
            "resolved_at",
            "resolution_notes",
            "created_at",
        ]

        async with self._session_factory() as session:
            if continuation is not None:
                # Need to join with requests for continuation filter
                conditions.append(Request.continuation == continuation)

                # Count query with join
                count_stmt = (
                    select(sa.func.count())
                    .select_from(Error)
                    .join(
                        Request, Error.request_id == Request.id, isouter=True
                    )
                )
                for cond in conditions:
                    count_stmt = count_stmt.where(cond)

                count_result = await session.execute(count_stmt)
                total = count_result.scalar() or 0

                # Data query with join
                query = (
                    select(*error_columns)
                    .join(
                        Request, Error.request_id == Request.id, isouter=True
                    )
                    .order_by(Error.created_at.desc())  # type: ignore[union-attr]
                )
                for cond in conditions:
                    query = query.where(cond)

                query = query.limit(limit).offset(offset)
                result = await session.execute(query)
                rows = result.all()
            else:
                # Simple query without join
                count_stmt = select(sa.func.count()).select_from(Error)
                for cond in conditions:
                    count_stmt = count_stmt.where(cond)

                count_result = await session.execute(count_stmt)
                total = count_result.scalar() or 0

                query = select(*error_columns).order_by(
                    Error.created_at.desc()  # type: ignore[union-attr]
                )
                for cond in conditions:
                    query = query.where(cond)

                query = query.limit(limit).offset(offset)
                result = await session.execute(query)
                rows = result.all()

        # Convert rows to dictionaries
        items = []
        for row in rows:
            error_dict = dict(zip(error_column_names, row))
            # Convert SQLite 1/0 to Python bool
            error_dict["is_resolved"] = bool(error_dict["is_resolved"])
            items.append(error_dict)

        return Page(items=items, total=total, limit=limit, offset=offset)

    async def get_error(self, error_id: int) -> dict[str, Any] | None:
        """Get a single error by ID with full details.

        Args:
            error_id: The error ID.

        Returns:
            Error dictionary with all fields, or None if not found.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Error.id,
                    Error.request_id,
                    Error.error_type,
                    Error.error_class,
                    Error.message,
                    Error.request_url,
                    Error.context_json,
                    Error.selector,
                    Error.selector_type,
                    Error.expected_min,
                    Error.expected_max,
                    Error.actual_count,
                    Error.model_name,
                    Error.validation_errors_json,
                    Error.failed_doc_json,
                    Error.status_code,
                    Error.timeout_seconds,
                    Error.traceback,
                    Error.is_resolved,
                    Error.resolved_at,
                    Error.resolution_notes,
                    Error.created_at,
                ).where(Error.id == error_id)
            )
            row = result.first()
            if row:
                error_columns = [
                    "id",
                    "request_id",
                    "error_type",
                    "error_class",
                    "message",
                    "request_url",
                    "context_json",
                    "selector",
                    "selector_type",
                    "expected_min",
                    "expected_max",
                    "actual_count",
                    "model_name",
                    "validation_errors_json",
                    "failed_doc_json",
                    "status_code",
                    "timeout_seconds",
                    "traceback",
                    "is_resolved",
                    "resolved_at",
                    "resolution_notes",
                    "created_at",
                ]
                error_dict = dict(zip(error_columns, row))
                # Convert SQLite 1/0 to Python bool
                error_dict["is_resolved"] = bool(error_dict["is_resolved"])
                return error_dict
            return None

    async def get_error_summary(self) -> dict[str, Any]:
        """Get summary of error counts by type and resolution status.

        Returns:
            Dictionary with error counts by_type, by_continuation, and totals.
        """
        # Get counts by type and resolution
        by_type: dict[str, dict[str, int]] = {}
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Error.error_type,
                    Error.is_resolved,
                    sa.func.count().label("count"),
                ).group_by(Error.error_type, Error.is_resolved)
            )
            rows = result.all()
            for row in rows:
                et = row[0]
                resolved = bool(row[1])
                count = row[2]

                if et not in by_type:
                    by_type[et] = {"resolved": 0, "unresolved": 0}

                if resolved:
                    by_type[et]["resolved"] = count
                else:
                    by_type[et]["unresolved"] = count

        # Get counts by continuation
        by_continuation: dict[str, int] = {}
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Request.continuation,
                    sa.func.count(Error.id),
                )
                .select_from(Error)
                .join(Request, Error.request_id == Request.id)
                .group_by(Request.continuation)
            )
            rows = result.all()
            for row in rows:
                by_continuation[row[0]] = row[1]

        # Get totals
        stats = await self.sql.get_stats()
        error_stats = stats.errors
        totals = {
            "resolved": error_stats.resolved,
            "unresolved": error_stats.unresolved,
            "total": error_stats.total,
        }

        return {
            "by_type": by_type,
            "by_continuation": by_continuation,
            "totals": totals,
        }

    # =========================================================================
    # Result Inspection
    # =========================================================================

    async def list_results(
        self,
        result_type: str | None = None,
        is_valid: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Page[ResultRecord]:
        """List results with optional filtering.

        Args:
            result_type: Filter by result type (Pydantic model class name).
            is_valid: Filter by validation status.
            limit: Maximum number of results to return.
            offset: Number of results to skip (for pagination).

        Returns:
            Page object containing ResultRecord items.
        """
        return await self.sql.list_results(
            result_type=result_type,
            is_valid=is_valid,
            limit=limit,
            offset=offset,
        )

    async def get_result(self, result_id: int) -> ResultRecord | None:
        """Get a single result by ID.

        Args:
            result_id: The result ID.

        Returns:
            ResultRecord if found, None otherwise.
        """
        return await self.sql.get_result(result_id)

    async def get_result_summary(self) -> dict[str, dict[str, int]]:
        """Get summary of result counts by type and validity.

        Returns:
            Dictionary mapping result_type -> {valid, invalid, total}.
        """
        summary: dict[str, dict[str, int]] = {}
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Result.result_type,
                    sa.func.sum(
                        sa.case((Result.is_valid == sa.true(), 1), else_=0)
                    ).label("valid_count"),
                    sa.func.sum(
                        sa.case((Result.is_valid == sa.false(), 1), else_=0)
                    ).label("invalid_count"),
                    sa.func.count().label("total_count"),
                )
                .group_by(Result.result_type)
                .order_by(sa.func.count().desc())
            )
            rows = result.all()
            for row in rows:
                result_type = row[0]
                valid_count = row[1]
                invalid_count = row[2]
                total_count = row[3]

                summary[result_type] = {
                    "valid": valid_count,
                    "invalid": invalid_count,
                    "total": total_count,
                }

        return summary

    # =========================================================================
    # Speculation Inspection
    # =========================================================================

    async def get_speculation_summary(self) -> dict[str, Any]:
        """Get summary of speculation configuration and progress.

        Returns:
            Dictionary with config, progress, and tracking state.
        """
        config = await self.sql.get_speculation_config()
        progress = await self.sql.get_all_speculation_progress()
        tracking = await self.sql.load_all_speculation_states()

        return {
            "config": config,
            "progress": progress,
            "tracking": tracking,
        }

    async def get_speculative_progress(self) -> dict[str, int]:
        """Get current speculative progress for all steps.

        Returns:
            Dictionary mapping step_name -> highest_successful_id.
        """
        return await self.sql.get_all_speculation_progress()

    # =========================================================================
    # Rate Limiter Inspection
    # =========================================================================

    async def get_rate_limiter_state(self) -> dict[str, Any] | None:
        """Get current rate limiter state.

        Returns:
            Dictionary with rate limiter state, or None if no state exists.
        """
        return await self.sql.get_rate_limiter_state()

    async def get_throughput_stats(self) -> dict[str, Any]:
        """Get request throughput statistics.

        Returns:
            Dictionary with throughput stats from get_stats()['throughput'].
        """
        stats = await self.sql.get_stats()
        return stats.throughput.to_dict()

    # =========================================================================
    # Compression Inspection
    # =========================================================================

    async def get_compression_stats(self) -> dict[str, Any]:
        """Get compression statistics.

        Returns:
            Dictionary with compression stats (total, sizes, ratio, etc.).
        """
        stats = await self.sql.get_stats()
        compression_dict = stats.compression.to_dict()
        # Map field names for test compatibility
        return {
            "total": compression_dict.get("total_responses", 0),
            "total_original": compression_dict.get("total_original_bytes", 0),
            "total_compressed": compression_dict.get(
                "total_compressed_bytes", 0
            ),
            "with_dict": compression_dict.get("dict_compressed_count", 0),
            "no_dict": compression_dict.get("no_dict_compressed_count", 0),
            "compression_ratio": compression_dict.get(
                "compression_ratio", 1.0
            ),
        }

    async def list_compression_dicts(self) -> list[dict[str, Any]]:
        """List all compression dictionaries.

        Returns:
            List of compression dictionary metadata dictionaries.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    CompressionDict.id,
                    CompressionDict.continuation,
                    CompressionDict.version,
                    CompressionDict.sample_count,
                    sa.func.length(CompressionDict.dictionary_data).label(
                        "size"
                    ),
                    CompressionDict.created_at,
                ).order_by(CompressionDict.created_at.desc())  # type: ignore[union-attr]
            )
            rows = result.all()
            return [
                {
                    "id": row[0],
                    "continuation": row[1],
                    "version": row[2],
                    "sample_count": row[3],
                    "size": row[4],
                    "created_at": row[5],
                }
                for row in rows
            ]
