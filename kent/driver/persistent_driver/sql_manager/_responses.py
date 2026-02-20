"""Response storage operations for SQLManager."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from kent.driver.persistent_driver.models import (
    ArchivedFile,
    CompressionDict,
    IncidentalRequest,
    Request,
)

if TYPE_CHECKING:
    import asyncio


class ResponseStorageMixin:
    """Response, ArchivedFile, IncidentalRequest, and CompressionDict operations."""

    _lock: asyncio.Lock
    _session_factory: async_sessionmaker

    # --- Response Storage ---

    async def store_response(
        self,
        request_id: int,
        status_code: int,
        headers_json: str | None,
        url: str,
        compressed_content: bytes | None,
        content_size_original: int,
        content_size_compressed: int,
        dict_id: int | None,
        continuation: str,
        warc_record_id: str,
        speculation_outcome: str | None = None,
    ) -> int:
        """Store an HTTP response by updating the request row.

        Args:
            request_id: The database ID of the request to update.
            status_code: HTTP status code.
            headers_json: JSON-encoded response headers.
            url: Final URL after redirects.
            compressed_content: Compressed content bytes.
            content_size_original: Original content size.
            content_size_compressed: Compressed content size.
            dict_id: Compression dictionary ID if used.
            continuation: Continuation method name (unused, kept for API compat).
            warc_record_id: UUID for WARC export.
            speculation_outcome: For speculative requests: 'success', 'stopped', 'skipped'.

        Returns:
            The request_id (same as input).
        """
        from sqlalchemy import func

        async with self._lock, self._session_factory() as session:
            await session.execute(
                update(Request)
                .where(Request.id == request_id)
                .values(
                    response_status_code=status_code,
                    response_headers_json=headers_json,
                    response_url=url,
                    content_compressed=compressed_content,
                    content_size_original=content_size_original,
                    content_size_compressed=content_size_compressed,
                    compression_dict_id=dict_id,
                    warc_record_id=warc_record_id,
                    speculation_outcome=speculation_outcome,
                    response_created_at=func.current_timestamp(),
                )
            )
            await session.commit()
            return request_id

    async def store_archived_file(
        self,
        request_id: int,
        file_path: str,
        original_url: str,
        expected_type: str | None,
        file_size: int,
        content_hash: str | None,
    ) -> int:
        """Store archived file metadata.

        Args:
            request_id: The database ID of the associated request.
            file_path: Local file system path.
            original_url: URL the file was downloaded from.
            expected_type: Expected file type.
            file_size: File size in bytes.
            content_hash: SHA256 hash of content.

        Returns:
            The database ID of the archived file record.
        """
        async with self._lock, self._session_factory() as session:
            af = ArchivedFile(
                request_id=request_id,
                file_path=file_path,
                original_url=original_url,
                expected_type=expected_type,
                file_size=file_size,
                content_hash=content_hash,
            )
            session.add(af)
            await session.commit()
            await session.refresh(af)
            return af.id  # type: ignore[return-value]

    # --- Incidental Requests (Playwright driver) ---

    async def insert_incidental_request(
        self,
        parent_request_id: int,
        resource_type: str,
        method: str,
        url: str,
        headers_json: str | None = None,
        body: bytes | None = None,
        status_code: int | None = None,
        response_headers_json: str | None = None,
        content_compressed: bytes | None = None,
        content_size_original: int | None = None,
        content_size_compressed: int | None = None,
        compression_dict_id: int | None = None,
        started_at_ns: int | None = None,
        completed_at_ns: int | None = None,
        from_cache: bool = False,
        failure_reason: str | None = None,
    ) -> int:
        """Store an incidental browser request (Playwright driver).

        Args:
            parent_request_id: ID of the primary request that triggered this navigation.
            resource_type: Resource type (document, stylesheet, image, script, etc.).
            method: HTTP method.
            url: Request URL.
            headers_json: JSON-encoded request headers.
            body: Request body (if any).
            status_code: HTTP status code (None if request failed).
            response_headers_json: JSON-encoded response headers.
            content_compressed: Zstd-compressed response body.
            content_size_original: Original response size.
            content_size_compressed: Compressed response size.
            compression_dict_id: Compression dictionary ID if used.
            started_at_ns: Nanosecond timestamp when request started.
            completed_at_ns: Nanosecond timestamp when request completed.
            from_cache: Whether browser served from cache.
            failure_reason: Reason if request failed (timeout, aborted, etc.).

        Returns:
            The database ID of the stored incidental request.
        """
        async with self._lock, self._session_factory() as session:
            ir = IncidentalRequest(
                parent_request_id=parent_request_id,
                resource_type=resource_type,
                method=method,
                url=url,
                headers_json=headers_json,
                body=body,
                status_code=status_code,
                response_headers_json=response_headers_json,
                content_compressed=content_compressed,
                content_size_original=content_size_original,
                content_size_compressed=content_size_compressed,
                compression_dict_id=compression_dict_id,
                started_at_ns=started_at_ns,
                completed_at_ns=completed_at_ns,
                from_cache=from_cache,
                failure_reason=failure_reason,
            )
            session.add(ir)
            await session.commit()
            await session.refresh(ir)
            return ir.id  # type: ignore[return-value]

    async def get_incidental_requests(
        self, parent_request_id: int
    ) -> list[dict[str, Any]]:
        """Get all incidental requests for a parent request.

        Args:
            parent_request_id: ID of the parent request.

        Returns:
            List of incidental request records as dicts.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(IncidentalRequest)
                .where(
                    IncidentalRequest.parent_request_id == parent_request_id
                )
                .order_by(IncidentalRequest.started_at_ns.asc())  # type: ignore[union-attr]
            )
            rows = result.scalars().all()
            return [
                {
                    "id": r.id,
                    "parent_request_id": r.parent_request_id,
                    "resource_type": r.resource_type,
                    "method": r.method,
                    "url": r.url,
                    "headers_json": r.headers_json,
                    "body": r.body,
                    "status_code": r.status_code,
                    "response_headers_json": r.response_headers_json,
                    "content_compressed": r.content_compressed,
                    "content_size_original": r.content_size_original,
                    "content_size_compressed": r.content_size_compressed,
                    "compression_dict_id": r.compression_dict_id,
                    "started_at_ns": r.started_at_ns,
                    "completed_at_ns": r.completed_at_ns,
                    "from_cache": r.from_cache,
                    "failure_reason": r.failure_reason,
                    "created_at": r.created_at,
                }
                for r in rows
            ]

    async def get_incidental_request_by_id(
        self, incidental_id: int
    ) -> dict[str, Any] | None:
        """Get a single incidental request by ID.

        Args:
            incidental_id: ID of the incidental request.

        Returns:
            Incidental request record as dict, or None if not found.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(IncidentalRequest).where(
                    IncidentalRequest.id == incidental_id
                )
            )
            r = result.scalar_one_or_none()
            if not r:
                return None
            return {
                "id": r.id,
                "parent_request_id": r.parent_request_id,
                "resource_type": r.resource_type,
                "method": r.method,
                "url": r.url,
                "headers_json": r.headers_json,
                "body": r.body,
                "status_code": r.status_code,
                "response_headers_json": r.response_headers_json,
                "content_compressed": r.content_compressed,
                "content_size_original": r.content_size_original,
                "content_size_compressed": r.content_size_compressed,
                "compression_dict_id": r.compression_dict_id,
                "started_at_ns": r.started_at_ns,
                "completed_at_ns": r.completed_at_ns,
                "from_cache": r.from_cache,
                "failure_reason": r.failure_reason,
                "created_at": r.created_at,
            }

    async def get_response_compressed(
        self, request_id: int
    ) -> tuple[bytes | None, int | None] | None:
        """Get compressed response content and dict ID for a request.

        Args:
            request_id: The database ID of the request.

        Returns:
            Tuple of (compressed_content, dict_id) or None if not found.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Request.content_compressed,
                    Request.compression_dict_id,
                ).where(Request.id == request_id)
            )
            row = result.first()
            return tuple(row) if row else None  # type: ignore[return-value]

    async def get_cached_response(
        self, cache_key: str
    ) -> dict[str, Any] | None:
        """Look up a cached response by cache key.

        Returns the most recent successful (2xx) response for the given
        cache key, if one exists.

        Args:
            cache_key: The cache key (hash of method+url+body+headers).

        Returns:
            Dictionary with response data if found, None otherwise.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Request.id,
                    Request.response_status_code,
                    Request.response_headers_json,
                    Request.response_url,
                    Request.content_compressed,
                    Request.compression_dict_id,
                    Request.response_created_at,
                    Request.method,
                )
                .where(
                    Request.cache_key == cache_key,
                    Request.response_status_code >= 200,  # type: ignore[operator]
                    Request.response_status_code < 300,  # type: ignore[operator]
                )
                .order_by(Request.id.desc())  # type: ignore[union-attr]
                .limit(1)
            )
            row = result.first()
            if row is None:
                return None
            return {
                "id": row[0],
                "request_id": row[0],
                "status_code": row[1],
                "headers_json": row[2],
                "url": row[3],
                "content_compressed": row[4],
                "compression_dict_id": row[5],
                "created_at": row[6],
                "method": row[7],
            }

    async def get_compression_dict(self, dict_id: int) -> bytes | None:
        """Get compression dictionary data by ID.

        Args:
            dict_id: The database ID of the compression dictionary.

        Returns:
            Dictionary bytes if found, None otherwise.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(CompressionDict.dictionary_data).where(
                    CompressionDict.id == dict_id
                )
            )
            return result.scalar()
