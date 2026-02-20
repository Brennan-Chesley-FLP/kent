"""Standalone types and utility functions for the sql_manager package.

Defines data transfer objects (Page, RequeueResult, record dataclasses)
and the compute_cache_key helper used across the driver.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from pydantic import BaseModel


def compute_cache_key(
    method: str,
    url: str,
    body: bytes | None = None,
    headers_json: str | None = None,
) -> str:
    """Compute a cache key for response caching.

    The cache key is a SHA256 hash of the request parameters that affect
    the response: method, URL, body, and headers.

    Args:
        method: HTTP method (GET, POST, etc.).
        url: Request URL.
        body: Request body bytes (for POST/PUT requests).
        headers_json: JSON-encoded headers (optional).

    Returns:
        Hex-encoded SHA256 hash string.
    """
    hasher = hashlib.sha256()
    hasher.update(method.encode("utf-8"))
    hasher.update(b"\x00")
    hasher.update(url.encode("utf-8"))
    hasher.update(b"\x00")
    if body:
        hasher.update(body)
    hasher.update(b"\x00")
    if headers_json:
        hasher.update(headers_json.encode("utf-8"))
    return hasher.hexdigest()


T = TypeVar("T")


@dataclass
class Page(Generic[T]):
    """Paginated result set.

    Attributes:
        items: List of items for this page.
        total: Total number of items matching the query.
        offset: Number of items skipped.
        limit: Maximum items per page.
    """

    items: list[T]
    total: int
    offset: int
    limit: int

    @property
    def has_more(self) -> bool:
        """Check if there are more items after this page."""
        return self.offset + len(self.items) < self.total

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "items": [
                item.to_dict() if hasattr(item, "to_dict") else str(item)
                for item in self.items
            ],
            "total": self.total,
            "offset": self.offset,
            "limit": self.limit,
            "has_more": self.has_more,
        }

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.to_dict())


class RequeueResult(BaseModel):
    """Result of a requeue operation.

    Reports what was affected by a requeue operation including created
    requests, cleared responses, downstream artifacts, and resolved errors.

    Attributes:
        requeued_request_ids: List of new request IDs created.
        cleared_response_ids: List of response IDs deleted.
        cleared_downstream_request_ids: List of downstream request IDs deleted.
        cleared_result_ids: List of result IDs deleted.
        cleared_error_ids: List of error IDs deleted.
        resolved_error_ids: List of error IDs marked as resolved.
        dry_run: Boolean indicating if this was a dry run.
    """

    requeued_request_ids: list[int] = []
    cleared_response_ids: list[int] = []
    cleared_downstream_request_ids: list[int] = []
    cleared_result_ids: list[int] = []
    cleared_error_ids: list[int] = []
    resolved_error_ids: list[int] = []
    dry_run: bool = False


@dataclass
class RequestRecord:
    """Request record from database.

    Represents a row from the requests table with essential fields
    for display and inspection.
    """

    id: int
    status: str
    priority: int
    queue_counter: int
    method: str
    url: str
    continuation: str
    current_location: str
    created_at: str | None
    started_at: str | None
    completed_at: str | None
    retry_count: int
    cumulative_backoff: float | None
    last_error: str | None
    # High-precision monotonic timestamps (nanoseconds from time.monotonic_ns())
    created_at_ns: int | None = None
    started_at_ns: int | None = None
    completed_at_ns: int | None = None

    @property
    def duration_ns(self) -> int | None:
        """Calculate request duration in nanoseconds (from started to completed)."""
        if self.started_at_ns is not None and self.completed_at_ns is not None:
            return self.completed_at_ns - self.started_at_ns
        return None

    @property
    def duration_ms(self) -> float | None:
        """Calculate request duration in milliseconds."""
        duration = self.duration_ns
        if duration is not None:
            return duration / 1_000_000
        return None

    @property
    def queue_time_ns(self) -> int | None:
        """Calculate time spent in queue in nanoseconds (from created to started)."""
        if self.created_at_ns is not None and self.started_at_ns is not None:
            return self.started_at_ns - self.created_at_ns
        return None

    @property
    def queue_time_ms(self) -> float | None:
        """Calculate time spent in queue in milliseconds."""
        queue_time = self.queue_time_ns
        if queue_time is not None:
            return queue_time / 1_000_000
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "status": self.status,
            "priority": self.priority,
            "queue_counter": self.queue_counter,
            "method": self.method,
            "url": self.url,
            "continuation": self.continuation,
            "current_location": self.current_location,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "retry_count": self.retry_count,
            "cumulative_backoff": self.cumulative_backoff,
            "last_error": self.last_error,
            "created_at_ns": self.created_at_ns,
            "started_at_ns": self.started_at_ns,
            "completed_at_ns": self.completed_at_ns,
            "duration_ms": self.duration_ms,
            "queue_time_ms": self.queue_time_ms,
        }

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.to_dict())


@dataclass
class ResponseRecord:
    """Response projection from the requests table.

    Represents the response-related fields of a request row.
    Does not include compressed content.
    """

    id: int
    status_code: int
    url: str
    content_size_original: int | None
    content_size_compressed: int | None
    continuation: str
    created_at: str | None
    compression_dict_id: int | None
    speculation_outcome: str | None = None

    @property
    def compression_ratio(self) -> float | None:
        """Calculate compression ratio if sizes are available."""
        if self.content_size_original and self.content_size_compressed:
            return round(
                self.content_size_original / self.content_size_compressed, 2
            )
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "status_code": self.status_code,
            "url": self.url,
            "content_size_original": self.content_size_original,
            "content_size_compressed": self.content_size_compressed,
            "compression_ratio": self.compression_ratio,
            "continuation": self.continuation,
            "created_at": self.created_at,
            "compression_dict_id": self.compression_dict_id,
            "speculation_outcome": self.speculation_outcome,
        }

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.to_dict())


@dataclass
class ResultRecord:
    """Result record from database.

    Represents a row from the results table with essential fields
    for display.
    """

    id: int
    request_id: int | None
    result_type: str
    data_json: str
    is_valid: bool
    validation_errors_json: str | None
    created_at: str | None

    @property
    def data(self) -> dict[str, Any] | None:
        """Parse and return the data as a dictionary."""
        if self.data_json:
            return json.loads(self.data_json)
        return None

    @property
    def validation_errors(self) -> list[str] | None:
        """Parse and return validation errors as a list."""
        if self.validation_errors_json:
            return json.loads(self.validation_errors_json)
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "request_id": self.request_id,
            "result_type": self.result_type,
            "data": json.loads(self.data_json) if self.data_json else None,
            "is_valid": self.is_valid,
            "validation_errors": (
                json.loads(self.validation_errors_json)
                if self.validation_errors_json
                else None
            ),
            "created_at": self.created_at,
        }

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.to_dict())
