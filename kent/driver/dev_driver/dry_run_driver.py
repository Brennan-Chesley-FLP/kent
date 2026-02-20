"""DryRunDriver - Capture continuation yields without network I/O.

This module provides a driver for replaying stored responses through
continuation code without making any actual network requests. It's used
by the debugger's compare functionality to test how code changes affect
scraper output.

The DryRunDriver captures:
- Navigating request yields (without executing HTTP requests)
- Non-navigating request yields (without executing HTTP requests)
- Archive request yields (without downloading files)
- ParsedData yields
- Errors raised during continuation execution

This enables comparison of scraper behavior between different versions
of continuation code against the same stored responses.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
    ScraperYield,
)

if TYPE_CHECKING:
    pass

T = TypeVar("T")


@dataclass
class CapturedRequest:
    """A captured request yield from a dry run.

    Attributes:
        request_type: Type of request (navigating, non_navigating, archive).
        url: The resolved URL for the request.
        method: HTTP method.
        continuation: Continuation method name.
        accumulated_data: Data accumulated through the request chain.
        aux_data: Navigation metadata (tokens, session data).
        permanent: Persistent data (cookies, headers).
        current_location: URL context for relative URL resolution.
        priority: Request priority.
        deduplication_key: Key for deduplication.
        is_speculative: Whether this is a speculative request.
        speculation_id: (function_name, integer_id) for speculative requests.
        expected_type: For archive requests, the expected file type.
    """

    request_type: str  # "navigating", "non_navigating", "archive"
    url: str
    method: str
    continuation: str
    accumulated_data: dict[str, Any]
    aux_data: dict[str, Any]
    permanent: dict[str, Any]
    current_location: str
    priority: int
    deduplication_key: str | None
    is_speculative: bool
    speculation_id: tuple[str, int] | None
    expected_type: str | None = None  # Only for archive requests


@dataclass
class CapturedData:
    """A captured ParsedData yield from a dry run.

    Attributes:
        data: The parsed data dict.
    """

    data: dict[str, Any]


@dataclass
class CapturedError:
    """An error captured during dry run execution.

    Attributes:
        error_type: The exception class name.
        error_message: The exception message.
    """

    error_type: str
    error_message: str


@dataclass
class DryRunResult(Generic[T]):
    """Result of running a continuation in dry-run mode.

    Captures all yields (requests and data) and any error that occurred,
    without executing any network I/O.

    Attributes:
        requests: List of captured request yields.
        data: List of captured ParsedData yields.
        error: Captured error if the continuation raised an exception.
    """

    requests: list[CapturedRequest] = field(default_factory=list)
    data: list[CapturedData] = field(default_factory=list)
    error: CapturedError | None = None


class DryRunDriver(Generic[T]):
    """Driver for replaying responses through continuations without network I/O.

    The DryRunDriver executes continuation methods against stored responses,
    capturing all yields without making any actual HTTP requests or file
    downloads. This enables comparison of scraper behavior across code versions.

    Attributes:
        scraper: The scraper instance to execute continuations on.
    """

    def __init__(self, scraper: BaseScraper[T]) -> None:
        """Initialize the DryRunDriver.

        Args:
            scraper: The scraper instance with continuation methods.
        """
        self.scraper = scraper

    def run_continuation(
        self,
        continuation_name: str,
        response_data: dict[str, Any],
        request_data: dict[str, Any],
    ) -> DryRunResult[T]:
        """Run a continuation against stored response/request data.

        Reconstructs a Response object from stored data and executes the
        continuation method, capturing all yields without network I/O.

        Args:
            continuation_name: Name of the continuation method to execute.
            response_data: Stored response data with fields:
                - status_code: HTTP status code
                - headers_json: JSON string of response headers
                - content: Response bytes
                - text: Decoded response text
                - url: Final URL after redirects
            request_data: Stored request data with fields:
                - accumulated_data_json: JSON string of accumulated_data
                - aux_data_json: JSON string of aux_data
                - permanent_json: JSON string of permanent data
                - current_location: URL context for relative URLs
                - url: Request URL
                - method: HTTP method
                - continuation: Continuation method name

        Returns:
            DryRunResult containing captured requests, data, and any error.
        """
        # Reconstruct context from stored request data
        accumulated_data = (
            json.loads(request_data["accumulated_data_json"])
            if request_data.get("accumulated_data_json")
            else {}
        )
        aux_data = (
            json.loads(request_data["aux_data_json"])
            if request_data.get("aux_data_json")
            else {}
        )
        permanent = (
            json.loads(request_data["permanent_json"])
            if request_data.get("permanent_json")
            else {}
        )
        current_location = request_data.get("current_location", "")

        # Reconstruct the base request
        base_request = Request(
            request=HTTPRequestParams(
                method=HttpMethod(request_data.get("method", "GET")),
                url=request_data.get("url", ""),
            ),
            continuation=request_data.get("continuation", continuation_name),
            current_location=current_location,
            accumulated_data=accumulated_data,
            aux_data=aux_data,
            permanent=permanent,
        )

        # Reconstruct the Response object
        headers = (
            json.loads(response_data["headers_json"])
            if response_data.get("headers_json")
            else {}
        )

        response = Response(
            status_code=response_data["status_code"],
            headers=headers,
            content=response_data.get("content", b""),
            text=response_data.get("text", ""),
            url=response_data.get("url", ""),
            request=base_request,
        )

        # Run the continuation and capture yields
        return self._execute_and_capture(continuation_name, response)

    def _execute_and_capture(
        self, continuation_name: str, response: Response
    ) -> DryRunResult[T]:
        """Execute a continuation and capture all yields without I/O.

        Args:
            continuation_name: Name of the continuation method.
            response: The Response object to pass to the continuation.

        Returns:
            DryRunResult with captured yields and any error.
        """
        result = DryRunResult[T]()

        try:
            # Get the continuation method
            continuation_method = self.scraper.get_continuation(
                continuation_name
            )

            # Execute the generator
            gen = continuation_method(response)

            # Iterate through yields and capture them
            for item in gen:
                self._capture_yield(item, result)

        except Exception as e:
            # Capture the error
            result.error = CapturedError(
                error_type=type(e).__name__,
                error_message=str(e),
            )

        return result

    def _capture_yield(
        self, item: ScraperYield[T], result: DryRunResult[T]
    ) -> None:
        """Capture a single yield from the continuation.

        Args:
            item: The yielded item (request or data).
            result: The DryRunResult to append to.
        """
        match item:
            case Request():
                result.requests.append(self._capture_request(item))
            case ParsedData():
                result.data.append(self._capture_parsed_data(item))
            case None:
                # None yields are valid but not captured
                pass

    def _capture_request(self, request: Request) -> CapturedRequest:
        """Capture a Request yield.

        Args:
            request: The Request to capture.

        Returns:
            CapturedRequest representation.
        """
        # Derive request_type from flags
        if request.archive:
            request_type = "archive"
        elif request.nonnavigating:
            request_type = "non_navigating"
        else:
            request_type = "navigating"

        return CapturedRequest(
            request_type=request_type,
            url=request.request.url,
            method=request.request.method.value,
            continuation=(
                request.continuation
                if isinstance(request.continuation, str)
                else request.continuation.__name__
            ),
            accumulated_data=request.accumulated_data,
            aux_data=request.aux_data,
            permanent=request.permanent,
            current_location=request.current_location,
            priority=request.priority,
            deduplication_key=(
                request.deduplication_key
                if isinstance(request.deduplication_key, str)
                else None
            ),
            is_speculative=request.is_speculative,
            speculation_id=request.speculation_id,
            expected_type=request.expected_type if request.archive else None,
        )

    def _capture_parsed_data(self, data: ParsedData[T]) -> CapturedData:
        """Capture a ParsedData yield.

        Args:
            data: The ParsedData to capture.

        Returns:
            CapturedData representation.
        """
        # Convert the data to a dict if it's a Pydantic model
        data_dict = (
            data.data.model_dump()
            if hasattr(data.data, "model_dump")
            else data.data
        )

        return CapturedData(data=cast(dict[str, Any], data_dict))
