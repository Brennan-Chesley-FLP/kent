"""Testing utilities for LocalDevDriver.

This module provides testing infrastructure for LocalDevDriver tests:
- TestRequestManager: A mock request manager for testing
- Helper functions for creating test fixtures
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

from kent.common.request_manager import (
    AsyncRequestManager,
)
from kent.data_types import BaseRequest, Response


@dataclass
class MockResponse:
    """A mock response configuration.

    Attributes:
        content: Raw bytes content of the response.
        text: Decoded text content (if not provided, derived from content).
        status_code: HTTP status code (default: 200).
        headers: Response headers (default: empty dict).
    """

    content: bytes = b""
    text: str | None = None
    status_code: int = 200
    headers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Derive text from content if not provided."""
        if self.text is None:
            self.text = self.content.decode("utf-8", errors="replace")


class TestRequestManager(AsyncRequestManager):
    """Mock request manager for testing that provides canned responses.

    This request manager allows tests to:
    - Supply canned responses for specific URLs
    - Raise exceptions for specific URLs (to test error handling)
    - Track which requests were made
    - Verify request ordering and parameters

    Example:
        manager = TestRequestManager()
        manager.add_response(
            "https://example.com/page1",
            MockResponse(content=b"<html>Page 1</html>")
        )
        manager.add_error(
            "https://example.com/error",
            RequestTimeoutException(url="...", timeout_seconds=30.0)
        )

        # Use with LocalDevDriver
        async with LocalDevDriver.open(
            scraper, db_path, request_manager=manager
        ) as driver:
            await driver.run()
    """

    def __init__(self) -> None:
        """Initialize the test request manager.

        Note: We intentionally skip the parent __init__ to avoid creating
        an httpx client, since we provide mock responses directly.
        """
        # Map of URL -> MockResponse
        self._responses: dict[str, MockResponse] = {}
        # Map of URL -> Exception to raise
        self._errors: dict[str, Exception] = {}
        # Map of URL pattern -> response generator function
        self._response_generators: dict[
            str, Callable[[BaseRequest], MockResponse]
        ] = {}
        # Track all requests made (in order)
        self.requests: list[BaseRequest] = []
        # Count of requests by URL
        self.request_counts: dict[str, int] = {}

    def add_response(self, url: str, response: MockResponse) -> None:
        """Add a mock response for a specific URL.

        Args:
            url: The URL to mock (exact match).
            response: The mock response to return.
        """
        self._responses[url] = response

    def add_error(self, url: str, error: Exception) -> None:
        """Add an exception to raise for a specific URL.

        Args:
            url: The URL that should trigger the error.
            error: The exception to raise when this URL is requested.
        """
        self._errors[url] = error

    def add_response_generator(
        self,
        url_prefix: str,
        generator: Callable[[BaseRequest], MockResponse],
    ) -> None:
        """Add a response generator for URLs matching a prefix.

        Args:
            url_prefix: URL prefix to match (e.g., "https://example.com/items/").
            generator: Function that takes a BaseRequest and returns MockResponse.
        """
        self._response_generators[url_prefix] = generator

    def clear(self) -> None:
        """Clear all mock responses, errors, and request history."""
        self._responses.clear()
        self._errors.clear()
        self._response_generators.clear()
        self.requests.clear()
        self.request_counts.clear()

    def get_request_count(self, url: str) -> int:
        """Get the number of times a URL was requested.

        Args:
            url: The URL to check.

        Returns:
            Number of times this URL was requested.
        """
        return self.request_counts.get(url, 0)

    async def resolve_request(self, request: BaseRequest) -> Response:
        """Process a request and return a mock response.

        Args:
            request: The request to process.

        Returns:
            Response with mocked data.

        Raises:
            Exception: If an error is configured for this URL.
            ValueError: If no mock is configured for this URL.
        """
        url = request.request.url

        # Track the request
        self.requests.append(request)
        self.request_counts[url] = self.request_counts.get(url, 0) + 1

        # Check for configured error
        if url in self._errors:
            raise self._errors[url]

        # Check for exact URL match
        if url in self._responses:
            mock = self._responses[url]
            return Response(
                request=request,
                status_code=mock.status_code,
                headers=mock.headers,
                content=mock.content,
                text=mock.text
                or mock.content.decode("utf-8", errors="replace"),
                url=url,
            )

        # Check for URL prefix match with generator
        for prefix, generator in self._response_generators.items():
            if url.startswith(prefix):
                mock = generator(request)
                return Response(
                    request=request,
                    status_code=mock.status_code,
                    headers=mock.headers,
                    content=mock.content,
                    text=mock.text
                    or mock.content.decode("utf-8", errors="replace"),
                    url=url,
                )

        # No mock configured - raise error for better test isolation
        raise ValueError(
            f"No mock response configured for URL: {url}. "
            "Use add_response() or add_response_generator() to configure a mock."
        )

    async def close(self) -> None:
        """No-op close for compatibility with real request managers."""
        pass


def create_html_response(
    body: str,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
) -> MockResponse:
    """Create a mock HTML response.

    Args:
        body: HTML body content.
        status_code: HTTP status code.
        headers: Optional response headers.

    Returns:
        MockResponse configured for HTML content.
    """
    default_headers = {"Content-Type": "text/html; charset=utf-8"}
    if headers:
        default_headers.update(headers)

    content = body.encode("utf-8")
    return MockResponse(
        content=content,
        text=body,
        status_code=status_code,
        headers=default_headers,
    )


def create_json_response(
    data: Any,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
) -> MockResponse:
    """Create a mock JSON response.

    Args:
        data: JSON-serializable data.
        status_code: HTTP status code.
        headers: Optional response headers.

    Returns:
        MockResponse configured for JSON content.
    """
    import json

    default_headers = {"Content-Type": "application/json"}
    if headers:
        default_headers.update(headers)

    text = json.dumps(data)
    content = text.encode("utf-8")
    return MockResponse(
        content=content,
        text=text,
        status_code=status_code,
        headers=default_headers,
    )
