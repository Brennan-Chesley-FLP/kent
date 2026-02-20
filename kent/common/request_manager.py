"""Request managers for handling HTTP requests.

This module provides SyncRequestManager and AsyncRequestManager classes that
encapsulate the HTTP client, and request resolution logic.

The request manager is responsible for:
- Maintaining the HTTP client (httpx.Client or httpx.AsyncClient)
- Converting HTTP responses to Response objects

This separation allows drivers to focus on queue management and scraper
orchestration while delegating HTTP concerns to the request manager.
"""

from __future__ import annotations

import logging
import ssl
from typing import TYPE_CHECKING, Any, cast

import httpx

from kent.common.exceptions import (
    HTMLResponseAssumptionException,
    RequestTimeoutException,
    TransientException,
)
from kent.data_types import BaseRequest, Response

if TYPE_CHECKING:
    from kent.driver.persistent_driver.sql_manager import (
        SQLManager,
    )

logger = logging.getLogger(__name__)


class SyncRequestManager:
    """Manages HTTP requests for synchronous drivers.

    This class encapsulates:

    - httpx.Client lifecycle
    - Request resolution (URL fetching)
    - Response transformation

    Example::

        manager = SyncRequestManager(
            ssl_context=scraper.get_ssl_context(),
            timeout=30.0,
        )
        response = manager.resolve_request(request)
    """

    def __init__(
        self,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float | None = None,
    ) -> None:
        """Initialize the request manager.

        Args:
            ssl_context: Optional SSL context for HTTPS connections. Use this
                for servers requiring specific cipher suites.
            timeout: Request timeout in seconds. None means no timeout (default).
        """
        self.timeout = timeout

        # Initialize httpx client
        if ssl_context:
            self._client = httpx.Client(verify=ssl_context, timeout=timeout)
        else:
            self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        """Close the HTTP client and release resources."""
        self._client.close()

    def __enter__(self) -> SyncRequestManager:
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit - closes the client."""
        self.close()

    def resolve_request(self, request: BaseRequest) -> Response:
        """Fetch a BaseRequest and return the Response.

        Args:
            request: The BaseRequest to fetch. URL should be absolute.

        Returns:
            Response containing the HTTP response data.

        Raises:
            HTMLResponseAssumptionException: If server returns 5xx status code.
            httpx.TimeoutException: If request times out (for retry handling).
        """
        # Use the modified request for HTTP
        http_params = request.request

        try:
            http_response = self._client.request(
                method=http_params.method.value,
                url=http_params.url,
                headers=http_params.headers,
                cookies=http_params.cookies,
                content=http_params.data
                if isinstance(http_params.data, bytes)
                else None,
                data=http_params.data  # type: ignore[arg-type]
                if isinstance(http_params.data, dict)
                else None,
            )
        except httpx.TimeoutException:
            raise RequestTimeoutException(
                url=http_params.url, timeout_seconds=30
            )

        # Check for server errors (5xx status codes)
        if http_response.status_code >= 500:
            raise HTMLResponseAssumptionException(
                status_code=http_response.status_code,
                expected_codes=[200],
                url=http_params.url,
            )

        response = Response(
            status_code=http_response.status_code,
            headers=dict(http_response.headers),
            content=http_response.content,
            text=http_response.text,
            url=http_params.url,
            request=request,
        )

        return response


class AsyncRequestManager:
    """Manages HTTP requests for asynchronous drivers.

    This class encapsulates:

    - httpx.AsyncClient lifecycle
    - Request resolution (URL fetching)
    - Response transformation

    Example::

        manager = AsyncRequestManager(
            ssl_context=scraper.get_ssl_context(),
            timeout=30.0,
        )
        response = await manager.resolve_request(request)
    """

    def __init__(
        self,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float | None = None,
    ) -> None:
        """Initialize the request manager.

        Args:
            ssl_context: Optional SSL context for HTTPS connections. Use this
                for servers requiring specific cipher suites.
            timeout: Request timeout in seconds. None means no timeout (default).
        """
        self.timeout = timeout

        # Initialize httpx async client
        if ssl_context:
            self._client = httpx.AsyncClient(
                verify=ssl_context, timeout=timeout
            )
        else:
            self._client = httpx.AsyncClient(timeout=timeout)

    async def close(self) -> None:
        """Close the HTTP client and release resources."""
        await self._client.aclose()

    async def __aenter__(self) -> AsyncRequestManager:
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit - closes the client."""
        await self.close()

    async def resolve_request(self, request: BaseRequest) -> Response:
        """Fetch a BaseRequest and return the Response.

        Args:
            request: The BaseRequest to fetch. URL should be absolute.

        Returns:
            Response containing the HTTP response data.

        Raises:
            HTMLResponseAssumptionException: If server returns 5xx status code.
            httpx.TimeoutException: If request times out (for retry handling).
        """

        # Use the modified request for HTTP
        http_params = request.request

        # Prepare content and data parameters for httpx
        request_data = http_params.data
        content_param: bytes | None = (
            request_data if isinstance(request_data, bytes) else None
        )
        data_param: dict[str, Any] | None = (
            cast(dict[str, Any], request_data)
            if isinstance(request_data, dict)
            else None
        )

        # Make the HTTP request
        try:
            http_response = await self._client.request(
                method=http_params.method.value,
                url=http_params.url,
                headers=http_params.headers,
                cookies=http_params.cookies,
                content=content_param,
                data=data_param,
            )
        except httpx.TimeoutException:
            raise RequestTimeoutException(
                url=http_params.url,
                timeout_seconds=30,
            )

        # Check for server errors (5xx status codes)
        if http_response.status_code >= 500:
            raise HTMLResponseAssumptionException(
                status_code=http_response.status_code,
                expected_codes=[200],
                url=http_params.url,
            )

        response = Response(
            status_code=http_response.status_code,
            headers=dict(http_response.headers),
            content=http_response.content,
            text=http_response.text,
            url=http_params.url,
            request=request,
        )

        return response


class SQLBackedAsyncRequestManager(AsyncRequestManager):
    """AsyncRequestManager with SQLite-backed retry/backoff and response storage.

    Extends AsyncRequestManager to add:

    - Exponential backoff retry logic for transient errors
    - Maximum backoff time enforcement
    - Database storage of requests and responses via SQLManager

    The retry algorithm uses exponential backoff::

        next_retry_delay = base_delay * 2^retry_count

    Individual delays are capped at max_backoff_time / 4 to prevent
    excessively long single delays. When cumulative backoff exceeds
    max_backoff_time, the request is marked as failed.

    Example::

        manager = SQLBackedAsyncRequestManager(
            sql_manager=sql_manager,
            max_backoff_time=3600.0,  # 1 hour max
            ssl_context=scraper.get_ssl_context(),
            timeout=30.0,
        )
        # With retry handling:
        response = await manager.resolve_request_with_retry(request_id, request)

        # Or use base class method without retry:
        response = await manager.resolve_request(request)
    """

    def __init__(
        self,
        sql_manager: SQLManager,
        max_backoff_time: float = 3600.0,
        retry_base_delay: float = 1.0,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float | None = None,
    ) -> None:
        """Initialize the SQL-backed request manager.

        Args:
            sql_manager: SQLManager for database operations (requests, responses,
                retry state tracking).
            max_backoff_time: Maximum cumulative backoff time in seconds before
                marking a request as failed. Default: 3600.0 (1 hour).
            retry_base_delay: Base delay for exponential backoff calculation.
                Default: 1.0 second.
            ssl_context: Optional SSL context for HTTPS connections.
            timeout: Request timeout in seconds. None means no timeout.
        """
        super().__init__(
            ssl_context=ssl_context,
            timeout=timeout,
        )
        self.sql_manager = sql_manager
        self.max_backoff_time = max_backoff_time
        self.retry_base_delay = retry_base_delay

    async def resolve_request_with_retry(
        self,
        request_id: int,
        request: BaseRequest,
    ) -> Response:
        """Resolve a request with automatic retry on transient errors.

        This method wraps resolve_request() with retry logic:
        1. Attempts the HTTP request
        2. On TransientException, checks if retry is allowed
        3. If allowed, schedules retry in database and re-raises
        4. If max_backoff exceeded, marks request failed and re-raises

        Args:
            request_id: Database ID of the request (for retry state tracking).
            request: The BaseRequest to fetch.

        Returns:
            Response containing the HTTP response data.

        Raises:
            TransientException: If request fails transiently. Check the
                exception to determine if a retry was scheduled.
            HTMLResponseAssumptionException: If server returns 5xx status code.
        """
        try:
            return await self.resolve_request(request)
        except TransientException as e:
            # Check if we should retry
            should_retry = await self.handle_retry(request_id, e)
            if not should_retry:
                # Max backoff exceeded - mark as failed
                await self.sql_manager.mark_request_failed(request_id, str(e))
            # Re-raise regardless - caller decides whether to continue
            raise

    async def handle_retry(
        self,
        request_id: int,
        error: Exception,
    ) -> bool:
        """Handle retry logic for transient errors with exponential backoff.

        Calculates the next retry delay using exponential backoff formula:
            next_retry_delay = base_delay * 2^retry_count

        Adds the delay to cumulative_backoff. If cumulative_backoff exceeds
        max_backoff_time, returns False to indicate the request should be
        marked as failed instead of retried.

        Args:
            request_id: The database ID of the request.
            error: The transient exception that was raised.

        Returns:
            True if the request was scheduled for retry, False if it should fail.
        """
        # Get current retry state
        retry_state = await self.sql_manager.get_retry_state(request_id)
        if retry_state is None:
            return False

        retry_count, cumulative_backoff = retry_state

        # Calculate next retry delay with exponential backoff
        next_retry_delay = self.retry_base_delay * (2**retry_count)

        # Cap individual retry delay at max_backoff_time / 4 to ensure
        # we don't have a single very long delay
        max_individual_delay = self.max_backoff_time / 4
        next_retry_delay = min(next_retry_delay, max_individual_delay)

        # Check if we would exceed max_backoff_time
        new_cumulative_backoff = cumulative_backoff + next_retry_delay
        if new_cumulative_backoff >= self.max_backoff_time:
            logger.warning(
                f"Request {request_id} exceeded max backoff time "
                f"({new_cumulative_backoff:.1f}s >= {self.max_backoff_time:.1f}s)"
            )
            return False

        # Schedule retry by resetting to pending with updated backoff tracking
        await self.sql_manager.schedule_retry(
            request_id, new_cumulative_backoff, next_retry_delay, str(error)
        )

        logger.info(
            f"Request {request_id} scheduled for retry #{retry_count + 1} "
            f"(delay: {next_retry_delay:.1f}s, cumulative: {new_cumulative_backoff:.1f}s)"
        )

        return True
