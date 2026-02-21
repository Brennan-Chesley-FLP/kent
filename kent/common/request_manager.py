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
)
from kent.data_types import BaseRequest, Response

if TYPE_CHECKING:
    from pyrate_limiter import Rate


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
        rates: list[Rate] | None = None,
    ) -> None:
        """Initialize the request manager.

        Args:
            ssl_context: Optional SSL context for HTTPS connections. Use this
                for servers requiring specific cipher suites.
            timeout: Request timeout in seconds. None means no timeout (default).
            rates: Optional list of pyrate_limiter Rate objects. When provided,
                requests are throttled at the httpx transport layer.
        """
        self.timeout = timeout

        # Initialize httpx client, with rate-limited transport if rates given
        if rates:
            from pyrate_limiter import Limiter
            from pyrate_limiter.extras.httpx_limiter import (
                RateLimiterTransport,
            )

            limiter = Limiter(rates)
            transport_kwargs: dict[str, Any] = {}
            if ssl_context:
                transport_kwargs["verify"] = ssl_context
            transport = RateLimiterTransport(
                limiter=limiter, **transport_kwargs
            )
            self._client = httpx.Client(transport=transport, timeout=timeout)
        elif ssl_context:
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
        rates: list[Rate] | None = None,
    ) -> None:
        """Initialize the request manager.

        Args:
            ssl_context: Optional SSL context for HTTPS connections. Use this
                for servers requiring specific cipher suites.
            timeout: Request timeout in seconds. None means no timeout (default).
            rates: Optional list of pyrate_limiter Rate objects. When provided,
                requests are throttled at the httpx transport layer.
        """
        self.timeout = timeout

        # Initialize httpx async client, with rate-limited transport if rates given
        if rates:
            from pyrate_limiter import Limiter
            from pyrate_limiter.extras.httpx_limiter import (
                AsyncRateLimiterTransport,
            )

            limiter = Limiter(rates)
            transport_kwargs: dict[str, Any] = {}
            if ssl_context:
                transport_kwargs["verify"] = ssl_context
            transport = AsyncRateLimiterTransport(
                limiter=limiter, **transport_kwargs
            )
            self._client = httpx.AsyncClient(
                transport=transport, timeout=timeout
            )
        elif ssl_context:
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
