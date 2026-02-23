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
    from pyrate_limiter import Limiter, Rate


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
        self._ssl_context = ssl_context
        self._rates = rates
        self._limiter: Limiter | None = None
        self._alt_clients: dict[str, httpx.Client] = {}

        # Initialize httpx client, with rate-limited transport if rates given
        if rates:
            from pyrate_limiter import Limiter
            from pyrate_limiter.extras.httpx_limiter import (
                RateLimiterTransport,
            )

            self._limiter = Limiter(rates)
            transport_kwargs: dict[str, Any] = {}
            if ssl_context:
                transport_kwargs["verify"] = ssl_context
            transport = RateLimiterTransport(
                limiter=self._limiter, **transport_kwargs
            )
            self._client = httpx.Client(transport=transport, timeout=timeout)
        elif ssl_context:
            self._client = httpx.Client(verify=ssl_context, timeout=timeout)
        else:
            self._client = httpx.Client(timeout=timeout)

    def _make_client(self, verify: bool | str) -> httpx.Client:
        """Create a new httpx.Client with the given verify setting.

        Shares the same Limiter instance (if rate-limited) so that
        alternate-verify clients are still rate-limited together.
        """
        if self._limiter is not None:
            from pyrate_limiter.extras.httpx_limiter import (
                RateLimiterTransport,
            )

            transport_kwargs: dict[str, Any] = {"verify": verify}
            transport = RateLimiterTransport(
                limiter=self._limiter, **transport_kwargs
            )
            return httpx.Client(transport=transport, timeout=self.timeout)
        return httpx.Client(verify=verify, timeout=self.timeout)

    def _client_for(self, verify: bool | str) -> httpx.Client:
        """Return the appropriate httpx.Client for the given verify value.

        Returns the default client when verify is True (the default).
        Otherwise returns a lazily-created cached alternate client.
        """
        if verify is True:
            return self._client
        key = str(verify)
        if key not in self._alt_clients:
            self._alt_clients[key] = self._make_client(verify)
        return self._alt_clients[key]

    def close(self) -> None:
        """Close the HTTP client and release resources."""
        self._client.close()
        for client in self._alt_clients.values():
            client.close()
        self._alt_clients.clear()

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
        client = self._client_for(http_params.verify)

        try:
            http_response = client.request(
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
        self._ssl_context = ssl_context
        self._rates = rates
        self._limiter: Limiter | None = None
        self._alt_clients: dict[str, httpx.AsyncClient] = {}

        # Initialize httpx async client, with rate-limited transport if rates given
        if rates:
            from pyrate_limiter import Limiter
            from pyrate_limiter.extras.httpx_limiter import (
                AsyncRateLimiterTransport,
            )

            self._limiter = Limiter(rates)
            transport_kwargs: dict[str, Any] = {}
            if ssl_context:
                transport_kwargs["verify"] = ssl_context
            transport = AsyncRateLimiterTransport(
                limiter=self._limiter, **transport_kwargs
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

    def _make_client(self, verify: bool | str) -> httpx.AsyncClient:
        """Create a new httpx.AsyncClient with the given verify setting.

        Shares the same Limiter instance (if rate-limited) so that
        alternate-verify clients are still rate-limited together.
        """
        if self._limiter is not None:
            from pyrate_limiter.extras.httpx_limiter import (
                AsyncRateLimiterTransport,
            )

            transport_kwargs: dict[str, Any] = {"verify": verify}
            transport = AsyncRateLimiterTransport(
                limiter=self._limiter, **transport_kwargs
            )
            return httpx.AsyncClient(transport=transport, timeout=self.timeout)
        return httpx.AsyncClient(verify=verify, timeout=self.timeout)

    def _client_for(self, verify: bool | str) -> httpx.AsyncClient:
        """Return the appropriate httpx.AsyncClient for the given verify value.

        Returns the default client when verify is True (the default).
        Otherwise returns a lazily-created cached alternate client.
        """
        if verify is True:
            return self._client
        key = str(verify)
        if key not in self._alt_clients:
            self._alt_clients[key] = self._make_client(verify)
        return self._alt_clients[key]

    async def close(self) -> None:
        """Close the HTTP client and release resources."""
        await self._client.aclose()
        for client in self._alt_clients.values():
            await client.aclose()
        self._alt_clients.clear()

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
        client = self._client_for(http_params.verify)

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
            http_response = await client.request(
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
