"""Rate-limited request manager using pyrate_limiter.

This module provides a request manager that combines:
- HTTP request handling (via AsyncRequestManager)
- Fixed-window rate limiting (via pyrate_limiter)
- Response caching for development efficiency

Rate limits are declared on scraper classes via BaseScraper.rate_limits
using pyrate_limiter Rate objects.
"""

from __future__ import annotations

import json
import logging
import ssl
import time
from typing import TYPE_CHECKING, Any

import zstandard as zstd
from pyrate_limiter import Limiter, Rate, RateItem

from kent.common.request_manager import (
    AsyncRequestManager,
)
from kent.data_types import BaseRequest, Response
from kent.driver.persistent_driver.rate_limiter import (
    AioSQLiteBucket,
)
from kent.driver.persistent_driver.sql_manager import (
    compute_cache_key,
)

if TYPE_CHECKING:
    from kent.driver.persistent_driver.sql_manager import (
        SQLManager,
    )

logger = logging.getLogger(__name__)


class RateLimitedRequestManager(AsyncRequestManager):
    """Request manager with pyrate_limiter rate limiting and response caching.

    This class extends AsyncRequestManager to add:
    1. Fixed-window rate limiting via pyrate_limiter
    2. Response caching (cache hit skips rate limiter)

    Rate limits are specified as a list of pyrate_limiter Rate objects.
    When multiple rates are provided, all are enforced simultaneously.

    Example:
        from pyrate_limiter import Duration, Rate

        rates = [Rate(5, Duration.SECOND), Rate(100, Duration.MINUTE)]
        manager = RateLimitedRequestManager(
            rates=rates,
            sql_manager=sql_manager,
            ssl_context=scraper.get_ssl_context(),
        )
        await manager.initialize()

        response = await manager.resolve_request(request)
    """

    def __init__(
        self,
        sql_manager: SQLManager,
        rates: list[Rate] | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float | None = None,
    ) -> None:
        """Initialize the rate-limited request manager.

        Args:
            sql_manager: SQLManager for database operations.
            rates: List of pyrate_limiter Rate objects. If None,
                no rate limiting is applied.
            ssl_context: Optional SSL context for HTTPS connections.
            timeout: Request timeout in seconds. None means no timeout.
        """
        super().__init__(
            ssl_context=ssl_context,
            timeout=timeout,
        )
        self.sql_manager = sql_manager
        self._rates = rates
        self._limiter: Limiter | None = None

        # Statistics
        self._total_requests = 0
        self._total_successes = 0

    async def initialize(self) -> None:
        """Initialize the rate limiter.

        Creates the AioSQLiteBucket and Limiter if rates are configured.
        """
        if self._rates:
            bucket = AioSQLiteBucket(
                self.sql_manager._session_factory, self._rates
            )
            self._limiter = Limiter(bucket)
            logger.info(
                f"Rate limiter initialized with {len(self._rates)} rate(s): "
                + ", ".join(f"{r.limit}/{r.interval}ms" for r in self._rates)
            )
        else:
            logger.info("No rate limits configured")

    async def resolve_request(self, request: BaseRequest) -> Response:
        """Fetch a BaseRequest with rate limiting and response caching.

        First checks the cache for a matching response. If found, returns
        the cached response without consuming a rate limit token.

        If not cached, acquires a rate limiter token, then makes the request.

        Args:
            request: The BaseRequest to fetch. URL should be absolute.

        Returns:
            Response containing the HTTP response data.
        """
        # Check cache first
        cached = await self._get_cached_response(request)
        if cached is not None:
            logger.debug(f"Cache hit for {request.request.url}")
            return cached

        # Cache miss - acquire rate limiter token if configured
        if self._limiter:
            item = RateItem(
                name="request",
                timestamp=time.time_ns(),
                weight=1,
            )
            await self._limiter.try_acquire_async(item)

        # Make the actual request via parent class
        response = await super().resolve_request(request)

        # Track stats
        self._total_requests += 1
        if 200 <= response.status_code < 300:
            self._total_successes += 1

        return response

    async def _get_cached_response(
        self, request: BaseRequest
    ) -> Response | None:
        """Check cache for a matching response.

        Args:
            request: The request to look up.

        Returns:
            Response if cached, None otherwise.
        """
        # Compute cache key from request parameters
        http_request = request.request

        # Handle body/data - can be bytes, str, dict, etc.
        body: bytes | None = None
        if http_request.data:
            if isinstance(http_request.data, bytes):
                body = http_request.data
            elif isinstance(http_request.data, str):
                body = http_request.data.encode("utf-8")
            else:
                # Dict or other - serialize to JSON
                body = json.dumps(http_request.data, sort_keys=True).encode(
                    "utf-8"
                )
        elif http_request.json is not None:
            body = json.dumps(http_request.json, sort_keys=True).encode(
                "utf-8"
            )

        headers_json = (
            json.dumps(dict(http_request.headers), sort_keys=True)
            if http_request.headers
            else None
        )
        cache_key = compute_cache_key(
            http_request.method.value,
            http_request.url,
            body,
            headers_json,
        )

        # Look up cached response
        cached = await self.sql_manager.get_cached_response(cache_key)
        if cached is None:
            return None

        # Decompress content
        content = b""
        if cached["content_compressed"]:
            dict_id = cached["compression_dict_id"]
            if dict_id is not None:
                dict_data = await self.sql_manager.get_compression_dict(
                    dict_id
                )
                if dict_data:
                    dict_obj = zstd.ZstdCompressionDict(dict_data)
                    decompressor = zstd.ZstdDecompressor(dict_data=dict_obj)
                else:
                    decompressor = zstd.ZstdDecompressor()
            else:
                decompressor = zstd.ZstdDecompressor()
            content = decompressor.decompress(cached["content_compressed"])

        # Parse headers
        headers: dict[str, str] = {}
        if cached["headers_json"]:
            headers = json.loads(cached["headers_json"])

        # Build Response object
        return Response(
            status_code=cached["status_code"],
            headers=headers,
            content=content,
            text=content.decode("utf-8", errors="replace"),
            url=cached["url"],
            request=request,
        )

    @property
    def state(self) -> dict[str, Any]:
        """Get current rate limiter state for monitoring.

        Returns:
            Dictionary with current state information.
        """
        rates_info = []
        if self._rates:
            for r in self._rates:
                rates_info.append(
                    {"limit": r.limit, "interval_ms": r.interval}
                )

        return {
            "rates": rates_info,
            "total_requests": self._total_requests,
            "total_successes": self._total_successes,
            "success_rate": (
                self._total_successes / self._total_requests * 100
                if self._total_requests > 0
                else 100.0
            ),
        }
