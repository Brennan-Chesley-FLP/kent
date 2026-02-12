"""Adaptive Token Bucket (ATB) Rate Limiter.

This module implements an adaptive rate limiter based on the ATB algorithm
from "Rethinking API Rate Limiting: A Client-Side Approach" https://arxiv.org/abs/2510.04516.
It dynamically adjusts the request rate based on server responses:
- On success (2xx): increase rate using multiplicative factors
- On rate limiting (429/5xx): halve the rate and record congestion level

The rate limiter persists its state to SQLite for suspend/resume capability.

Key features:
- Token bucket for burst control
- Adaptive rate based on server feedback
- SQL persistence for state recovery
- Response caching for development efficiency
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import zstandard as zstd

from kent.common.request_manager import (
    AsyncRequestManager,
)
from kent.data_types import BaseRequest, Response
from kent.driver.dev_driver.sql_manager import (
    compute_cache_key,
)

if TYPE_CHECKING:
    from kent.driver.dev_driver.sql_manager import (
        SQLManager,
    )

logger = logging.getLogger(__name__)


@dataclass
class ATBConfig:
    """Configuration for Adaptive Token Bucket rate limiter.

    Attributes:
        bucket_size: Maximum tokens in the bucket. Default: 4.0
        initial_tokens: Starting token count. Default: 1.0
        initial_rate: Initial rate in tokens/second. Default: 0.1 (6 req/min)
        initial_congestion: Initial congestion rate. Default: 1.0
        first_step: Aggressive rate increase multiplier (below congestion). Default: 1.5
        second_step: Conservative rate increase multiplier (above congestion). Default: 1.2
        min_rate: Minimum allowed rate. Default: 0.01
        max_rate: Maximum allowed rate. Default: None (no cap)
    """

    bucket_size: float = 4.0
    initial_tokens: float = 1.0
    initial_rate: float = 0.1
    initial_congestion: float = 1.0
    first_step: float = 1.5
    second_step: float = 1.2
    min_rate: float = 0.01
    max_rate: float = 40.0


class ATBAsyncRequestManager(AsyncRequestManager):
    """Adaptive Token Bucket rate limiter as a request manager.

    This class extends AsyncRequestManager to add ATB rate limiting:
    1. Waits for token availability before making requests
    2. Adjusts rate based on response status code

    The token bucket generates tokens at the current rate. When a request
    needs to be made, the manager waits until a token is available.

    Rate adjustment:
    - Success (2xx): Rate increases using first_step (aggressive) or
      second_step (conservative) based on whether we're below or above
      the last congestion rate.
    - Rate limited (429) or server error (5xx): Rate halves and current
      rate is recorded as the new congestion rate.

    Example:
        config = ATBConfig(initial_rate=0.1)
        manager = ATBAsyncRequestManager(
            config=config,
            sql_manager=sql_manager,
            ssl_context=scraper.get_ssl_context(),
        )
        await manager.initialize()

        # Make rate-limited requests:
        response = await manager.resolve_request(request)
    """

    def __init__(
        self,
        config: ATBConfig,
        sql_manager: SQLManager,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float | None = None,
    ) -> None:
        """Initialize the ATB request manager.

        Args:
            config: ATB configuration parameters.
            sql_manager: SQLManager for database operations.
            ssl_context: Optional SSL context for HTTPS connections.
            timeout: Request timeout in seconds. None means no timeout.
        """
        super().__init__(
            ssl_context=ssl_context,
            timeout=timeout,
        )
        self.config = config
        self.sql_manager = sql_manager

        # In-memory state (loaded from/persisted to DB)
        self._tokens = config.initial_tokens
        self._rate = config.initial_rate
        self._bucket_size = config.bucket_size
        self._last_congestion_rate = config.initial_congestion
        self._last_used = time.time()

        # Lock for thread-safe token operations
        self._lock = asyncio.Lock()

        # Statistics (also persisted)
        self._total_requests = 0
        self._total_successes = 0
        self._total_rate_limited = 0

    async def initialize(self) -> None:
        """Initialize the rate limiter from database or config.

        Loads existing state from database if available, otherwise
        initializes with config defaults and persists to database.
        """
        state = await self.sql_manager.get_rate_limiter_state()

        if state is not None:
            # Restore from database
            self._tokens = state["tokens"]
            self._rate = state["rate"]
            self._bucket_size = state["bucket_size"]
            self._last_congestion_rate = state["last_congestion_rate"]
            self._last_used = state["last_used_at"]
            self._total_requests = state["total_requests"]
            self._total_successes = state["total_successes"]
            self._total_rate_limited = state["total_rate_limited"]

            # Regenerate tokens based on time elapsed since last_used
            elapsed = time.time() - self._last_used
            self._tokens = min(
                self._bucket_size, self._tokens + elapsed * self._rate
            )

            logger.info(
                f"ATB rate limiter restored: rate={self._rate:.4f}/s "
                f"({self._rate * 60:.2f}/min), tokens={self._tokens:.2f}, "
                f"congestion_rate={self._last_congestion_rate:.4f}"
            )
        else:
            # Initialize with config defaults
            self._tokens = self.config.initial_tokens
            self._rate = self.config.initial_rate
            self._bucket_size = self.config.bucket_size
            self._last_congestion_rate = self.config.initial_congestion
            self._last_used = time.time()

            await self._persist_state()

            logger.info(
                f"ATB rate limiter initialized: rate={self._rate:.4f}/s "
                f"({self._rate * 60:.2f}/min), bucket_size={self._bucket_size}"
            )

    async def _persist_state(self) -> None:
        """Persist current state to database."""
        await self.sql_manager.upsert_rate_limiter_state(
            tokens=self._tokens,
            rate=self._rate,
            bucket_size=self._bucket_size,
            last_congestion_rate=self._last_congestion_rate,
            jitter=0.0,  # Kept for DB schema compatibility
            last_used_at=self._last_used,
            total_requests=self._total_requests,
            total_successes=self._total_successes,
            total_rate_limited=self._total_rate_limited,
        )

    async def _acquire_token(self) -> None:
        """Acquire a token from the bucket, waiting if necessary.

        Uses a "next available" time tracking approach to properly
        stagger concurrent workers. Each worker reserves a future
        time slot and waits until that slot arrives.

        This correctly handles multiple workers:
        - Worker A: reserves slot at T=0, no wait
        - Worker B: reserves slot at T=1/rate, waits 1/rate seconds
        - Worker C: reserves slot at T=2/rate, waits 2/rate seconds
        """
        wait_time = 0.0

        async with self._lock:
            now = time.time()

            # Generate tokens based on time elapsed since last update
            elapsed = now - self._last_used
            if elapsed > 0:
                self._tokens = min(
                    self._bucket_size, self._tokens + elapsed * self._rate
                )
                self._last_used = now

            if self._tokens >= 1.0:
                # Token available - consume it immediately
                self._tokens -= 1.0
            else:
                # Need to wait for token generation
                # Calculate time until we'll have a full token
                wait_time = (1.0 - self._tokens) / self._rate

                # Reserve the token now by going negative
                # This ensures the next worker sees the correct state
                self._tokens -= 1.0  # Now negative, indicating debt

        # Wait outside the lock so other workers can calculate their slots
        if wait_time > 0:
            await asyncio.sleep(wait_time)

    def _increase_rate(self) -> float:
        """Increase rate based on success.

        Uses first_step (aggressive) if below congestion rate,
        second_step (conservative) if at or above. Caps at max_rate if configured.

        Returns:
            New rate after increase.
        """
        min_increase = 0.01

        if self._rate < self._last_congestion_rate:
            # Below congestion - aggressive increase
            new_rate = max(
                self._rate + min_increase, self._rate * self.config.first_step
            )
            step = "aggressive"
        else:
            # At or above congestion - conservative increase
            new_rate = max(
                self._rate + min_increase, self._rate * self.config.second_step
            )
            step = "conservative"

        # Apply max_rate cap if configured
        if self.config.max_rate is not None:
            new_rate = min(new_rate, self.config.max_rate)

        old_rate = self._rate
        self._rate = round(new_rate, 4)

        logger.debug(
            f"ATB rate increased ({step}): {old_rate:.4f} -> {self._rate:.4f}/s "
            f"({self._rate * 60:.2f}/min)"
        )

        return self._rate

    def _decrease_rate(self) -> float:
        """Decrease rate upon congestion (429/5xx).

        Halves the rate, records current rate as congestion rate,
        and empties the token bucket.

        Returns:
            New rate after decrease.
        """
        old_rate = self._rate
        self._last_congestion_rate = self._rate
        self._rate = max(self.config.min_rate, round(self._rate / 2.0, 4))
        self._tokens = 0.0

        logger.info(
            f"ATB rate decreased (congestion): {old_rate:.4f} -> {self._rate:.4f}/s "
            f"({self._rate * 60:.2f}/min), congestion_rate={self._last_congestion_rate:.4f}"
        )

        return self._rate

    async def _adjust_rate_for_response(self, response: Response) -> None:
        """Adjust rate based on response status code.

        Args:
            response: The response received.
        """
        status_code = response.status_code

        if 200 <= status_code < 300:
            # Success - increase rate
            self._total_requests += 1
            self._total_successes += 1
            new_rate = self._increase_rate()
            await self.sql_manager.update_rate_limiter_rate_increase(new_rate)

        elif status_code in (429, 408, 425, 500, 502, 503, 504):
            # Rate limited or server error - decrease rate
            self._total_requests += 1
            self._total_rate_limited += 1
            new_rate = self._decrease_rate()
            await self.sql_manager.update_rate_limiter_rate_decrease(
                new_rate, self._last_congestion_rate
            )

        else:
            # Other status codes - track but don't adjust rate
            self._total_requests += 1
            # Update tokens state in DB
            await self.sql_manager.update_rate_limiter_tokens(
                self._tokens, self._last_used
            )

    async def resolve_request(self, request: BaseRequest) -> Response:
        """Fetch a BaseRequest with rate limiting and response caching.

        First checks the cache for a matching response. If found, returns
        the cached response without consuming a rate limit token.

        If not cached, waits for token availability, makes the request,
        and adjusts the rate based on the response.

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

        # Cache miss - wait for rate limiter token
        await self._acquire_token()

        # Make the actual request via parent class
        response = await super().resolve_request(request)

        # Adjust rate based on response
        await self._adjust_rate_for_response(response)

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
        return {
            "tokens": self._tokens,
            "rate": self._rate,
            "bucket_size": self._bucket_size,
            "last_congestion_rate": self._last_congestion_rate,
            "last_used_at": self._last_used,
            "total_requests": self._total_requests,
            "total_successes": self._total_successes,
            "total_rate_limited": self._total_rate_limited,
            "approximate_requests_per_minute": self._rate * 60,
            "success_rate": (
                self._total_successes / self._total_requests * 100
                if self._total_requests > 0
                else 100.0
            ),
            "status": self._compute_status(),
        }

    def _compute_status(self) -> str:
        """Compute human-readable status.

        Returns:
            One of: "healthy", "throttled", "recovering"
        """
        if self._total_rate_limited == 0:
            return "healthy"
        elif self._rate < self._last_congestion_rate:
            return "recovering"
        return "throttled"
