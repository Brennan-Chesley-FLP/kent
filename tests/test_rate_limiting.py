"""Tests that rate limits declared on scrapers are respected by all drivers.

Each test creates a simple scraper with a known rate limit, sends multiple
requests through the driver against a local aiohttp server, and asserts
that the elapsed wall-clock time is consistent with the declared rate.
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from pyrate_limiter import Duration, Rate, RateItem

from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
)
from kent.driver.persistent_driver.database import init_database
from kent.driver.persistent_driver.rate_limiter import AioSQLiteBucket
from tests.utils import collect_results, collect_results_async

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NUM_REQUESTS = 4
# 1 request per second → 4 requests needs ≥ 3 s
# (first fires immediately, then 1 s gap before each subsequent request)
RATE = Rate(1, Duration.SECOND)
MINIMUM_SECONDS = NUM_REQUESTS - 1  # 3.0 s


def _make_scraper_class(
    server_url: str,
    rate: Rate = RATE,
    n_requests: int = NUM_REQUESTS,
):
    """Build a minimal scraper class that emits *n_requests* GETs."""

    class RateLimitScraper(BaseScraper[dict]):
        rate_limits = [rate]

        def get_entry(self) -> Generator[Request, None, None]:
            for i in range(n_requests):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test?i={i}",
                    ),
                    continuation="parse",
                )

        def parse(self, response: Response):
            yield ParsedData(data={"url": response.url})

    return RateLimitScraper


# ---------------------------------------------------------------------------
# SyncDriver
# ---------------------------------------------------------------------------


class TestSyncDriverRateLimiting:
    def test_rate_limit_respected(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """SyncDriver should throttle requests per scraper.rate_limits."""
        from kent.driver.sync_driver import SyncDriver

        Scraper = _make_scraper_class(server_url)
        scraper = Scraper()
        callback, results = collect_results()

        start = time.monotonic()
        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )
        driver.run()
        elapsed = time.monotonic() - start

        assert len(results) == NUM_REQUESTS
        assert elapsed >= MINIMUM_SECONDS, (
            f"SyncDriver completed {NUM_REQUESTS} requests in {elapsed:.2f}s, "
            f"expected >= {MINIMUM_SECONDS:.1f}s with rate limit {RATE}"
        )


# ---------------------------------------------------------------------------
# AsyncDriver
# ---------------------------------------------------------------------------


class TestAsyncDriverRateLimiting:
    async def test_rate_limit_respected(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """AsyncDriver should throttle requests per scraper.rate_limits."""
        from kent.driver.async_driver import AsyncDriver

        Scraper = _make_scraper_class(server_url)
        scraper = Scraper()
        callback, results = collect_results_async()

        start = time.monotonic()
        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            num_workers=1,
        )
        await driver.run()
        elapsed = time.monotonic() - start

        assert len(results) == NUM_REQUESTS
        assert elapsed >= MINIMUM_SECONDS, (
            f"AsyncDriver completed {NUM_REQUESTS} requests in {elapsed:.2f}s, "
            f"expected >= {MINIMUM_SECONDS:.1f}s with rate limit {RATE}"
        )


# ---------------------------------------------------------------------------
# PersistentDriver
# ---------------------------------------------------------------------------


class TestPersistentDriverRateLimiting:
    async def test_rate_limit_respected(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """PersistentDriver should throttle requests per scraper.rate_limits."""
        from kent.driver.persistent_driver import PersistentDriver

        Scraper = _make_scraper_class(server_url)
        scraper = Scraper()
        callback, results = collect_results_async()

        db_path = tmp_path / "rate_limit_test.db"

        start = time.monotonic()
        async with PersistentDriver.open(
            scraper,
            db_path,
            num_workers=1,
            resume=False,
            enable_monitor=False,
        ) as driver:
            driver.on_data = callback
            await driver.run()
        elapsed = time.monotonic() - start

        assert len(results) == NUM_REQUESTS
        assert elapsed >= MINIMUM_SECONDS, (
            f"PersistentDriver completed {NUM_REQUESTS} requests in {elapsed:.2f}s, "
            f"expected >= {MINIMUM_SECONDS:.1f}s with rate limit {RATE}"
        )


# ---------------------------------------------------------------------------
# PlaywrightDriver
# ---------------------------------------------------------------------------


class TestPlaywrightDriverRateLimiting:
    @pytest.mark.slow
    async def test_rate_limit_respected(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """PlaywrightDriver should throttle navigations per scraper.rate_limits."""
        pw = pytest.importorskip("playwright")  # noqa: F841
        from kent.driver.playwright_driver import PlaywrightDriver

        Scraper = _make_scraper_class(server_url)
        scraper = Scraper()
        callback, results = collect_results_async()

        db_path = tmp_path / "rate_limit_pw_test.db"

        start = time.monotonic()
        async with PlaywrightDriver.open(
            scraper,
            db_path,
            num_workers=1,
            resume=False,
            enable_monitor=False,
            headless=True,
        ) as driver:
            driver.on_data = callback
            await driver.run()
        elapsed = time.monotonic() - start

        assert len(results) == NUM_REQUESTS
        assert elapsed >= MINIMUM_SECONDS, (
            f"PlaywrightDriver completed {NUM_REQUESTS} requests in {elapsed:.2f}s, "
            f"expected >= {MINIMUM_SECONDS:.1f}s with rate limit {RATE}"
        )


# ---------------------------------------------------------------------------
# AioSQLiteBucket unit tests
# ---------------------------------------------------------------------------


class TestAioSQLiteBucketPut:
    """Tests for AioSQLiteBucket.put() rate-limit enforcement.

    These verify that put() returns False (and sets failing_rate) when
    the bucket is at capacity, which is the mechanism pyrate_limiter
    relies on to trigger delay logic.
    """

    @pytest.fixture
    async def bucket(self, tmp_path: Path) -> AsyncGenerator[AioSQLiteBucket]:
        db_path = tmp_path / "bucket_test.db"
        engine, session_factory = await init_database(db_path)
        # 2 requests per second
        rates = [Rate(2, Duration.SECOND)]
        bucket = AioSQLiteBucket(session_factory, rates)
        yield bucket  # type: ignore[misc]
        await engine.dispose()

    async def test_put_accepts_items_within_limit(
        self, bucket: AioSQLiteBucket
    ) -> None:
        """put() returns True while under the rate limit."""
        now = int(time.time() * 1000)
        assert await bucket.put(RateItem("r", now, 1)) is True
        assert await bucket.put(RateItem("r", now, 1)) is True
        assert bucket.failing_rate is None

    async def test_put_rejects_when_limit_exceeded(
        self, bucket: AioSQLiteBucket
    ) -> None:
        """put() returns False once the rate limit is reached."""
        now = int(time.time() * 1000)
        # Fill the bucket (limit=2)
        await bucket.put(RateItem("r", now, 1))
        await bucket.put(RateItem("r", now, 1))
        # Third item should be rejected
        result = await bucket.put(RateItem("r", now, 1))
        assert result is False

    async def test_put_sets_failing_rate(
        self, bucket: AioSQLiteBucket
    ) -> None:
        """put() sets failing_rate to the exceeded Rate object."""
        now = int(time.time() * 1000)
        await bucket.put(RateItem("r", now, 1))
        await bucket.put(RateItem("r", now, 1))
        await bucket.put(RateItem("r", now, 1))
        assert bucket.failing_rate is not None
        assert bucket.failing_rate.limit == 2

    async def test_put_accepts_after_window_expires(
        self, bucket: AioSQLiteBucket
    ) -> None:
        """put() accepts items again once previous items leave the window."""
        now = int(time.time() * 1000)
        await bucket.put(RateItem("r", now, 1))
        await bucket.put(RateItem("r", now, 1))
        # Rejected within the same window
        assert await bucket.put(RateItem("r", now, 1)) is False
        # 1001 ms later (outside the 1-second window), should be accepted
        assert await bucket.put(RateItem("r", now + 1001, 1)) is True
