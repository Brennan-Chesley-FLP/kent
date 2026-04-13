"""Tests for the _race_await_lists method on PlaywrightDriver.

These tests verify the async racing logic without a real browser.
A mock Playwright page is used, with wait_for_selector controlled via
asyncio.Event objects to simulate conditions resolving at different times.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from kent.data_types import WaitForSelector
from kent.driver.interstitials import InterstitialHandler, WaitCondition

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _StubHandler(InterstitialHandler):
    """InterstitialHandler whose waitlist uses a controllable selector."""

    def __init__(self, selector: str = "div.interstitial") -> None:
        self._selector = selector
        self.navigate_through_called = False

    def waitlist(self) -> list[WaitCondition]:
        return [WaitForSelector(self._selector)]

    async def navigate_through(self, page: Any) -> None:
        self.navigate_through_called = True


class _DriverStub:
    """Minimal stand-in for PlaywrightDriver with only the methods under test."""

    _race_await_lists: Any
    _process_await_list: Any

    def __init__(self, handlers: list[InterstitialHandler]) -> None:
        self._interstitial_handlers = handlers


def _make_driver_stub(
    handlers: list[InterstitialHandler],
) -> Any:
    """Build a minimal object that has the methods under test.

    Rather than constructing a full PlaywrightDriver (which requires a
    database, browser context, scraper, etc.), we bind the real unbound
    methods onto a lightweight stand-in.
    """
    from kent.driver.playwright_driver.playwright_driver import (
        PlaywrightDriver,
    )

    stub = _DriverStub(handlers)
    stub._race_await_lists = PlaywrightDriver._race_await_lists.__get__(stub)  # type: ignore[attr-defined]
    stub._process_await_list = PlaywrightDriver._process_await_list.__get__(
        stub
    )  # type: ignore[attr-defined]
    return stub


def _make_page(
    events: dict[str, asyncio.Event] | None = None,
) -> AsyncMock:
    """Create a mock Page whose wait_for_selector blocks until signalled.

    Args:
        events: mapping from CSS selector string to an asyncio.Event.
            ``wait_for_selector(sel)`` will block until the corresponding
            event is set.  Selectors not in the dict resolve immediately.
    """
    events = events or {}
    page = AsyncMock()

    async def _wait_for_selector(
        selector: str, /, **_kwargs: Any
    ) -> MagicMock:
        ev = events.get(selector)
        if ev is not None:
            await ev.wait()
        return MagicMock()  # locator-like return

    page.wait_for_selector = AsyncMock(side_effect=_wait_for_selector)
    return page


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRaceAwaitLists:
    """Tests for PlaywrightDriver._race_await_lists."""

    async def test_scraper_wins_returns_none(self) -> None:
        """When the scraper await_list resolves first, return None."""
        scraper_ready = asyncio.Event()
        interstitial_ready = asyncio.Event()

        page = _make_page(
            {
                "#content": scraper_ready,
                "div.interstitial": interstitial_ready,
            }
        )
        handler = _StubHandler("div.interstitial")
        driver = _make_driver_stub([handler])

        scraper_await = [WaitForSelector("#content")]

        # Let scraper resolve immediately, interstitial never resolves
        scraper_ready.set()

        result = await driver._race_await_lists(page, scraper_await)
        assert result is None
        assert not handler.navigate_through_called

    async def test_interstitial_wins_returns_handler(self) -> None:
        """When the interstitial handler resolves first, return it."""
        scraper_ready = asyncio.Event()
        interstitial_ready = asyncio.Event()

        page = _make_page(
            {
                "#content": scraper_ready,
                "div.interstitial": interstitial_ready,
            }
        )
        handler = _StubHandler("div.interstitial")
        driver = _make_driver_stub([handler])

        scraper_await = [WaitForSelector("#content")]

        # Let interstitial resolve immediately, scraper never resolves
        interstitial_ready.set()

        result = await driver._race_await_lists(page, scraper_await)
        assert result is handler

    async def test_loser_tasks_are_cancelled(self) -> None:
        """Pending tasks should be cancelled after the winner resolves."""
        scraper_ready = asyncio.Event()

        page = _make_page(
            {
                "#content": scraper_ready,
                "div.interstitial": asyncio.Event(),  # never set
            }
        )
        handler = _StubHandler("div.interstitial")
        driver = _make_driver_stub([handler])

        scraper_await = [WaitForSelector("#content")]
        scraper_ready.set()

        await driver._race_await_lists(page, scraper_await)

        # If we get here without hanging, the interstitial task was
        # successfully cancelled (it was waiting on an event that
        # would never be set).

    async def test_winner_exception_propagates(self) -> None:
        """If the winning task raises, the exception propagates."""
        from playwright.async_api import (
            TimeoutError as PlaywrightTimeoutError,
        )

        # Gate the interstitial so it never resolves; the scraper
        # selector will raise immediately, making it the "winner".
        page = _make_page({"div.interstitial": asyncio.Event()})

        original_side_effect = page.wait_for_selector.side_effect

        async def _exploding_wait(selector: str, /, **kwargs: Any) -> None:
            if selector == "#content":
                raise PlaywrightTimeoutError("timed out")
            return await original_side_effect(selector, **kwargs)

        page.wait_for_selector = AsyncMock(side_effect=_exploding_wait)

        handler = _StubHandler("div.interstitial")
        driver = _make_driver_stub([handler])

        scraper_await = [WaitForSelector("#content")]

        with pytest.raises(PlaywrightTimeoutError):
            await driver._race_await_lists(page, scraper_await)

    async def test_multiple_interstitial_handlers(self) -> None:
        """With multiple handlers, the first to resolve wins."""
        scraper_ready = asyncio.Event()  # never set
        handler_a_ready = asyncio.Event()
        handler_b_ready = asyncio.Event()

        page = _make_page(
            {
                "#content": scraper_ready,
                "div.captcha": handler_a_ready,
                "div.disclaimer": handler_b_ready,
            }
        )

        handler_a = _StubHandler("div.captcha")
        handler_b = _StubHandler("div.disclaimer")
        driver = _make_driver_stub([handler_a, handler_b])

        scraper_await = [WaitForSelector("#content")]

        # Only handler_b resolves
        handler_b_ready.set()

        result = await driver._race_await_lists(page, scraper_await)
        assert result is handler_b
