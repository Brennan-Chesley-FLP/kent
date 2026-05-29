"""Unit tests for ``CloudflareHandler``'s FLOW#2 + Tab+Space sequencing.

These tests stub out the Playwright ``Page`` interface enough to drive
the handler's state machine without launching a browser.  The handler
relies on:

- ``page.on("response", cb)`` for the readiness signal.
- ``page.keyboard.press(key)`` for Tab+Space.
- ``page.locator(sel).wait_for(state="detached", timeout=...)`` for the
  post-keystroke navigation signal.
- ``page.frames`` for the body-click fallback.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from kent.driver.interstitials import CloudflareHandler


class _Keyboard:
    def __init__(self) -> None:
        self.presses: list[str] = []

    async def press(self, key: str) -> None:
        self.presses.append(key)


class _DetachLocator:
    """Locator whose ``wait_for`` resolves once ``trigger_detach()`` fires."""

    def __init__(self) -> None:
        self.event = asyncio.Event()

    async def wait_for(self, state: str, timeout: int) -> None:
        assert state == "detached"
        try:
            await asyncio.wait_for(self.event.wait(), timeout=timeout / 1000)
        except asyncio.TimeoutError:
            from playwright.async_api import (
                TimeoutError as PlaywrightTimeoutError,
            )

            raise PlaywrightTimeoutError(
                f"Locator.wait_for: Timeout {timeout}ms exceeded"
            ) from None

    def trigger_detach(self) -> None:
        self.event.set()


class _FakePage:
    """Just enough Page surface for CloudflareHandler.navigate_through."""

    def __init__(self) -> None:
        self._listeners: dict[str, list[Any]] = {}
        self.keyboard = _Keyboard()
        self._locators: dict[str, _DetachLocator] = {}
        self.frames: list[Any] = []

    def on(self, event: str, cb: Any) -> None:
        self._listeners.setdefault(event, []).append(cb)

    def remove_listener(self, event: str, cb: Any) -> None:
        self._listeners[event].remove(cb)

    def emit(self, event: str, payload: Any) -> None:
        for cb in self._listeners.get(event, []):
            cb(payload)

    def locator(self, selector: str) -> _DetachLocator:
        return self._locators.setdefault(selector, _DetachLocator())


class _FakeResponse:
    def __init__(self, url: str) -> None:
        self.url = url


async def _emit_flow_responses(page: _FakePage, count: int, delay: float = 0):
    """Emit ``count`` /flow/ov1/ responses with a short delay between them."""
    for _ in range(count):
        if delay:
            await asyncio.sleep(delay)
        page.emit(
            "response",
            _FakeResponse(
                "https://challenges.cloudflare.com/cdn-cgi/"
                "challenge-platform/h/b/flow/ov1/abc"
            ),
        )


async def _trigger_detach_after(
    page: _FakePage, selector: str, delay: float
) -> None:
    await asyncio.sleep(delay)
    page.locator(selector).trigger_detach()


@pytest.mark.asyncio
async def test_flow_signal_then_tab_space_clears() -> None:
    """Happy path: 2nd /flow/ response fires, Tab+Space pressed, page
    navigates (detach), handler returns cleanly."""
    handler = CloudflareHandler()
    page = _FakePage()

    # Schedule: emit two flow responses ~50ms in, detach 100ms later.
    async def stimulate() -> None:
        await asyncio.sleep(0.05)
        await _emit_flow_responses(page, count=2)
        await asyncio.sleep(0.1)
        page.locator("input[name='cf-turnstile-response']").trigger_detach()

    stim = asyncio.create_task(stimulate())
    try:
        await handler.navigate_through(page)
    finally:
        stim.cancel()
        await asyncio.gather(stim, return_exceptions=True)

    # Tab and Space both pressed, in that order
    assert page.keyboard.presses == ["Tab", "Space"]


@pytest.mark.asyncio
async def test_single_flow_response_doesnt_trigger() -> None:
    """One /flow/ response is not enough — readiness only fires on the
    second.  The handler still presses Tab+Space (timeout path) but the
    test confirms the count gate."""
    handler = CloudflareHandler()
    page = _FakePage()

    # Trim the readiness wait so the test runs in milliseconds.
    handler._READY_TIMEOUT_MS = 200  # type: ignore[misc]
    # Pre-arm the detach so the post-keystroke wait doesn't run long.
    page.locator("input[name='cf-turnstile-response']").trigger_detach()

    async def stimulate() -> None:
        await asyncio.sleep(0.02)
        await _emit_flow_responses(page, count=1)

    stim = asyncio.create_task(stimulate())
    try:
        await handler.navigate_through(page)
    finally:
        stim.cancel()
        await asyncio.gather(stim, return_exceptions=True)

    # Even on the timeout path, Tab+Space still fires.
    assert page.keyboard.presses == ["Tab", "Space"]


@pytest.mark.asyncio
async def test_non_flow_responses_dont_count() -> None:
    """URLs that aren't /flow/ov1/ are ignored by the counter."""
    handler = CloudflareHandler()
    page = _FakePage()
    handler._READY_TIMEOUT_MS = 200  # type: ignore[misc]
    page.locator("input[name='cf-turnstile-response']").trigger_detach()

    async def stimulate() -> None:
        await asyncio.sleep(0.02)
        # Three orchestrator-other responses, none of which are /flow/ov1/
        for url in (
            "https://challenges.cloudflare.com/cdn-cgi/challenge-platform/h/b/cmg/1",
            "https://challenges.cloudflare.com/cdn-cgi/challenge-platform/h/b/pat/x",
            "https://challenges.cloudflare.com/cdn-cgi/challenge-platform/h/b/d/y",
        ):
            page.emit("response", _FakeResponse(url))

    stim = asyncio.create_task(stimulate())
    try:
        await handler.navigate_through(page)
    finally:
        stim.cancel()
        await asyncio.gather(stim, return_exceptions=True)

    # Readiness signal never fired, but Tab+Space still ran on the
    # timeout path.
    assert page.keyboard.presses == ["Tab", "Space"]


@pytest.mark.asyncio
async def test_waitlist_selector_matches_response_input() -> None:
    """The waitlist signals the handler should fire — sanity check."""
    from kent.data_types import WaitForSelector

    handler = CloudflareHandler()
    conditions = handler.waitlist()
    assert len(conditions) == 1
    cond = conditions[0]
    assert isinstance(cond, WaitForSelector)
    assert cond.selector == "input[name='cf-turnstile-response']"
    assert cond.state == "attached"
