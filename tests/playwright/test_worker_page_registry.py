"""Property-based tests for PlaywrightDriver worker-page registry invariants.

Tests that _acquire_worker_page / _release_worker_page maintain correct 1:1
mapping between worker IDs and Playwright pages, regardless of operation order.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

# ---------------------------------------------------------------------------
# Fakes — lightweight stand-ins for Playwright Page and BrowserContext
# ---------------------------------------------------------------------------


class FakePage:
    """Minimal Page stand-in with a unique identity and closeable state."""

    def __init__(self) -> None:
        self.id = uuid.uuid4()
        self._closed = False

    def is_closed(self) -> bool:
        return self._closed

    async def close(self) -> None:
        self._closed = True

    async def goto(self, url: str, **kwargs: Any) -> None:
        pass

    def on(self, event: str, callback: Any) -> None:
        pass


class FakeBrowserContext:
    """Minimal BrowserContext that produces FakePages."""

    def __init__(self) -> None:
        self.pages_created: list[FakePage] = []

    async def new_page(self) -> FakePage:
        page = FakePage()
        self.pages_created.append(page)
        return page


# ---------------------------------------------------------------------------
# Helper — build a minimal PlaywrightDriver with only the registry wired up
# ---------------------------------------------------------------------------


def _make_driver_with_fake_context() -> tuple[Any, FakeBrowserContext]:
    """Create a PlaywrightDriver instance with a FakeBrowserContext.

    Bypasses __init__ entirely and sets only the attributes needed by
    _acquire_worker_page / _release_worker_page.
    """
    from kent.driver.playwright_driver.playwright_driver import (
        PlaywrightDriver,
    )

    # Create an uninitialised instance (skip __init__)
    driver = object.__new__(PlaywrightDriver)
    ctx = FakeBrowserContext()
    driver.browser_context = ctx
    driver._worker_pages = {}
    driver.excluded_resource_types = {"image", "media", "font"}
    driver._browser_restart_lock = asyncio.Lock()
    driver._browser_launcher = None
    return driver, ctx


# ---------------------------------------------------------------------------
# Property-based test
# ---------------------------------------------------------------------------


@pytest.mark.generative
@given(data=st.data())
@settings(max_examples=200, deadline=None)
@pytest.mark.asyncio
async def test_worker_page_registry_invariants(data: st.DataObject) -> None:
    """Random sequences of acquire / release / close never violate registry invariants.

    Properties:
      P1: acquire(wid) returns the same WorkerPage for a given wid (until released)
      P2: two distinct wids never share the same underlying page object
      P3: after release(wid), re-acquiring creates a fresh page
      P4: if a page is closed, next acquire creates a new page
    """
    driver, ctx = _make_driver_with_fake_context()

    # Model: tracks what we *expect* the registry to look like
    model: dict[int, uuid.UUID] = {}  # worker_id -> expected page id
    released_page_ids: set[uuid.UUID] = set()

    n_ops = data.draw(st.integers(min_value=10, max_value=80), label="n_ops")

    for _ in range(n_ops):
        op = data.draw(
            st.sampled_from(["acquire", "release", "close_page"]),
            label="op",
        )
        wid = data.draw(st.integers(min_value=0, max_value=7), label="wid")

        if op == "acquire":
            wp = await driver._acquire_worker_page(wid)

            if wid in model:
                # P1: same worker gets same page (unless it was closed)
                assert wp.page.id == model[wid], (
                    f"P1 violated: worker {wid} got page {wp.page.id}, "
                    f"expected {model[wid]}"
                )
            else:
                # New page — record it
                model[wid] = wp.page.id

            # P2: no two active workers share a page
            active_pages: dict[uuid.UUID, int] = {}
            for w, pid in model.items():
                assert pid not in active_pages or active_pages[pid] == w, (
                    f"P2 violated: workers {active_pages.get(pid)} and {w} "
                    f"share page {pid}"
                )
                active_pages[pid] = w

            # The page should not be one that was previously released
            # (i.e., it should be a fresh allocation)
            assert wp.page.id not in released_page_ids, (
                f"Reused a released page {wp.page.id} for worker {wid}"
            )

        elif op == "release":
            if wid in model:
                released_page_ids.add(model[wid])
                del model[wid]
            await driver._release_worker_page(wid)

            # P3: after release, registry should not contain this worker
            assert wid not in driver._worker_pages, (
                f"P3 violated: worker {wid} still in registry after release"
            )

        elif op == "close_page":
            # Simulate a browser-side page close (crash, OOM, etc.)
            wp = driver._worker_pages.get(wid)
            if wp is not None:
                await wp.page.close()
                old_id = wp.page.id

                # P4: next acquire should detect closed page and create new
                new_wp = await driver._acquire_worker_page(wid)
                assert new_wp.page.id != old_id, (
                    f"P4 violated: worker {wid} got same closed page {old_id}"
                )
                assert not new_wp.page.is_closed(), (
                    f"P4 violated: worker {wid} got a closed page"
                )
                # Update model
                if wid in model:
                    released_page_ids.add(model[wid])
                model[wid] = new_wp.page.id

    # Final invariant: every page in the registry is open
    for wid, wp in driver._worker_pages.items():
        assert not wp.page.is_closed(), (
            f"Final check: worker {wid} has a closed page"
        )


@pytest.mark.generative
@given(
    worker_ids=st.lists(
        st.integers(min_value=0, max_value=15), min_size=2, max_size=10
    )
)
@settings(max_examples=100, deadline=None)
@pytest.mark.asyncio
async def test_concurrent_acquires_never_share_pages(
    worker_ids: list[int],
) -> None:
    """Multiple workers acquiring pages concurrently never get the same page."""
    driver, ctx = _make_driver_with_fake_context()

    # Acquire pages for all unique worker IDs
    unique_wids = list(set(worker_ids))
    worker_pages = {}
    for wid in unique_wids:
        wp = await driver._acquire_worker_page(wid)
        worker_pages[wid] = wp

    # Verify all pages are distinct objects
    page_ids = [wp.page.id for wp in worker_pages.values()]
    assert len(page_ids) == len(set(page_ids)), (
        f"Duplicate pages found: {page_ids}"
    )

    # Verify context created exactly the right number of pages
    assert len(ctx.pages_created) == len(unique_wids)


@pytest.mark.asyncio
async def test_release_then_reacquire_gives_fresh_page() -> None:
    """Releasing a worker page and re-acquiring gives a brand new page."""
    driver, ctx = _make_driver_with_fake_context()

    wp1 = await driver._acquire_worker_page(0)
    page_id_1 = wp1.page.id

    await driver._release_worker_page(0)

    wp2 = await driver._acquire_worker_page(0)
    page_id_2 = wp2.page.id

    assert page_id_1 != page_id_2
    assert len(ctx.pages_created) == 2


# ---------------------------------------------------------------------------
# Browser crash recovery tests
# ---------------------------------------------------------------------------


class DeadBrowserContext:
    """Simulates a browser context whose connection has died."""

    def __init__(self) -> None:
        self.pages_created: list[FakePage] = []

    async def new_page(self) -> FakePage:
        from playwright.async_api import Error as PlaywrightError

        raise PlaywrightError("Connection closed")


class RevivableBrowserContext:
    """Starts dead, becomes alive after restart() is called."""

    def __init__(self) -> None:
        self.alive = False
        self.pages_created: list[FakePage] = []

    async def new_page(self) -> FakePage:
        if not self.alive:
            from playwright.async_api import Error as PlaywrightError

            raise PlaywrightError("Connection closed")
        page = FakePage()
        self.pages_created.append(page)
        return page

    async def close(self) -> None:
        pass


class FakeBrowser:
    """Minimal Browser stand-in for restart tests."""

    def __init__(self, context: Any) -> None:
        self._context = context

    async def new_context(self, **kwargs: Any) -> Any:
        self._context.alive = True
        return self._context

    async def close(self) -> None:
        pass


class FakeLauncher:
    """Mimics playwright.firefox / playwright.chromium."""

    def __init__(self, browser: FakeBrowser) -> None:
        self._browser = browser

    async def launch(self, **kwargs: Any) -> FakeBrowser:
        return self._browser


def _make_driver_with_dead_browser() -> tuple[Any, RevivableBrowserContext]:
    """Create a PlaywrightDriver whose browser connection is dead,
    but can be restarted via _restart_browser_context."""
    from kent.driver.playwright_driver.playwright_driver import (
        PlaywrightDriver,
    )

    ctx = RevivableBrowserContext()
    browser = FakeBrowser(ctx)
    launcher = FakeLauncher(browser)

    driver = object.__new__(PlaywrightDriver)
    driver.browser_context = ctx
    driver._worker_pages = {}
    driver.excluded_resource_types = {"image", "media", "font"}
    driver._browser_restart_lock = asyncio.Lock()
    driver._playwright = None
    driver._browser_obj = browser
    driver._browser_launcher = launcher
    driver._launch_kwargs = {}
    driver._context_kwargs = {}
    driver._browser_profile = None

    return driver, ctx


@pytest.mark.asyncio
async def test_acquire_restarts_browser_on_connection_closed() -> None:
    """When new_page() raises 'Connection closed', the driver restarts
    the browser and retries, returning a valid page."""
    driver, ctx = _make_driver_with_dead_browser()

    wp = await driver._acquire_worker_page(0)
    assert not wp.page.is_closed()
    assert ctx.alive
    assert len(ctx.pages_created) == 1


@pytest.mark.asyncio
async def test_acquire_clears_stale_worker_pages_on_restart() -> None:
    """Browser restart clears all stale worker pages from the registry."""
    from kent.driver.playwright_driver.playwright_driver import WorkerPage

    driver, ctx = _make_driver_with_dead_browser()

    # Manually seed stale worker pages (as if they existed before crash)
    stale_page = FakePage()
    stale_page._closed = True
    driver._worker_pages[1] = WorkerPage(
        stale_page, driver.excluded_resource_types
    )
    driver._worker_pages[2] = WorkerPage(
        stale_page, driver.excluded_resource_types
    )

    # Acquiring worker 0 triggers restart, which should clear all pages
    wp = await driver._acquire_worker_page(0)
    assert not wp.page.is_closed()
    # Stale pages for workers 1 and 2 should be gone
    assert 1 not in driver._worker_pages
    assert 2 not in driver._worker_pages


@pytest.mark.asyncio
async def test_is_connection_dead_detects_known_messages() -> None:
    """_is_connection_dead recognises the error strings from Playwright crashes."""
    from playwright.async_api import Error as PlaywrightError

    driver, _ = _make_driver_with_dead_browser()

    assert driver._is_connection_dead(
        PlaywrightError("Connection closed while reading from the driver")
    )
    assert driver._is_connection_dead(
        PlaywrightError("Browser has been closed")
    )
    assert driver._is_connection_dead(
        PlaywrightError("Target page, context or browser has been closed")
    )
    assert not driver._is_connection_dead(PlaywrightError("NS_ERROR_ABORT"))
    assert not driver._is_connection_dead(
        PlaywrightError("Navigation timeout of 30000ms exceeded")
    )
