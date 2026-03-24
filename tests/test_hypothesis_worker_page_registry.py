"""Property-based tests for PlaywrightDriver worker-page registry invariants.

Tests that _acquire_worker_page / _release_worker_page maintain correct 1:1
mapping between worker IDs and Playwright pages, regardless of operation order.
"""

from __future__ import annotations

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
    return driver, ctx


# ---------------------------------------------------------------------------
# Property-based test
# ---------------------------------------------------------------------------


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
