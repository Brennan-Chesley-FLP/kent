"""Property-based and regression tests for Playwright driver page identity.

Part 2A: Direct navigation — Hypothesis varies page count and worker count,
         with injected delays at critical await points to force interleaving.
Part 2B: ViaLink navigation — Hypothesis varies tree topology and worker count,
         with many siblings sharing a parent to stress route interception.
Part 3:  Incidental list mutation — patches insert_incidental_request with
         asyncio.sleep(0) to force event-loop cycling during list iteration.

Each test verifies the core invariant: every stored response contains HTML
matching the URL that was actually navigated to for that request.
"""

from __future__ import annotations

import asyncio
import functools
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from hypothesis import given, settings
from hypothesis import strategies as st

from kent.common.decorators import step
from kent.common.lxml_page_element import LxmlPageElement
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    WaitForLoadState,
)
from kent.driver.playwright_driver import PlaywrightDriver
from tests.conftest import AioHttpTestServer, find_free_port

# ---------------------------------------------------------------------------
# Fingerprint mock server
#
# All handlers derive content from URL path params. No post-start mutation.
# Some handlers include artificial delays to widen race windows.
# ---------------------------------------------------------------------------

_BASE_URL_KEY: web.AppKey[str] = web.AppKey("base_url", str)
_TREE_KEY: web.AppKey[dict[str, Any]] = web.AppKey("tree", dict)


def _fingerprint_html(page_id: str, children: list[str], base_url: str) -> str:
    """Generate HTML with a title-embedded fingerprint and optional child links."""
    child_links = "\n".join(
        f'    <a class="child-link" href="{base_url}/fp/{cid}">{cid}</a>'
        for cid in children
    )
    return f"""<!DOCTYPE html>
<html>
<head><title>Page {page_id}</title></head>
<body>
<h1 id="fingerprint">{page_id}</h1>
<div class="links">
{child_links}
</div>
</body>
</html>"""


async def handle_fingerprint_page(request: web.Request) -> web.Response:
    """Serve a fingerprinted page with a small random delay."""
    page_id = request.match_info["page_id"]
    base_url = request.app[_BASE_URL_KEY]
    tree = request.app.get(_TREE_KEY)

    # Delay some responses to create timing variation between workers
    page_num = int(page_id.rsplit("-", 1)[-1]) if "-" in page_id else 0
    if page_num % 3 == 0:
        await asyncio.sleep(0.1)  # Slow every 3rd page

    if tree is not None:
        node = tree.get(page_id)
        children = node.get("children", []) if node else []
    else:
        children = []

    html = _fingerprint_html(page_id, children, base_url)
    return web.Response(text=html, content_type="text/html")


async def handle_heavy_page(request: web.Request) -> web.Response:
    """Serve a page with many sub-resource references (Part 3)."""
    page_id = request.match_info["page_id"]
    base_url = request.app[_BASE_URL_KEY]
    n_resources = 15

    img_tags = "\n".join(
        f'    <img src="{base_url}/resource/{page_id}/{i}" />'
        for i in range(n_resources)
    )
    html = f"""<!DOCTYPE html>
<html>
<head><title>Heavy {page_id}</title></head>
<body>
<h1 id="fingerprint">{page_id}</h1>
{img_tags}
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def handle_resource(request: web.Request) -> web.Response:
    """Serve a sub-resource with a delay to widen the race window."""
    page_id = request.match_info["page_id"]
    resource_id = request.match_info["resource_id"]
    # Stagger delays so responses arrive at different times during iteration
    delay = 0.02 + (int(resource_id) * 0.01)
    await asyncio.sleep(delay)
    return web.Response(
        body=f"resource-{page_id}-{resource_id}".encode(),
        content_type="application/octet-stream",
    )


def _create_fingerprint_app(
    base_url: str,
    tree: dict[str, Any] | None = None,
) -> web.Application:
    """Create a dedicated aiohttp app for fingerprint/page-identity tests."""
    app = web.Application()
    app[_BASE_URL_KEY] = base_url
    if tree is not None:
        app[_TREE_KEY] = tree
    app.router.add_get("/fp/{page_id}", handle_fingerprint_page)
    app.router.add_get("/heavy/{page_id}", handle_heavy_page)
    app.router.add_get("/resource/{page_id}/{resource_id}", handle_resource)
    return app


# ---------------------------------------------------------------------------
# Helper: generate tree for ViaLink tests
# ---------------------------------------------------------------------------


def generate_tree(
    n_pages: int, branching: int
) -> tuple[str, dict[str, dict[str, Any]]]:
    """Generate a BFS tree with up to n_pages nodes.

    Returns (root_id, tree_dict) where tree_dict maps page_id -> {"children": [...]}.
    """
    tree: dict[str, dict[str, Any]] = {}
    root_id = "node-0"
    queue = [root_id]
    count = 1

    tree[root_id] = {"children": []}

    while queue and count < n_pages:
        parent = queue.pop(0)
        n_children = min(branching, n_pages - count)
        children = []
        for _ in range(n_children):
            child_id = f"node-{count}"
            tree[child_id] = {"children": []}
            children.append(child_id)
            queue.append(child_id)
            count += 1
            if count >= n_pages:
                break
        tree[parent]["children"] = children

    return root_id, tree


def generate_wide_tree(
    n_children: int,
) -> tuple[str, dict[str, dict[str, Any]]]:
    """Generate a 2-level tree: 1 root with n_children leaves.

    This stresses the ViaLink path because all children share the same parent
    and thus all go through _setup_tab_with_parent_response concurrently.
    """
    root_id = "root-0"
    children = [f"leaf-{i}" for i in range(n_children)]
    tree: dict[str, dict[str, Any]] = {root_id: {"children": children}}
    for cid in children:
        tree[cid] = {"children": []}
    return root_id, tree


# ---------------------------------------------------------------------------
# Server start helper
# ---------------------------------------------------------------------------


def _start_server(tree: dict[str, Any] | None = None) -> AioHttpTestServer:
    """Start a fingerprint test server with optional tree state baked in."""
    port = find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    app = _create_fingerprint_app(base_url, tree=tree)
    server = AioHttpTestServer(app, port)
    server.start()
    return server


# ---------------------------------------------------------------------------
# Helper: extract page ID from LxmlPageElement
# ---------------------------------------------------------------------------


def _extract_page_id_from_dom(page: LxmlPageElement) -> str | None:
    """Extract the fingerprint page ID from the <h1 id='fingerprint'> element."""
    elems = page.query_xpath(
        "//h1[@id='fingerprint']", "fingerprint heading", min_count=0
    )
    if elems:
        return elems[0].text_content().strip()
    return None


def _expected_id_from_url(url: str) -> str | None:
    """Derive expected page ID from the URL path (last segment)."""
    return url.rstrip("/").split("/")[-1] if url else None


# ---------------------------------------------------------------------------
# Monkey-patches to inject delays at critical await points
# ---------------------------------------------------------------------------


def _patch_store_response_with_yield(driver: PlaywrightDriver) -> None:
    """Wrap _store_response to yield control after storing, forcing interleaving."""
    original = driver._store_response

    @functools.wraps(original)
    async def patched(*args: Any, **kwargs: Any) -> Any:
        result = await original(*args, **kwargs)
        # Force event loop to service other workers between store and next op
        await asyncio.sleep(0)
        return result

    driver._store_response = patched  # type: ignore[method-assign]


def _patch_page_content_with_yield(driver: PlaywrightDriver) -> None:
    """Wrap each worker page's content() to yield control, forcing interleaving.

    Must be called after pages are created (lazy), so we patch
    _acquire_worker_page instead.
    """
    original_acquire = driver._acquire_worker_page

    @functools.wraps(original_acquire)
    async def patched_acquire(worker_id: int) -> Any:
        wp = await original_acquire(worker_id)
        page = wp.page

        # Only patch once per page
        if not getattr(page, "_content_patched", False):
            original_content = page.content

            async def content_with_yield() -> str:
                result = await original_content()
                await asyncio.sleep(0)  # yield to other workers
                return result

            page.content = content_with_yield  # type: ignore[method-assign]
            page._content_patched = True  # type: ignore[attr-defined]

        return wp

    driver._acquire_worker_page = patched_acquire  # type: ignore[method-assign]


# ============================================================================
# Part 2A: Direct Navigation Fingerprint Test
# ============================================================================


class DirectFingerprintScraper(BaseScraper[dict]):
    """Scraper that navigates directly to each page and validates fingerprints."""

    def __init__(self, base_url: str, page_ids: list[str]) -> None:
        super().__init__()
        self.base_url = base_url
        self.page_ids = page_ids
        self.mismatches: list[tuple[str | None, str | None]] = []
        self.validated_count = 0

    def get_entry(self):
        for pid in self.page_ids:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.base_url}/fp/{pid}",
                ),
                continuation=self.parse_page,
            )

    @step(await_list=[WaitForLoadState(state="domcontentloaded")])
    def parse_page(self, page: LxmlPageElement):
        fingerprint = _extract_page_id_from_dom(page)
        expected_id = _expected_id_from_url(page._url)

        if fingerprint != expected_id:
            self.mismatches.append((expected_id, fingerprint))
        else:
            self.validated_count += 1

        yield ParsedData(
            data={"page_id": expected_id, "fingerprint": fingerprint}
        )


class TestDirectFingerprint:
    """Part 2A: Property test — direct URL navigation with forced interleaving."""

    @pytest.mark.generative
    @given(
        n_pages=st.integers(min_value=5, max_value=20),
        n_workers=st.integers(min_value=2, max_value=5),
    )
    @settings(max_examples=25, deadline=None)
    @pytest.mark.asyncio
    async def test_direct_fingerprints_match(
        self, n_pages: int, n_workers: int
    ) -> None:
        """Every response contains the fingerprint matching its navigated URL.

        Injects asyncio.sleep(0) after page.content() and _store_response()
        to force worker interleaving at the critical snapshot→store boundary.
        """
        server = _start_server()
        try:
            page_ids = [f"page-{i}" for i in range(n_pages)]
            scraper = DirectFingerprintScraper(server.url, page_ids)

            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = Path(tmpdir) / "test.db"
                async with PlaywrightDriver.open(
                    scraper,
                    db_path,
                    headless=True,
                    num_workers=n_workers,
                    enable_monitor=False,
                ) as driver:
                    # Inject delays at critical points
                    _patch_store_response_with_yield(driver)
                    _patch_page_content_with_yield(driver)

                    await driver.run(setup_signal_handlers=False)
                    stats = await driver.get_stats()

                assert scraper.mismatches == [], (
                    f"Fingerprint mismatches: {scraper.mismatches}"
                )
                assert scraper.validated_count == n_pages, (
                    f"Expected {n_pages} validations, got {scraper.validated_count}"
                )
                assert stats.queue.failed == 0
        finally:
            server.stop()


# ============================================================================
# Part 2B: ViaLink Navigation Fingerprint Test
# ============================================================================


class ViaLinkFingerprintScraper(BaseScraper[dict]):
    """Scraper that follows links from parent pages, exercising route interception."""

    def __init__(
        self, base_url: str, root_id: str, tree: dict[str, Any]
    ) -> None:
        super().__init__()
        self.base_url = base_url
        self.root_id = root_id
        self.tree = tree
        self.mismatches: list[tuple[str | None, str | None]] = []
        self.validated_count = 0

    def get_entry(self):
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.base_url}/fp/{self.root_id}",
            ),
            continuation=self.parse_node,
        )

    @step(await_list=[WaitForLoadState(state="domcontentloaded")])
    def parse_node(self, page: LxmlPageElement):
        fingerprint = _extract_page_id_from_dom(page)
        expected_id = _expected_id_from_url(page._url)

        if fingerprint != expected_id:
            self.mismatches.append((expected_id, fingerprint))
        else:
            self.validated_count += 1

        yield ParsedData(
            data={"page_id": expected_id, "fingerprint": fingerprint}
        )

        # Follow child links
        node = self.tree.get(expected_id or "", {})
        children = node.get("children", [])
        if children:
            links = page.find_links(
                "//a[@class='child-link']",
                "child links",
                min_count=len(children),
            )
            for link in links:
                req = link.follow()
                yield replace(req, continuation=self.parse_node)


class TestViaLinkFingerprint:
    """Part 2B: Property tests — ViaLink navigation with route interception."""

    @pytest.mark.generative
    @given(
        n_pages=st.integers(min_value=3, max_value=12),
        n_workers=st.integers(min_value=2, max_value=4),
        branching=st.integers(min_value=1, max_value=3),
    )
    @settings(max_examples=15, deadline=None)
    @pytest.mark.asyncio
    async def test_vialink_fingerprints_match(
        self, n_pages: int, n_workers: int, branching: int
    ) -> None:
        """Every response via link.follow() contains the correct fingerprint.

        Injects delays to force interleaving during route interception setup.
        """
        root_id, tree = generate_tree(n_pages, branching)
        server = _start_server(tree=tree)
        try:
            scraper = ViaLinkFingerprintScraper(server.url, root_id, tree)

            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = Path(tmpdir) / "test.db"
                async with PlaywrightDriver.open(
                    scraper,
                    db_path,
                    headless=True,
                    num_workers=n_workers,
                    enable_monitor=False,
                ) as driver:
                    _patch_store_response_with_yield(driver)
                    _patch_page_content_with_yield(driver)

                    await driver.run(setup_signal_handlers=False)
                    stats = await driver.get_stats()

                expected_count = len(tree)
                assert scraper.mismatches == [], (
                    f"Fingerprint mismatches: {scraper.mismatches}"
                )
                assert scraper.validated_count == expected_count, (
                    f"Expected {expected_count} validations, got {scraper.validated_count}"
                )
                assert stats.queue.failed == 0
        finally:
            server.stop()

    @pytest.mark.generative
    @given(
        n_children=st.integers(min_value=4, max_value=10),
        n_workers=st.integers(min_value=2, max_value=4),
    )
    @settings(max_examples=15, deadline=None)
    @pytest.mark.asyncio
    async def test_wide_tree_siblings_share_parent(
        self, n_children: int, n_workers: int
    ) -> None:
        """Many siblings sharing one parent all get correct content.

        This is the highest-risk ViaLink scenario: all children call
        _setup_tab_with_parent_response for the same parent_request_id
        concurrently, each setting up route interception for the same URL.
        """
        root_id, tree = generate_wide_tree(n_children)
        server = _start_server(tree=tree)
        try:
            scraper = ViaLinkFingerprintScraper(server.url, root_id, tree)

            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = Path(tmpdir) / "test.db"
                async with PlaywrightDriver.open(
                    scraper,
                    db_path,
                    headless=True,
                    num_workers=n_workers,
                    enable_monitor=False,
                ) as driver:
                    _patch_store_response_with_yield(driver)
                    _patch_page_content_with_yield(driver)

                    await driver.run(setup_signal_handlers=False)
                    stats = await driver.get_stats()

                expected_count = 1 + n_children  # root + leaves
                assert scraper.mismatches == [], (
                    f"Fingerprint mismatches: {scraper.mismatches}"
                )
                assert scraper.validated_count == expected_count, (
                    f"Expected {expected_count} validations, got {scraper.validated_count}"
                )
                assert stats.queue.failed == 0
        finally:
            server.stop()


# ============================================================================
# Part 3: Incidental List Mutation Regression Test
# ============================================================================


class HeavyResourceScraper(BaseScraper[dict]):
    """Scraper that loads pages with many sub-resources."""

    def __init__(self, base_url: str, n_pages: int) -> None:
        super().__init__()
        self.base_url = base_url
        self.n_pages = n_pages
        self.parsed_ids: list[str] = []

    def get_entry(self):
        for i in range(self.n_pages):
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.base_url}/heavy/heavy-{i}",
                ),
                continuation=self.parse_page,
            )

    @step(await_list=[WaitForLoadState(state="networkidle")])
    def parse_page(self, page: LxmlPageElement):
        fingerprint = _extract_page_id_from_dom(page)
        if fingerprint:
            self.parsed_ids.append(fingerprint)
        yield ParsedData(data={"page_id": fingerprint})


def _patch_incidental_insert_with_yield(driver: PlaywrightDriver) -> None:
    """Wrap insert_incidental_request with asyncio.sleep(0) to force
    event-loop cycling during the incidental list iteration loop.

    This amplifies the race window at playwright_driver.py:871-874 where
    on_response can append to wp.incidental_requests while the for-loop
    is iterating and doing awaits.
    """
    original = driver.db.insert_incidental_request

    @functools.wraps(original)
    async def patched(*args: Any, **kwargs: Any) -> Any:
        # Yield BEFORE the insert to let response events fire
        await asyncio.sleep(0)
        result = await original(*args, **kwargs)
        # Yield AFTER the insert too
        await asyncio.sleep(0)
        return result

    driver.db.insert_incidental_request = patched  # type: ignore[method-assign]


class TestIncidentalListMutation:
    """Part 3: Regression test for concurrent incidental list modification."""

    @pytest.mark.asyncio
    async def test_incidental_requests_not_cross_contaminated(self) -> None:
        """Incidental requests are attributed to the correct parent request.

        Patches insert_incidental_request to yield control during iteration,
        maximizing the chance that on_response fires mid-loop.
        """
        server = _start_server()
        try:
            n_pages = 10
            scraper = HeavyResourceScraper(server.url, n_pages)

            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = Path(tmpdir) / "test.db"
                async with PlaywrightDriver.open(
                    scraper,
                    db_path,
                    headless=True,
                    num_workers=4,
                    enable_monitor=False,
                ) as driver:
                    # Inject yields during incidental iteration
                    _patch_incidental_insert_with_yield(driver)
                    _patch_store_response_with_yield(driver)

                    await driver.run(setup_signal_handlers=False)

                    stats = await driver.get_stats()

                    assert stats.queue.failed == 0, (
                        f"{stats.queue.failed} requests failed"
                    )
                    assert len(scraper.parsed_ids) == n_pages

                # Query DB for incidental requests and verify parentage
                from kent.driver.persistent_driver.sql_manager import (
                    SQLManager,
                )

                async with SQLManager.open(db_path) as db:
                    from sqlalchemy import select

                    from kent.driver.persistent_driver.models import (
                        IncidentalRequest,
                    )
                    from kent.driver.persistent_driver.models import (
                        Request as RequestModel,
                    )

                    async with db._session_factory() as session:
                        result = await session.execute(
                            select(
                                IncidentalRequest.parent_request_id,
                                IncidentalRequest.url,
                                RequestModel.url,
                            ).join(
                                RequestModel,
                                IncidentalRequest.parent_request_id
                                == RequestModel.id,
                            )
                        )
                        rows = result.all()

                    misattributed = []
                    for _parent_req_id, incidental_url, parent_url in rows:
                        # Parent URL is like .../heavy/heavy-3
                        parent_page_id = parent_url.rstrip("/").split("/")[-1]

                        # Incidental URL should reference the same page_id
                        # e.g., .../resource/heavy-3/5
                        if (
                            f"/resource/{parent_page_id}/"
                            not in incidental_url
                            and "about:blank" not in incidental_url
                            and f"/heavy/{parent_page_id}"
                            not in incidental_url
                        ):
                            misattributed.append(
                                (_parent_req_id, parent_url, incidental_url)
                            )

                    assert misattributed == [], (
                        "Incidental requests attributed to wrong parent:\n"
                        + "\n".join(
                            f"  parent={pu} got incidental={iu}"
                            for _, pu, iu in misattributed[:10]
                        )
                    )
        finally:
            server.stop()

    @pytest.mark.asyncio
    async def test_reset_does_not_leak_stale_incidentals(self) -> None:
        """After reset_for_reuse(), no incidentals from the prior request survive.

        Calls the real reset_for_reuse (which clears before and after goto),
        then injects a yield and checks if any stale events leak through.
        """
        server = _start_server()
        try:
            n_pages = 10
            scraper = HeavyResourceScraper(server.url, n_pages)

            stale_events: list[dict[str, Any]] = []

            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = Path(tmpdir) / "test.db"
                async with PlaywrightDriver.open(
                    scraper,
                    db_path,
                    headless=True,
                    num_workers=4,
                    enable_monitor=False,
                ) as driver:
                    # Wrap reset_for_reuse: call the real method, then inject
                    # a yield and probe for any events that leaked through.
                    original_acquire = driver._acquire_worker_page

                    async def patched_acquire(worker_id: int) -> Any:
                        wp = await original_acquire(worker_id)

                        if not getattr(wp, "_reset_patched", False):
                            original_reset = wp.reset_for_reuse

                            async def reset_then_probe() -> None:
                                await original_reset()
                                # Force yield — gives stale callbacks a chance
                                # to fire after reset completed
                                await asyncio.sleep(0)
                                # Anything in the list now is a leak
                                if wp.incidental_requests:
                                    stale_events.extend(
                                        wp.incidental_requests.copy()
                                    )

                            wp.reset_for_reuse = reset_then_probe  # type: ignore[method-assign]
                            wp._reset_patched = True  # type: ignore[attr-defined]

                        return wp

                    driver._acquire_worker_page = patched_acquire  # type: ignore[method-assign]

                    await driver.run(setup_signal_handlers=False)
                    stats = await driver.get_stats()

                    assert stats.queue.failed == 0

                if stale_events:
                    stale_urls = [e.get("url", "???") for e in stale_events]
                    pytest.fail(
                        f"Stale incidental events leaked after reset_for_reuse "
                        f"({len(stale_events)} events):\n"
                        + "\n".join(f"  {u}" for u in stale_urls[:10])
                    )
        finally:
            server.stop()
