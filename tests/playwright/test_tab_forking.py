"""Integration tests for Playwright tab forking (per-request pages with route interception)."""

import tempfile
from dataclasses import replace
from pathlib import Path

import pytest

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
from tests.conftest import AioHttpTestServer
from tests.mock_server import same_url_search_count_key


class SessionTreeScraper(BaseScraper[None]):
    """Scraper that navigates a 3-level tree with session cookie validation.

    Root → 2 branches → 2 leaves each = 4 leaf results.
    Each level validates that the cookie set at root matches the query param.
    Uses link.follow() to produce ViaLink requests, exercising route interception.
    """

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.parsed_numbers: list[int] = []

    def get_entry(self):
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.base_url}/session-tree",
            ),
            continuation=self.parse_root,
        )

    @step(await_list=[WaitForLoadState(state="networkidle")])
    def parse_root(self, page: LxmlPageElement):
        links = page.find_links(
            "//a[@data-needs-secret]", "branch links", min_count=2
        )
        for link in links:
            req = link.follow()
            yield replace(req, continuation=self.parse_branch)

    @step(await_list=[WaitForLoadState(state="networkidle")])
    def parse_branch(self, page: LxmlPageElement):
        links = page.find_links(
            "//a[@data-needs-secret]", "leaf links", min_count=2
        )
        for link in links:
            req = link.follow()
            yield replace(req, continuation=self.parse_leaf)

    @step(await_list=[WaitForLoadState(state="networkidle")])
    def parse_leaf(self, page: LxmlPageElement):
        number_els = page.query_xpath(
            "//div[@class='number']", "number div", min_count=1
        )
        number = int(number_els[0].text_content().strip())
        self.parsed_numbers.append(number)
        yield ParsedData(data={"number": number})


class TestPlaywrightTabForking:
    """Tests for per-request tab forking with route interception."""

    @pytest.mark.asyncio
    async def test_session_tree_full_scrape(
        self, bug_court_server: AioHttpTestServer
    ):
        """Run the 3-level session tree scraper end-to-end.

        Verifies that:
        - Each request gets its own tab (no shared page state)
        - Cookies are shared across tabs (browser context)
        - All 4 leaf numbers are collected without 404 errors
        - ViaLink route interception serves cached parent pages
        """
        scraper = SessionTreeScraper(bug_court_server.url)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                stats = await driver.get_stats()
                # 4 leaf pages should yield 4 ParsedData results
                assert stats.results.total == 4
                assert sorted(scraper.parsed_numbers) == [1, 2, 3, 4]

                # Verify no failed requests
                assert stats.queue.failed == 0

    @pytest.mark.asyncio
    async def test_session_tree_resume_with_cookies(
        self, bug_court_server: AioHttpTestServer
    ):
        """Run session tree, scrub leaf results, resume and verify cookies persist.

        Steps:
        1. Run full scrape (collects 4 results)
        2. Scrub leaf request responses + results (reset to pending)
        3. Resume — cookies should be restored from DB
        4. Leaves re-run and collect 4 results again
        """
        scraper1 = SessionTreeScraper(bug_court_server.url)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # --- First run: full scrape ---
            async with PlaywrightDriver.open(
                scraper1,
                db_path,
                headless=True,
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)
                stats = await driver.get_stats()
                assert stats.results.total == 4

            # --- Scrub leaf requests: reset them to pending ---
            from kent.driver.persistent_driver.sql_manager import SQLManager

            async with SQLManager.open(db_path) as db:
                from sqlalchemy import delete, select, update

                from kent.driver.persistent_driver.models import (
                    Request as RequestModel,
                )
                from kent.driver.persistent_driver.models import (
                    Result,
                )

                async with db._session_factory() as session:
                    # Find leaf requests (parse_leaf continuation)
                    leaf_result = await session.execute(
                        select(RequestModel.id).where(
                            RequestModel.continuation == "parse_leaf"
                        )
                    )
                    leaf_ids = [r[0] for r in leaf_result.all()]
                    assert len(leaf_ids) == 4

                    # Delete their results
                    await session.execute(
                        delete(Result).where(
                            Result.request_id.in_(leaf_ids)  # type: ignore[union-attr]
                        )
                    )

                    # Reset leaf requests to pending with cleared response
                    await session.execute(
                        update(RequestModel)
                        .where(RequestModel.id.in_(leaf_ids))  # type: ignore[union-attr]
                        .values(
                            status="pending",
                            response_status_code=None,
                            response_headers_json=None,
                            response_url=None,
                            content_compressed=None,
                            content_size_original=None,
                            content_size_compressed=None,
                            compression_dict_id=None,
                            completed_at=None,
                            completed_at_ns=None,
                        )
                    )
                    await session.commit()

            # --- Second run: resume ---
            scraper2 = SessionTreeScraper(bug_court_server.url)

            async with PlaywrightDriver.open(
                scraper2,
                db_path,
                headless=True,
                enable_monitor=False,
                resume=True,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                stats = await driver.get_stats()
                # Should have 4 new results (from re-processed leaves)
                # plus the original 4 results from branches/root
                # Actually: we deleted the 4 leaf results, so total = 4 new leaf results
                assert stats.results.total == 4
                assert sorted(scraper2.parsed_numbers) == [1, 2, 3, 4]
                assert stats.queue.failed == 0


class SameUrlSearchScraper(BaseScraper[None]):
    """Scraper where the form POSTs to the same URL it was loaded from.

    Tests that route interception is properly removed (unrouted) after
    serving the cached parent page, so the form POST hits the real server
    instead of being intercepted and served the cached form HTML again.
    """

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.answer: int | None = None

    def get_entry(self):
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.base_url}/same-url-search",
            ),
            continuation=self.parse_form,
        )

    @step
    def parse_form(self, page: LxmlPageElement):
        form = page.find_form("#search-form", "search form")
        request = form.submit()
        yield replace(request, continuation=self.parse_results)

    @step
    def parse_results(self, page: LxmlPageElement):
        els = page.query_css(".answer", "answer div", min_count=1)
        self.answer = int(els[0].text_content().strip())
        yield ParsedData(data={"answer": self.answer})


class TestPlaywrightUnroute:
    """Tests that route interception is properly cleaned up."""

    @pytest.mark.asyncio
    async def test_same_url_post_not_intercepted(
        self, bug_court_server: AioHttpTestServer
    ):
        """Form POST to the same URL as the cached GET must hit the real server.

        The parent (GET /same-url-search) is cached and served via route
        interception. The child (POST /same-url-search via form submit)
        must NOT be intercepted — it must reach the real server and get
        the results page with "42", not the cached form page.
        """
        scraper = SameUrlSearchScraper(bug_court_server.url)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                stats = await driver.get_stats()
                assert stats.results.total == 1
                assert scraper.answer == 42
                assert stats.queue.failed == 0

                # Server should have received exactly 2 requests:
                # 1 GET (entry point) + 1 POST (form submit).
                # If route interception leaked, the POST would have been
                # served from cache and this count would be 1.
                assert bug_court_server.app[same_url_search_count_key][0] == 2
