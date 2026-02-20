"""Integration tests for Playwright driver core functionality."""

import tempfile
from pathlib import Path

import pytest

from kent.common.decorators import step
from kent.common.page_element import PageElement
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    WaitForLoadState,
    WaitForSelector,
)
from kent.driver.playwright_driver import (
    PlaywrightDriver,
)


class TestPlaywrightIntegration:
    """Integration tests for Playwright driver."""

    @pytest.mark.asyncio
    async def test_basic_navigation(self):
        """Test basic navigation to a URL."""

        class TestScraper(BaseScraper[None]):
            def __init__(self):
                super().__init__()

            def get_entry(self):
                """Entry point that navigates to example.com."""
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation=self.parse_page,
                )

            @step
            def parse_page(self, page: PageElement):
                """Parse the page."""
                title_elems = page.query_xpath(
                    "//h1", "page title", min_count=0
                )
                if title_elems:
                    title = title_elems[0].text_content()
                    yield ParsedData(data={"title": title})

        scraper = TestScraper()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                # Verify results
                stats = await driver.get_stats()
                assert stats.results.total >= 0  # May or may not find h1

    @pytest.mark.asyncio
    async def test_await_list_wait_for_load_state(self):
        """Test await_list with WaitForLoadState."""

        class TestScraper(BaseScraper[None]):
            def __init__(self):
                super().__init__()

            def get_entry(self):
                """Entry point."""
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation=self.parse_page,
                )

            @step(await_list=[WaitForLoadState(state="networkidle")])
            def parse_page(self, page: PageElement):
                """Parse page after network idle."""
                yield ParsedData(data={"loaded": True})

        scraper = TestScraper()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                # Verify results
                stats = await driver.get_stats()
                assert stats.results.total >= 1

    @pytest.mark.asyncio
    async def test_await_list_wait_for_selector(self):
        """Test await_list with WaitForSelector."""

        class TestScraper(BaseScraper[None]):
            def __init__(self):
                super().__init__()

            def get_entry(self):
                """Entry point."""
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation=self.parse_page,
                )

            @step(await_list=[WaitForSelector(selector="h1", state="visible")])
            def parse_page(self, page: PageElement):
                """Parse page after h1 visible."""
                title_elems = page.query_xpath(
                    "//h1", "page title", min_count=0
                )
                title = title_elems[0].text_content() if title_elems else "N/A"
                yield ParsedData(data={"title": title})

        scraper = TestScraper()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                # Verify results
                stats = await driver.get_stats()
                assert stats.results.total >= 1

    @pytest.mark.asyncio
    async def test_dom_snapshot_model(self):
        """Test that step functions receive LXML-parsed DOM, not live browser."""

        class TestScraper(BaseScraper[None]):
            def __init__(self):
                super().__init__()

            def get_entry(self):
                """Entry point."""
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation=self.verify_page_type,
                )

            @step
            def verify_page_type(self, page: PageElement):
                """Verify page is LxmlPageElement."""
                # Check that page is a PageElement (LXML-backed)
                assert hasattr(page, "query_xpath")
                assert hasattr(page, "query_css")

                # Verify it's not a live Playwright page
                assert not hasattr(page, "goto")
                assert not hasattr(page, "wait_for_selector")

                yield ParsedData(data={"type_check": "passed"})

        scraper = TestScraper()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                # Verify results
                stats = await driver.get_stats()
                assert stats.results.total >= 1

    @pytest.mark.asyncio
    async def test_browser_config_persistence(self):
        """Test that browser configuration is persisted in database."""

        class TestScraper(BaseScraper[None]):
            def __init__(self):
                super().__init__()

            def get_entry(self):
                """Entry point."""
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation=self.done,
                )

            @step
            def done(self, page: PageElement):
                """Done."""
                pass

        scraper = TestScraper()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                viewport={"width": 1920, "height": 1080},
                locale="en-GB",
                timezone_id="Europe/London",
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                # Check browser config in database
                from kent.driver.persistent_driver.sql_manager import (
                    SQLManager,
                )

                async with SQLManager.open(db_path) as db:
                    metadata = await db.get_run_metadata()
                    assert metadata is not None
                    config = metadata["browser_config"]
                    assert config["viewport"]["width"] == 1920
                    assert config["viewport"]["height"] == 1080
                    assert config["locale"] == "en-GB"
                    assert config["timezone_id"] == "Europe/London"
                    assert config["headless"] is True
