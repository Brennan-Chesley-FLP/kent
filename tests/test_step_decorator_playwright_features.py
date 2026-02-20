"""Tests for Playwright-specific features in @step decorator.

This module tests the new features added to the @step decorator for
Playwright driver support:

- page parameter injection with SelectorObserver
- await_list parameter for wait conditions
- auto_await_timeout parameter for autowait retry
- Observer accessibility after step execution
"""

from collections.abc import Generator

from kent.common.decorators import (
    get_step_metadata,
    step,
)
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    ScraperYield,
    WaitForLoadState,
    WaitForSelector,
    WaitForTimeout,
    WaitForURL,
)
from kent.driver.sync_driver import SyncDriver
from tests.utils import collect_results


class TestPageInjection:
    """Tests for page parameter injection with observer."""

    def test_page_injected_with_observer(self, server_url: str, tmp_path):
        """The @step decorator shall inject PageElement when parameter is named 'page'."""

        class PageScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation=self.parse_with_page,
                )

            @step
            def parse_with_page(
                self, page
            ) -> Generator[ScraperYield[dict], None, None]:
                # page should be a PageElement
                # Query for something to verify it works (using min_count=0 since /test might 404)
                html_elements = page.query_xpath(
                    "//html", "html element", min_count=0
                )
                has_html = len(html_elements) > 0

                yield ParsedData(
                    data={"has_html": has_html, "page_works": True}
                )

        scraper = PageScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        # Page injection should work
        assert results[0]["page_works"] is True

    def test_page_and_lxml_tree_coexist(self, server_url: str, tmp_path):
        """Both page and lxml_tree parameters can be used in the same step."""

        class BothScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation=self.parse_with_both,
                )

            @step
            def parse_with_both(
                self, page, lxml_tree
            ) -> Generator[ScraperYield[dict], None, None]:
                # Both should be available
                # page is a PageElement, lxml_tree is a CheckedHtmlElement
                page_html_elements = page.query_xpath(
                    "//html", "page html", min_count=0
                )
                tree_html = lxml_tree.checked_xpath(
                    "//html", "tree html", min_count=0
                )

                yield ParsedData(
                    data={
                        "page_count": len(page_html_elements),
                        "tree_count": len(tree_html),
                        "both_work": True,
                    }
                )

        scraper = BothScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        # Both should work
        assert results[0]["both_work"] is True

    def test_observer_accessible_after_execution(
        self, server_url: str, tmp_path
    ):
        """The observer should be accessible via StepMetadata after step execution."""

        class ObserverScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation=self.parse_with_observer,
                )

            @step
            def parse_with_observer(
                self, page
            ) -> Generator[ScraperYield[dict], None, None]:
                # Query something to populate the observer
                page.query_xpath("//html", "html", min_count=0)
                yield ParsedData(data={"done": True})

        scraper = ObserverScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Check that observer was stored in metadata
        metadata = get_step_metadata(scraper.parse_with_observer)
        assert metadata is not None
        assert metadata.observer is not None
        # The observer should have recorded queries
        # (actual validation of observer state would be more complex)


class TestAwaitListMetadata:
    """Tests for await_list parameter on @step decorator."""

    def test_default_await_list_is_empty(self):
        """The default await_list should be an empty list."""

        @step
        def simple_step(
            self, lxml_tree
        ) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data={})

        metadata = get_step_metadata(simple_step)
        assert metadata is not None
        assert metadata.await_list == []

    def test_await_list_single_condition(self):
        """A single wait condition should be stored in metadata."""

        @step(await_list=[WaitForSelector("#content", state="visible")])
        def wait_step(self, lxml_tree) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data={})

        metadata = get_step_metadata(wait_step)
        assert metadata is not None
        assert len(metadata.await_list) == 1
        assert isinstance(metadata.await_list[0], WaitForSelector)
        assert metadata.await_list[0].selector == "#content"
        assert metadata.await_list[0].state == "visible"

    def test_await_list_multiple_conditions(self):
        """Multiple wait conditions should be preserved in order."""

        await_list = [
            WaitForLoadState(state="domcontentloaded"),
            WaitForSelector("//div[@id='results']", state="visible"),
            WaitForTimeout(timeout=500),
        ]

        @step(await_list=await_list)
        def multi_wait_step(
            self, lxml_tree
        ) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data={})

        metadata = get_step_metadata(multi_wait_step)
        assert metadata is not None
        assert len(metadata.await_list) == 3
        assert isinstance(metadata.await_list[0], WaitForLoadState)
        assert isinstance(metadata.await_list[1], WaitForSelector)
        assert isinstance(metadata.await_list[2], WaitForTimeout)

    def test_await_list_with_url_condition(self):
        """WaitForURL condition should be stored correctly."""

        @step(await_list=[WaitForURL(url="https://example.com/results")])
        def url_wait_step(
            self, lxml_tree
        ) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data={})

        metadata = get_step_metadata(url_wait_step)
        assert metadata is not None
        assert len(metadata.await_list) == 1
        assert isinstance(metadata.await_list[0], WaitForURL)
        assert metadata.await_list[0].url == "https://example.com/results"

    def test_await_list_with_other_parameters(self):
        """await_list should work alongside other step parameters."""

        @step(
            priority=5,
            encoding="utf-8",
            await_list=[WaitForSelector("#content")],
        )
        def combined_step(
            self, lxml_tree
        ) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data={})

        metadata = get_step_metadata(combined_step)
        assert metadata is not None
        assert metadata.priority == 5
        assert metadata.encoding == "utf-8"
        assert len(metadata.await_list) == 1


class TestAutoAwaitTimeout:
    """Tests for auto_await_timeout parameter on @step decorator."""

    def test_default_auto_await_timeout_is_none(self):
        """The default auto_await_timeout should be None."""

        @step
        def simple_step(
            self, lxml_tree
        ) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data={})

        metadata = get_step_metadata(simple_step)
        assert metadata is not None
        assert metadata.auto_await_timeout is None

    def test_auto_await_timeout_set(self):
        """auto_await_timeout should be stored in metadata."""

        @step(auto_await_timeout=10000)
        def autowait_step(
            self, lxml_tree
        ) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data={})

        metadata = get_step_metadata(autowait_step)
        assert metadata is not None
        assert metadata.auto_await_timeout == 10000

    def test_auto_await_timeout_with_await_list(self):
        """auto_await_timeout and await_list should work together."""

        @step(
            await_list=[WaitForLoadState(state="networkidle")],
            auto_await_timeout=15000,
        )
        def combined_wait_step(
            self, lxml_tree
        ) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data={})

        metadata = get_step_metadata(combined_wait_step)
        assert metadata is not None
        assert len(metadata.await_list) == 1
        assert metadata.auto_await_timeout == 15000

    def test_auto_await_timeout_various_values(self):
        """Different timeout values should be stored correctly."""
        timeouts = [1000, 5000, 10000, 30000]

        for timeout in timeouts:

            @step(auto_await_timeout=timeout)
            def timeout_step(
                self, lxml_tree
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

            metadata = get_step_metadata(timeout_step)
            assert metadata is not None
            assert metadata.auto_await_timeout == timeout


class TestCombinedPlaywrightFeatures:
    """Tests for combined Playwright features."""

    def test_page_with_await_list_and_auto_await(self):
        """All Playwright features should work together."""

        @step(
            await_list=[
                WaitForLoadState(state="domcontentloaded"),
                WaitForSelector("#content", state="visible", timeout=5000),
            ],
            auto_await_timeout=10000,
            priority=3,
        )
        def full_featured_step(
            self, page
        ) -> Generator[ScraperYield, None, None]:
            # Use page parameter with all Playwright features enabled
            yield ParsedData(data={"processed": True})

        metadata = get_step_metadata(full_featured_step)
        assert metadata is not None
        assert metadata.priority == 3
        assert len(metadata.await_list) == 2
        assert metadata.auto_await_timeout == 10000

    def test_backward_compatibility_maintained(self):
        """Old-style steps without Playwright features should still work."""

        @step
        def old_style_step(
            self, lxml_tree
        ) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data={})

        metadata = get_step_metadata(old_style_step)
        assert metadata is not None
        assert metadata.await_list == []
        assert metadata.auto_await_timeout is None
        assert metadata.observer is None
