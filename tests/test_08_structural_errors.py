"""Tests for Step 8: Structural Assumption Errors.

This module tests the structural assumption error handling introduced in Step 8:
1. ScraperAssumptionException base class with context-aware error messages
2. HTMLStructuralAssumptionException for HTML structure mismatches
3. CheckedHtmlElement wrapper with validated selectors (checked_xpath, checked_css)
4. Error page endpoint in mock server (?error=true query parameter)
5. Integration test showing exception raised when scraper encounters error page

Tests use a real aiohttp server to verify actual HTTP behavior.
"""

import pytest
from lxml.html import fromstring

from kent.common.checked_html import CheckedHtmlElement
from kent.common.exceptions import (
    HTMLStructuralAssumptionException,
    ScraperAssumptionException,
)


class TestScraperAssumptionException:
    """Tests for ScraperAssumptionException base class."""

    def test_exception_has_required_attributes(self):
        """ScraperAssumptionException shall have message, request_url, and context attributes."""
        exc = ScraperAssumptionException(
            message="Test error",
            request_url="http://example.com/test",
            context={"key": "value"},
        )

        assert exc.message == "Test error"
        assert exc.request_url == "http://example.com/test"
        assert exc.context == {"key": "value"}

    def test_exception_context_defaults_to_empty_dict(self):
        """ScraperAssumptionException shall default context to empty dict."""
        exc = ScraperAssumptionException(
            message="Test error",
            request_url="http://example.com/test",
        )

        assert exc.context == {}

    def test_exception_formats_message_with_url(self):
        """ScraperAssumptionException shall include URL in formatted message."""
        exc = ScraperAssumptionException(
            message="Test error",
            request_url="http://example.com/test",
        )

        formatted = str(exc)
        assert "Test error" in formatted
        assert "URL: http://example.com/test" in formatted

    def test_exception_formats_message_with_context(self):
        """ScraperAssumptionException shall include context in formatted message."""
        exc = ScraperAssumptionException(
            message="Test error",
            request_url="http://example.com/test",
            context={"selector": "//div", "count": 0},
        )

        formatted = str(exc)
        assert "Test error" in formatted
        assert "URL: http://example.com/test" in formatted
        assert "Context:" in formatted
        assert "selector: //div" in formatted
        assert "count: 0" in formatted


class TestHTMLStructuralAssumptionException:
    """Tests for HTMLStructuralAssumptionException."""

    def test_exception_has_required_attributes(self):
        """HTMLStructuralAssumptionException shall have all required attributes."""
        exc = HTMLStructuralAssumptionException(
            selector="//div[@class='test']",
            selector_type="xpath",
            description="test divs",
            expected_min=1,
            expected_max=5,
            actual_count=0,
            request_url="http://example.com/test",
        )

        assert exc.selector == "//div[@class='test']"
        assert exc.selector_type == "xpath"
        assert exc.description == "test divs"
        assert exc.expected_min == 1
        assert exc.expected_max == 5
        assert exc.actual_count == 0
        assert exc.request_url == "http://example.com/test"

    def test_exception_formats_expected_count_at_least(self):
        """HTMLStructuralAssumptionException shall format 'at least N' when max is None."""
        exc = HTMLStructuralAssumptionException(
            selector="//div",
            selector_type="xpath",
            description="test divs",
            expected_min=5,
            expected_max=None,
            actual_count=3,
            request_url="http://example.com/test",
        )

        formatted = str(exc)
        assert "at least 5" in formatted
        assert "found 3" in formatted

    def test_exception_formats_expected_count_exactly(self):
        """HTMLStructuralAssumptionException shall format 'exactly N' when min equals max."""
        exc = HTMLStructuralAssumptionException(
            selector="//div",
            selector_type="xpath",
            description="test div",
            expected_min=1,
            expected_max=1,
            actual_count=0,
            request_url="http://example.com/test",
        )

        formatted = str(exc)
        assert "exactly 1" in formatted
        assert "found 0" in formatted

    def test_exception_formats_expected_count_between(self):
        """HTMLStructuralAssumptionException shall format 'between N and M' when min != max."""
        exc = HTMLStructuralAssumptionException(
            selector="//div",
            selector_type="xpath",
            description="test divs",
            expected_min=5,
            expected_max=10,
            actual_count=12,
            request_url="http://example.com/test",
        )

        formatted = str(exc)
        assert "between 5 and 10" in formatted
        assert "found 12" in formatted

    def test_exception_includes_context_dict(self):
        """HTMLStructuralAssumptionException shall include context dict with all details."""
        exc = HTMLStructuralAssumptionException(
            selector="//div",
            selector_type="xpath",
            description="test divs",
            expected_min=1,
            expected_max=5,
            actual_count=0,
            request_url="http://example.com/test",
        )

        assert exc.context["selector"] == "//div"
        assert exc.context["selector_type"] == "xpath"
        assert exc.context["expected_min"] == 1
        assert exc.context["expected_max"] == 5
        assert exc.context["actual_count"] == 0


class TestCheckedHtmlElement:
    """Tests for CheckedHtmlElement wrapper class."""

    def test_checked_xpath_returns_results_when_count_matches(self):
        """CheckedHtmlElement.checked_xpath shall return results when count matches expectations."""
        html = "<html><body><div>1</div><div>2</div><div>3</div></body></html>"
        tree = CheckedHtmlElement(fromstring(html))

        results = tree.checked_xpath("//div", "divs", min_count=3, max_count=3)

        assert len(results) == 3
        assert results[0].text == "1"
        assert results[1].text == "2"
        assert results[2].text == "3"

    def test_checked_xpath_raises_when_count_below_min(self):
        """CheckedHtmlElement.checked_xpath shall raise exception when count is below min."""
        html = "<html><body><div>1</div></body></html>"
        tree = CheckedHtmlElement(
            fromstring(html), request_url="http://example.com/test"
        )

        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            tree.checked_xpath("//div", "divs", min_count=3)

        exc = exc_info.value
        assert exc.actual_count == 1
        assert exc.expected_min == 3
        assert exc.selector == "//div"
        assert exc.selector_type == "xpath"
        assert exc.request_url == "http://example.com/test"

    def test_checked_xpath_raises_when_count_above_max(self):
        """CheckedHtmlElement.checked_xpath shall raise exception when count is above max."""
        html = "<html><body><div>1</div><div>2</div><div>3</div></body></html>"
        tree = CheckedHtmlElement(
            fromstring(html), request_url="http://example.com/test"
        )

        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            tree.checked_xpath("//div", "divs", min_count=1, max_count=2)

        exc = exc_info.value
        assert exc.actual_count == 3
        assert exc.expected_max == 2

    def test_checked_xpath_defaults_to_min_count_one(self):
        """CheckedHtmlElement.checked_xpath shall default min_count to 1."""
        html = "<html><body></body></html>"
        tree = CheckedHtmlElement(fromstring(html))

        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            tree.checked_xpath("//div", "divs")

        exc = exc_info.value
        assert exc.expected_min == 1
        assert exc.actual_count == 0

    def test_checked_xpath_allows_unlimited_max_count(self):
        """CheckedHtmlElement.checked_xpath shall allow unlimited max_count when None."""
        html = (
            "<html><body>"
            + "".join(f"<div>{i}</div>" for i in range(100))
            + "</body></html>"
        )
        tree = CheckedHtmlElement(fromstring(html))

        results = tree.checked_xpath(
            "//div", "divs", min_count=10, max_count=None
        )

        assert len(results) == 100

    def test_checked_css_returns_results_when_count_matches(self):
        """CheckedHtmlElement.checked_css shall return results when count matches expectations."""
        html = "<html><body><div class='test'>1</div><div class='test'>2</div></body></html>"
        tree = CheckedHtmlElement(fromstring(html))

        results = tree.checked_css(
            "div.test", "test divs", min_count=2, max_count=2
        )

        assert len(results) == 2
        assert results[0].text == "1"
        assert results[1].text == "2"

    def test_checked_css_raises_when_count_below_min(self):
        """CheckedHtmlElement.checked_css shall raise exception when count is below min."""
        html = "<html><body><div class='test'>1</div></body></html>"
        tree = CheckedHtmlElement(
            fromstring(html), request_url="http://example.com/test"
        )

        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            tree.checked_css("div.test", "test divs", min_count=3)

        exc = exc_info.value
        assert exc.actual_count == 1
        assert exc.expected_min == 3
        assert exc.selector == "div.test"
        assert exc.selector_type == "css"

    def test_checked_css_raises_when_count_above_max(self):
        """CheckedHtmlElement.checked_css shall raise exception when count is above max."""
        html = "<html><body><div class='test'>1</div><div class='test'>2</div><div class='test'>3</div></body></html>"
        tree = CheckedHtmlElement(fromstring(html))

        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            tree.checked_css("div.test", "test divs", min_count=1, max_count=2)

        exc = exc_info.value
        assert exc.actual_count == 3
        assert exc.expected_max == 2

    def test_checked_css_handles_invalid_selector(self):
        """CheckedHtmlElement.checked_css shall raise exception for invalid CSS selector."""
        html = "<html><body></body></html>"
        tree = CheckedHtmlElement(
            fromstring(html), request_url="http://example.com/test"
        )

        # Invalid CSS selector (e.g., unmatched bracket)
        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            tree.checked_css("[invalid[", "invalid selector")

        exc = exc_info.value
        assert exc.selector == "[invalid["
        assert exc.selector_type == "css"

    def test_element_delegation_works(self):
        """CheckedHtmlElement shall delegate attribute access to wrapped element."""
        html = "<html><body><div id='test'>content</div></body></html>"
        tree = CheckedHtmlElement(fromstring(html))

        # Should be able to access lxml element methods
        assert tree.tag == "html"
        body = tree.find(".//body")
        assert body is not None
        assert body.tag == "body"

    def test_request_url_is_optional(self):
        """CheckedHtmlElement shall allow optional request_url parameter."""
        html = "<html><body></body></html>"
        tree = CheckedHtmlElement(fromstring(html))

        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            tree.checked_xpath("//div", "divs")

        exc = exc_info.value
        assert exc.request_url == ""


class TestNestedCheckedQueries:
    """Tests for nested checked_xpath and checked_css queries."""

    def test_nested_xpath_queries_work(self):
        """CheckedHtmlElement shall support nested checked_xpath queries."""
        html = "<doc><a><b>1</b><b>2</b></a></doc>"
        doc = CheckedHtmlElement(fromstring(html))

        # First level query
        a_elements = doc.checked_xpath("//a", "A elements", min_count=1)
        assert len(a_elements) == 1

        # Nested query - should return CheckedHtmlElement
        a = a_elements[0]
        assert isinstance(a, CheckedHtmlElement)

        # Second level query
        b_elements = a.checked_xpath(
            ".//b", "B elements", min_count=2, max_count=2
        )
        assert len(b_elements) == 2
        assert b_elements[0].text == "1"
        assert b_elements[1].text == "2"

    def test_nested_xpath_raises_on_count_mismatch(self):
        """CheckedHtmlElement shall raise exception in nested queries when count mismatches."""
        html = "<doc><a><b>1</b></a></doc>"
        doc = CheckedHtmlElement(fromstring(html), "http://example.com/test")

        # First level query succeeds
        a_elements = doc.checked_xpath("//a", "A elements", min_count=1)
        a = a_elements[0]

        # Second level query should fail - expects 2 but finds 1
        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            a.checked_xpath(".//b", "B elements", min_count=2)

        exc = exc_info.value
        assert exc.actual_count == 1
        assert exc.expected_min == 2
        assert exc.description == "B elements"
        assert exc.request_url == "http://example.com/test"

    def test_nested_css_queries_work(self):
        """CheckedHtmlElement shall support nested checked_css queries."""
        html = "<doc><div class='outer'><span class='inner'>1</span><span class='inner'>2</span></div></doc>"
        doc = CheckedHtmlElement(fromstring(html))

        # First level query
        outer_elements = doc.checked_css(
            "div.outer", "outer divs", min_count=1
        )
        assert len(outer_elements) == 1

        # Nested query
        outer = outer_elements[0]
        assert isinstance(outer, CheckedHtmlElement)

        inner_elements = outer.checked_css(
            "span.inner", "inner spans", min_count=2
        )
        assert len(inner_elements) == 2

    def test_nested_css_raises_on_count_mismatch(self):
        """CheckedHtmlElement shall raise exception in nested CSS queries when count mismatches."""
        html = (
            "<doc><div class='outer'><span class='inner'>1</span></div></doc>"
        )
        doc = CheckedHtmlElement(fromstring(html), "http://example.com/test")

        outer_elements = doc.checked_css(
            "div.outer", "outer divs", min_count=1
        )
        outer = outer_elements[0]

        # Should fail - expects 2 but finds 1
        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            outer.checked_css("span.inner", "inner spans", min_count=2)

        exc = exc_info.value
        assert exc.actual_count == 1
        assert exc.expected_min == 2

    def test_mixed_xpath_css_nesting(self):
        """CheckedHtmlElement shall support mixing XPath and CSS in nested queries."""
        html = "<doc><div class='container'><item id='i1'/><item id='i2'/></div></doc>"
        doc = CheckedHtmlElement(fromstring(html))

        # XPath first level
        containers = doc.checked_xpath(
            "//div[@class='container']", "containers", min_count=1
        )
        container = containers[0]

        # CSS second level
        items = container.checked_css(
            "item", "items", min_count=2, max_count=2
        )
        assert len(items) == 2

    def test_deeply_nested_queries(self):
        """CheckedHtmlElement shall support deeply nested queries (3+ levels)."""
        html = "<doc><level1><level2><level3>value</level3></level2></level1></doc>"
        doc = CheckedHtmlElement(fromstring(html))

        level1 = doc.checked_xpath("//level1", "level1", min_count=1)[0]
        level2 = level1.checked_xpath(".//level2", "level2", min_count=1)[0]
        level3 = level2.checked_xpath(".//level3", "level3", min_count=1)[0]

        assert level3.text == "value"

    def test_request_url_propagates_through_nesting(self):
        """CheckedHtmlElement shall propagate request_url through nested queries."""
        html = "<doc><a><b>1</b></a></doc>"
        doc = CheckedHtmlElement(fromstring(html), "http://example.com/page")

        a = doc.checked_xpath("//a", "A", min_count=1)[0]

        # Trigger an error in nested query
        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            a.checked_xpath(".//b", "B", min_count=5)

        exc = exc_info.value
        # request_url should be preserved from root
        assert exc.request_url == "http://example.com/page"

    def test_xpath_text_results_are_strings_not_checkable(self):
        """CheckedHtmlElement shall return strings for XPath text() queries, not CheckedHtmlElement."""
        html = "<doc><item>text1</item><item>text2</item></doc>"
        doc = CheckedHtmlElement(fromstring(html))

        # XPath that returns text nodes
        items = doc.xpath("//item/text()")

        # These should be strings, not CheckedHtmlElement
        assert len(items) == 2
        assert isinstance(items[0], str)
        assert isinstance(items[1], str)
        assert items[0] == "text1"
        assert items[1] == "text2"

        # Should not have checked_xpath method
        assert not hasattr(items[0], "checked_xpath")

    def test_xpath_attribute_results_are_strings_not_checkable(self):
        """CheckedHtmlElement shall return strings for XPath attribute queries, not CheckedHtmlElement."""
        html = "<doc><item id='a'/><item id='b'/></doc>"
        doc = CheckedHtmlElement(fromstring(html))

        # XPath that returns attributes
        ids = doc.xpath("//item/@id")

        # These should be strings
        assert len(ids) == 2
        assert isinstance(ids[0], str)
        assert isinstance(ids[1], str)
        assert ids[0] == "a"
        assert ids[1] == "b"


class TestMockServerErrorEndpoint:
    """Tests for mock server error page endpoint."""

    def test_error_page_returns_different_structure(self, server_url: str):
        """The mock server shall return error page with different HTML structure when ?error=true."""
        import httpx

        docket = "BCC-2024-001"

        # Normal response
        normal_response = httpx.get(f"{server_url}/cases/{docket}")
        assert normal_response.status_code == 200
        assert "Bug Civil Court" in normal_response.text
        assert "case-detail" in normal_response.text

        # Error response
        error_response = httpx.get(f"{server_url}/cases/{docket}?error=true")
        assert error_response.status_code == 200
        assert "Service Temporarily Unavailable" in error_response.text
        assert "error-container" in error_response.text
        # Normal structure should NOT be present
        assert "case-detail" not in error_response.text

    def test_error_page_includes_reference_docket(self, server_url: str):
        """The error page shall include the docket number as reference."""
        import httpx

        docket = "BCC-2024-001"
        response = httpx.get(f"{server_url}/cases/{docket}?error=true")

        assert response.status_code == 200
        assert docket in response.text
        assert "Reference:" in response.text

    def test_error_page_has_error_code(self, server_url: str):
        """The error page shall include an error code."""
        import httpx

        docket = "BCC-2024-001"
        response = httpx.get(f"{server_url}/cases/{docket}?error=true")

        assert response.status_code == 200
        assert "Error code:" in response.text
        assert "STRUCT_CHANGE_001" in response.text


class TestIntegrationWithScraper:
    """Integration tests showing exception raised when scraper encounters error page."""

    def test_scraper_raises_exception_on_structural_change(
        self, server_url: str, tmp_path
    ):
        """The scraper shall raise HTMLStructuralAssumptionException when encountering error page."""
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from tests.scraper.example.bug_court import (
            BugCourtScraper,
        )

        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        # Simulate receiving error page response
        error_html = """<!DOCTYPE html>
<html>
<head><title>Error - Bug Civil Court</title></head>
<body>
    <div class="error-container">
        <h1>Service Temporarily Unavailable</h1>
        <p>The case detail page is currently unavailable.</p>
    </div>
</body>
</html>"""

        response = Response(
            status_code=200,
            headers={},
            content=error_html.encode(),
            text=error_html,
            url=f"{server_url}/cases/BCC-2024-001?error=true",
            request=Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{server_url}/cases/BCC-2024-001?error=true",
                ),
                continuation="parse_detail",
            ),
        )

        # The scraper's parse_detail should raise an exception when expected elements are missing
        with pytest.raises(HTMLStructuralAssumptionException) as exc_info:
            list(scraper.parse_detail(response))

        exc = exc_info.value
        assert exc.actual_count == 0
        assert exc.request_url == f"{server_url}/cases/BCC-2024-001?error=true"
        # Should mention what was being looked for
        assert exc.description != ""

    def test_normal_page_does_not_raise_exception(
        self, server_url: str, tmp_path
    ):
        """The scraper shall not raise exception when encountering normal page structure."""
        from kent.driver.sync_driver import SyncDriver
        from tests.scraper.example.bug_court import (
            BugCourtScraper,
        )
        from tests.utils import collect_results

        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url
        callback, results = collect_results()
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)

        # Should complete without exceptions
        driver.run()

        # Should have results
        assert len(results) > 0


class TestStructuralErrorCallback:
    """Tests for on_structural_error callback in SyncDriver."""

    def test_callback_receives_exception_and_can_stop(
        self, server_url: str, tmp_path
    ):
        """The driver shall invoke on_structural_error callback and stop when it returns False."""
        from collections.abc import Generator

        from lxml.html import fromstring

        from kent.common.checked_html import (
            CheckedHtmlElement,
        )
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        # Track callback invocations
        callback_invocations = []

        def error_callback(exc: ScraperAssumptionException) -> bool:
            callback_invocations.append(exc)
            return False  # Stop scraping

        # Create a custom scraper that immediately encounters a structural error
        class ErrorScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001?error=true",
                    ),
                    continuation="parse_error_page",
                )

            def parse_error_page(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                tree = CheckedHtmlElement(
                    fromstring(response.text), response.url
                )
                # This will fail on error page (missing case-details element)
                tree.checked_xpath(
                    "//div[@class='case-details']",
                    "case details",
                    min_count=1,
                )
                yield ParsedData({})

        scraper = ErrorScraper()
        callback, results = collect_results()
        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_structural_error=error_callback,
        )

        # Run should not raise exception
        driver.run()

        # Callback should have been invoked once
        assert len(callback_invocations) == 1
        exc = callback_invocations[0]
        assert isinstance(exc, HTMLStructuralAssumptionException)
        assert exc.actual_count == 0

        # No results should have been collected
        assert len(results) == 0

    def test_callback_can_continue_scraping(self, server_url: str, tmp_path):
        """The driver shall continue scraping when on_structural_error callback returns True."""
        from collections.abc import Generator

        from lxml.html import fromstring

        from kent.common.checked_html import (
            CheckedHtmlElement,
        )
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        # Track callback invocations
        callback_invocations = []

        def error_callback(exc: ScraperAssumptionException) -> bool:
            callback_invocations.append(exc)
            return True  # Continue scraping

        # Create a scraper that yields error page then normal page
        class MixedScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases",
                    ),
                    continuation="parse_list",
                )

            def parse_list(
                self, response: Response
            ) -> Generator[Request, None, None]:
                # First yield error page
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001?error=true",
                    ),
                    continuation="parse_detail",
                )
                # Then yield normal page
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-002",
                    ),
                    continuation="parse_detail",
                )

            def parse_detail(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                tree = CheckedHtmlElement(
                    fromstring(response.text), response.url
                )
                # This will fail on error page but succeed on normal page
                tree.checked_xpath(
                    "//div[@class='case-details']",
                    "case details",
                    min_count=1,
                )
                yield ParsedData({"url": response.url})

        scraper = MixedScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_structural_error=error_callback,
        )

        # Run should not raise exception
        driver.run()

        # Callback should have been invoked once (for error page)
        assert len(callback_invocations) == 1

        # Should have results from the normal page (second request)
        assert len(results) == 1
        assert "BCC-2024-002" in results[0]["url"]

    def test_no_callback_raises_exception(self, server_url: str, tmp_path):
        """The driver shall raise exception when on_structural_error is not provided."""
        from collections.abc import Generator

        from lxml.html import fromstring

        from kent.common.checked_html import (
            CheckedHtmlElement,
        )
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        # Create a scraper that encounters structural error
        class ErrorScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001?error=true",
                    ),
                    continuation="parse_error_page",
                )

            def parse_error_page(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                tree = CheckedHtmlElement(
                    fromstring(response.text), response.url
                )
                # This will fail on error page
                tree.checked_xpath(
                    "//div[@class='case-details']",
                    "case details",
                    min_count=1,
                )
                yield ParsedData({})

        scraper = ErrorScraper()
        callback, results = collect_results()
        # No on_structural_error callback
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)

        # Should raise exception
        with pytest.raises(HTMLStructuralAssumptionException):
            driver.run()

    def test_log_structural_error_and_stop_callback(
        self, server_url: str, tmp_path, caplog
    ):
        """The log_structural_error_and_stop callback shall log the error and return False."""
        from collections.abc import Generator

        from lxml.html import fromstring

        from kent.common.checked_html import (
            CheckedHtmlElement,
        )
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import (
            collect_results,
            log_structural_error_and_stop,
        )

        # Create a scraper that encounters structural error
        class ErrorScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001?error=true",
                    ),
                    continuation="parse_error_page",
                )

            def parse_error_page(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                tree = CheckedHtmlElement(
                    fromstring(response.text), response.url
                )
                # This will fail on error page
                tree.checked_xpath(
                    "//div[@class='case-details']",
                    "case details",
                    min_count=1,
                )
                yield ParsedData({})

        scraper = ErrorScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_structural_error=log_structural_error_and_stop,
        )

        # Run should not raise exception
        driver.run()

        # Should have logged the error
        assert len(caplog.records) > 0
        log_record = caplog.records[0]
        assert "Structural assumption failed" in log_record.message
        assert log_record.url == f"{server_url}/cases/BCC-2024-001?error=true"

        # No results collected
        assert len(results) == 0
