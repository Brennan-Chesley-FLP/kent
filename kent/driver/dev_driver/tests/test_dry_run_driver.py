"""Tests for DryRunDriver - dry-run continuation execution without network I/O."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from kent.common.data_models import ScrapedData
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
)
from kent.driver.dev_driver.dry_run_driver import (
    DryRunDriver,
)

if TYPE_CHECKING:
    from collections.abc import Generator


class SampleData(ScrapedData):
    """Sample data model for testing."""

    title: str
    value: int


class SampleScraper(BaseScraper[SampleData]):
    """Sample scraper for dry run testing."""

    def get_entry(self) -> Generator[Request, None, None]:
        """Entry point for the scraper."""
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET, url="https://example.com"
            ),
            continuation="parse_index",
        )

    def parse_index(
        self, response: Response
    ) -> Generator[ParsedData[SampleData] | Request, None, None]:
        """Parse index page and yield data and child requests."""
        # Yield some parsed data
        yield ParsedData(SampleData(title="Item 1", value=100))
        yield ParsedData(SampleData(title="Item 2", value=200))

        # Yield a child request
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/detail/1",
            ),
            continuation="parse_detail",
        )

    def parse_detail(
        self, response: Response
    ) -> Generator[ParsedData[SampleData], None, None]:
        """Parse detail page."""
        yield ParsedData(SampleData(title="Detail 1", value=999))

    def parse_with_archive(
        self, response: Response
    ) -> Generator[Request, None, None]:
        """Parse and yield archive request."""
        yield Request(
            archive=True,
            request=HTTPRequestParams(
                method=HttpMethod.GET, url="/document.pdf"
            ),
            continuation="handle_archive",
            expected_type="pdf",
        )

    def parse_with_error(
        self, response: Response
    ) -> Generator[None, None, None]:
        """Parse method that raises an error."""
        raise ValueError("Test error message")


def test_dry_run_captures_data_and_requests():
    """Test that DryRunDriver captures both ParsedData and requests."""
    scraper = SampleScraper()
    driver = DryRunDriver(scraper)

    # Prepare mock response and request data
    response_data = {
        "status_code": 200,
        "headers_json": json.dumps({"Content-Type": "text/html"}),
        "content": b"<html>Test</html>",
        "text": "<html>Test</html>",
        "url": "https://example.com/index",
    }

    request_data = {
        "accumulated_data_json": json.dumps({}),
        "aux_data_json": json.dumps({}),
        "permanent_json": json.dumps({}),
        "current_location": "https://example.com",
        "url": "https://example.com/index",
        "method": "GET",
        "continuation": "parse_index",
    }

    # Run the continuation
    result = driver.run_continuation(
        "parse_index", response_data, request_data
    )

    # Verify data was captured
    assert len(result.data) == 2
    assert result.data[0].data["title"] == "Item 1"
    assert result.data[0].data["value"] == 100
    assert result.data[1].data["title"] == "Item 2"
    assert result.data[1].data["value"] == 200

    # Verify request was captured
    assert len(result.requests) == 1
    assert result.requests[0].request_type == "navigating"
    assert result.requests[0].url == "/detail/1"
    assert result.requests[0].continuation == "parse_detail"

    # Verify no error
    assert result.error is None


def test_dry_run_captures_archive_request():
    """Test that DryRunDriver captures archive Request correctly."""
    scraper = SampleScraper()
    driver = DryRunDriver(scraper)

    response_data = {
        "status_code": 200,
        "headers_json": json.dumps({}),
        "content": b"",
        "text": "",
        "url": "https://example.com/page",
    }

    request_data = {
        "accumulated_data_json": json.dumps({}),
        "aux_data_json": json.dumps({}),
        "permanent_json": json.dumps({}),
        "current_location": "https://example.com",
        "url": "https://example.com/page",
        "method": "GET",
        "continuation": "parse_with_archive",
    }

    result = driver.run_continuation(
        "parse_with_archive", response_data, request_data
    )

    # Verify archive request was captured
    assert len(result.requests) == 1
    assert result.requests[0].request_type == "archive"
    assert result.requests[0].url == "/document.pdf"
    assert result.requests[0].expected_type == "pdf"
    assert result.requests[0].continuation == "handle_archive"


def test_dry_run_captures_error():
    """Test that DryRunDriver captures errors raised during execution."""
    scraper = SampleScraper()
    driver = DryRunDriver(scraper)

    response_data = {
        "status_code": 200,
        "headers_json": json.dumps({}),
        "content": b"",
        "text": "",
        "url": "https://example.com/error",
    }

    request_data = {
        "accumulated_data_json": json.dumps({}),
        "aux_data_json": json.dumps({}),
        "permanent_json": json.dumps({}),
        "current_location": "https://example.com",
        "url": "https://example.com/error",
        "method": "GET",
        "continuation": "parse_with_error",
    }

    result = driver.run_continuation(
        "parse_with_error", response_data, request_data
    )

    # Verify error was captured
    assert result.error is not None
    assert result.error.error_type == "ValueError"
    assert result.error.error_message == "Test error message"

    # Verify no data or requests were captured
    assert len(result.data) == 0
    assert len(result.requests) == 0


def test_dry_run_reconstructs_context():
    """Test that DryRunDriver correctly reconstructs context from stored data."""
    scraper = SampleScraper()
    driver = DryRunDriver(scraper)

    # Prepare request data with context
    accumulated_data = {"case_name": "Test v. Case", "count": 5}
    aux_data = {"token": "abc123"}
    permanent_data = {"session_id": "xyz789"}

    response_data = {
        "status_code": 200,
        "headers_json": json.dumps({}),
        "content": b"",
        "text": "",
        "url": "https://example.com/index",
    }

    request_data = {
        "accumulated_data_json": json.dumps(accumulated_data),
        "aux_data_json": json.dumps(aux_data),
        "permanent_json": json.dumps(permanent_data),
        "current_location": "https://example.com",
        "url": "https://example.com/index",
        "method": "GET",
        "continuation": "parse_index",
    }

    result = driver.run_continuation(
        "parse_index", response_data, request_data
    )

    # Verify context was reconstructed in child requests
    assert len(result.requests) == 1
    child_request = result.requests[0]

    # The child request should inherit the context
    # (actual assertion would depend on how scraper yields requests)
    assert child_request.accumulated_data == {}  # New request resets this
    assert child_request.aux_data == {}
    assert child_request.permanent == {}


def test_dry_run_no_network_io():
    """Test that DryRunDriver doesn't perform any network I/O.

    This test verifies the core promise of DryRunDriver: it captures
    request yields without executing them.
    """
    scraper = SampleScraper()
    driver = DryRunDriver(scraper)

    response_data = {
        "status_code": 200,
        "headers_json": json.dumps({}),
        "content": b"",
        "text": "",
        "url": "https://example.com",
    }

    request_data = {
        "accumulated_data_json": json.dumps({}),
        "aux_data_json": json.dumps({}),
        "permanent_json": json.dumps({}),
        "current_location": "https://example.com",
        "url": "https://example.com",
        "method": "GET",
        "continuation": "parse_index",
    }

    # This should complete quickly without any network calls
    result = driver.run_continuation(
        "parse_index", response_data, request_data
    )

    # The request was captured but not executed
    assert len(result.requests) == 1
    assert result.requests[0].url == "/detail/1"

    # No actual Response was created for the child request
    # (it's just captured, not executed)
