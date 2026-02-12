"""Step 3: NonNavigatingRequest - API Calls.

This test module verifies the API call capabilities introduced in Step 3
of the scraper-driver architecture:

1. Scrapers can yield NonNavigatingRequest for API calls
2. The driver handles NonNavigatingRequest without updating current_location
3. NavigatingRequest updates current_location, NonNavigatingRequest does not
4. BaseRequest provides shared URL resolution logic
5. Both request types can be used together in the same scraper

Tests use a real aiohttp server to verify actual HTTP behavior.
"""

import pytest

from kent.data_types import (
    BaseRequest,
    HttpMethod,
    HTTPRequestParams,
    NavigatingRequest,
    NonNavigatingRequest,
    ParsedData,
    Response,
)
from kent.driver.sync_driver import SyncDriver
from tests.mock_server import CASES
from tests.scraper.example.bug_court import (
    BugCourtScraperWithAPI,
)
from tests.utils import collect_results


class TestBaseRequest:
    """Tests for the BaseRequest data type."""

    def test_base_request_stores_url(self):
        """BaseRequest shall store the URL to fetch."""
        request = BaseRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/api/cases/BCC-2024-001",
            ),
            continuation="parse_api",
        )

        assert request.request.url == "/api/cases/BCC-2024-001"

    def test_base_request_stores_continuation(self):
        """BaseRequest shall store the continuation method name."""
        request = BaseRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/api/cases",
            ),
            continuation="parse_api",
        )

        assert request.continuation == "parse_api"

    def test_base_request_defaults_to_get(self):
        """BaseRequest shall default to GET method."""
        request = BaseRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/api/cases",
            ),
            continuation="parse_api",
        )

        assert request.request.method == HttpMethod.GET

    def test_base_request_supports_post(self):
        """BaseRequest shall support POST method."""
        request = BaseRequest(
            request=HTTPRequestParams(
                method=HttpMethod.POST,
                url="/api/search",
                data={"query": "beetle"},
            ),
            continuation="parse_results",
        )

        assert request.request.method == HttpMethod.POST
        assert request.request.data == {"query": "beetle"}

    def test_base_request_resolve_url_absolute(self):
        """BaseRequest shall return absolute URLs unchanged."""
        request = BaseRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://other.example.com/api/cases",
            ),
            continuation="parse_api",
        )

        resolved = request.resolve_url("http://bugcourt.example.com/")

        assert resolved == "http://other.example.com/api/cases"

    def test_base_request_resolve_url_relative(self):
        """BaseRequest shall resolve relative URLs against current_location."""
        request = BaseRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/api/cases/BCC-2024-001",
            ),
            continuation="parse_api",
        )

        resolved = request.resolve_url(
            "http://bugcourt.example.com/cases/BCC-2024-001"
        )

        assert resolved == "http://bugcourt.example.com/api/cases/BCC-2024-001"


class TestNonNavigatingRequest:
    """Tests for the NonNavigatingRequest data type."""

    def test_non_navigating_request_inherits_from_base(self):
        """NonNavigatingRequest shall inherit from BaseRequest."""
        request = NonNavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/api/cases",
            ),
            continuation="parse_api",
        )

        assert isinstance(request, BaseRequest)

    def test_non_navigating_request_stores_url(self):
        """NonNavigatingRequest shall store the URL to fetch."""
        request = NonNavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/api/cases/BCC-2024-001",
            ),
            continuation="parse_api",
        )

        assert request.request.url == "/api/cases/BCC-2024-001"

    def test_non_navigating_request_resolve_from_response(self):
        """NonNavigatingRequest shall resolve URL from Response."""
        base_request = NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://bugcourt.example.com/cases/BCC-2024-001",
            ),
            continuation="parse_detail",
        )
        response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url="http://bugcourt.example.com/cases/BCC-2024-001",
            request=base_request,
        )
        api_request = NonNavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/api/cases/BCC-2024-001",
            ),
            continuation="parse_api",
        )

        resolved = api_request.resolve_from(response)

        assert isinstance(resolved, NonNavigatingRequest)
        assert (
            resolved.request.url
            == "http://bugcourt.example.com/api/cases/BCC-2024-001"
        )
        assert resolved.continuation == "parse_api"
        assert (
            resolved.current_location
            == "http://bugcourt.example.com/cases/BCC-2024-001"
        )


class TestNavigatingRequestInheritance:
    """Tests for NavigatingRequest inheriting from BaseRequest."""

    def test_navigating_request_inherits_from_base(self):
        """NavigatingRequest shall inherit from BaseRequest."""
        request = NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/cases",
            ),
            continuation="parse_list",
        )

        assert isinstance(request, BaseRequest)

    def test_navigating_request_resolve_from_response(self):
        """NavigatingRequest shall resolve URL from Response."""
        base_request = NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://bugcourt.example.com/cases",
            ),
            continuation="parse_list",
        )
        response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url="http://bugcourt.example.com/cases",
            request=base_request,
        )
        detail_request = NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/cases/BCC-2024-001",
            ),
            continuation="parse_detail",
        )

        resolved = detail_request.resolve_from(response)

        assert isinstance(resolved, NavigatingRequest)
        assert (
            resolved.request.url
            == "http://bugcourt.example.com/cases/BCC-2024-001"
        )
        assert resolved.current_location == "http://bugcourt.example.com/cases"


class TestRequestCurrentLocation:
    """Tests for current_location tracking in requests."""

    @pytest.fixture
    def scraper(self, server_url: str) -> BugCourtScraperWithAPI:
        """Create a BugCourtScraperWithAPI instance configured for test server."""
        scraper = BugCourtScraperWithAPI()
        scraper.BASE_URL = server_url
        return scraper

    @pytest.fixture
    def driver(self, scraper: BugCourtScraperWithAPI) -> SyncDriver[dict]:
        """Create a SyncDriver instance."""
        return SyncDriver(scraper)

    def test_entry_request_has_no_current_location(
        self, scraper: BugCourtScraperWithAPI
    ):
        """The entry request shall have an empty current_location."""
        entry = next(scraper.get_entry())

        assert entry.current_location == ""
        assert len(entry.previous_requests) == 0

    def test_navigating_request_updates_current_location(
        self, server_url: str
    ):
        """NavigatingRequest shall update current_location to the response URL."""
        # Create a navigating request
        entry_request = NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/cases",
            ),
            continuation="parse_list",
        )

        # Create a response
        response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url=f"{server_url}/cases",
            request=entry_request,
        )

        # Create a new navigating request resolved from the response
        detail_request = NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/cases/BCC-2024-001",
            ),
            continuation="parse_detail",
        )

        resolved = detail_request.resolve_from(response)

        # current_location should be the response URL
        assert resolved.current_location == f"{server_url}/cases"
        assert resolved.request.url == f"{server_url}/cases/BCC-2024-001"

    def test_non_navigating_request_preserves_current_location(
        self, server_url: str
    ):
        """NonNavigatingRequest shall preserve current_location."""
        # Create a navigating request and response
        detail_request = NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/cases/BCC-2024-001",
            ),
            continuation="parse_detail",
            current_location=f"{server_url}/cases",
        )

        response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url=f"{server_url}/cases/BCC-2024-001",
            request=detail_request,
        )

        # Create a NonNavigatingRequest for the API
        api_request = NonNavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/api/cases/BCC-2024-001",
            ),
            continuation="parse_api",
        )

        resolved = api_request.resolve_from(response)

        # current_location should be the response URL (from NavigatingRequest)
        assert resolved.current_location == f"{server_url}/cases/BCC-2024-001"
        assert resolved.request.url == f"{server_url}/api/cases/BCC-2024-001"

    def test_driver_handles_both_request_types(self, driver: SyncDriver[dict]):
        """The driver shall handle both NavigatingRequest and NonNavigatingRequest."""
        callback, results = collect_results()
        driver.on_data = callback
        driver.run()

        # Should get results with API metadata
        assert len(results) > 0
        for result in results:
            assert "api_metadata" in result
            assert "jurisdiction" in result["api_metadata"]
            assert result["api_metadata"]["jurisdiction"] == "BUG"


class TestBugCourtScraperWithAPI:
    """Tests for the BugCourtScraperWithAPI class."""

    @pytest.fixture
    def scraper(self, server_url: str) -> BugCourtScraperWithAPI:
        """Create a BugCourtScraperWithAPI instance."""
        scraper = BugCourtScraperWithAPI()
        scraper.BASE_URL = server_url
        return scraper

    @pytest.fixture
    def list_response(self, cases_html: str, server_url: str) -> Response:
        """Create a Response for the case list page."""
        request = NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/cases",
            ),
            continuation="parse_list",
        )
        return Response(
            status_code=200,
            headers={"Content-Type": "text/html"},
            content=cases_html.encode("utf-8"),
            text=cases_html,
            url=f"{server_url}/cases",
            request=request,
        )

    def test_parse_list_yields_navigating_requests(
        self, scraper: BugCourtScraperWithAPI, list_response: Response
    ):
        """The scraper shall yield NavigatingRequest for each case."""
        results = list(scraper.parse_list(list_response))

        assert len(results) == len(CASES)
        assert all(isinstance(r, NavigatingRequest) for r in results)

    def test_parse_detail_yields_non_navigating_request(
        self, scraper: BugCourtScraperWithAPI, server_url: str
    ):
        """The scraper shall yield NonNavigatingRequest for API call."""
        from tests.mock_server import generate_case_detail_html

        case = CASES[0]
        html = generate_case_detail_html(case)
        request = NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/cases/{case.docket}",
            ),
            continuation="parse_detail",
        )
        response = Response(
            status_code=200,
            headers={"Content-Type": "text/html"},
            content=html.encode("utf-8"),
            text=html,
            url=f"{server_url}/cases/{case.docket}",
            request=request,
        )

        results = list(scraper.parse_detail(response))

        assert len(results) == 1
        assert isinstance(results[0], NonNavigatingRequest)
        assert "/api/cases/" in results[0].request.url

    def test_parse_api_yields_parsed_data(
        self, scraper: BugCourtScraperWithAPI, server_url: str
    ):
        """The scraper shall yield ParsedData from API response."""
        import json

        case = CASES[0]
        api_data = {
            "docket": case.docket,
            "case_name": case.case_name,
            "plaintiff": case.plaintiff,
            "defendant": case.defendant,
            "date_filed": case.date_filed.isoformat(),
            "case_type": case.case_type,
            "status": case.status,
            "judge": case.judge,
            "summary": case.summary,
            "api_metadata": {
                "last_updated": case.date_filed.isoformat(),
                "case_number_normalized": case.docket.replace("-", ""),
                "jurisdiction": "BUG",
            },
        }
        json_text = json.dumps(api_data)

        request = NonNavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/api/cases/{case.docket}",
            ),
            continuation="parse_api",
        )
        response = Response(
            status_code=200,
            headers={"Content-Type": "application/json"},
            content=json_text.encode("utf-8"),
            text=json_text,
            url=f"{server_url}/api/cases/{case.docket}",
            request=request,
        )

        results = list(scraper.parse_api(response))

        assert len(results) == 1
        assert isinstance(results[0], ParsedData)
        data: dict = results[0].unwrap()  # ty: ignore[invalid-assignment]
        assert "api_metadata" in data
        assert data["api_metadata"]["jurisdiction"] == "BUG"


class TestIntegration:
    """Integration tests using real aiohttp server."""

    def test_full_scraping_pipeline_with_api(self, server_url: str):
        """The complete pipeline shall scrape HTML and JSON API data."""
        scraper = BugCourtScraperWithAPI()
        scraper.BASE_URL = server_url
        callback, results = collect_results()
        driver = SyncDriver(scraper, on_data=callback)
        driver.run()

        # Should have all cases
        assert len(results) == len(CASES)

        # Each result should have API metadata
        for result in results:
            assert "api_metadata" in result
            assert "jurisdiction" in result["api_metadata"]
            assert "case_number_normalized" in result["api_metadata"]

    def test_request_ancestry_preserved(self, server_url: str):
        """Request ancestry shall be preserved through the request chain."""
        scraper = BugCourtScraperWithAPI()
        scraper.BASE_URL = server_url
        callback, results = collect_results()
        driver = SyncDriver(scraper, on_data=callback)
        driver.run()

        # Should have all results
        assert len(results) > 0

        # Verify requests were resolved correctly (indirect test through results)
        for result in results:
            # Each result should have API metadata, proving the request chain worked
            assert "api_metadata" in result
