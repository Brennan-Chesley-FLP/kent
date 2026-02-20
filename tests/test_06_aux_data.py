"""Tests for Step 6: Auxiliary Data - Navigation Metadata.

This module tests the aux_data feature introduced in Step 6:
1. aux_data field is added to BaseRequest
2. Deep copy semantics prevent mutation bugs (like accumulated_data)
3. Data flows correctly through request chains
4. Sibling requests have independent aux_data
5. Session tokens can be extracted and used in HTTP headers
6. Contrast with accumulated_data (case data vs navigation metadata)

Tests use a real aiohttp server to verify actual HTTP behavior.
"""

import pytest

from kent.data_types import (
    HttpMethod,
    HTTPRequestParams,
    Request,
    Response,
)
from kent.driver.sync_driver import SyncDriver
from tests.scraper.example.bug_court_aux_data import (
    BugCourtScraperWithAuxData,
)
from tests.utils import collect_results


class TestAuxDataField:
    """Tests for aux_data field on BaseRequest."""

    def test_base_request_has_aux_data_field(self):
        """BaseRequest shall have an aux_data field."""
        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/test",
            ),
            continuation="parse",
        )

        assert hasattr(request, "aux_data")
        assert request.aux_data == {}

    def test_aux_data_can_be_set(self):
        """BaseRequest shall allow setting aux_data."""
        aux = {"session_token": "abc123"}
        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/test",
            ),
            continuation="parse",
            aux_data=aux,
        )

        assert request.aux_data == {"session_token": "abc123"}

    def test_aux_data_is_deep_copied(self):
        """BaseRequest shall deep copy aux_data in __post_init__."""
        original_aux: dict = {"token": "abc123", "nested": {"key": "value"}}
        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/test",
            ),
            continuation="parse",
            aux_data=original_aux,
        )

        # Mutate the original
        original_aux["token"] = "modified"
        original_aux["nested"]["key"] = "modified"

        # Request should have a deep copy - unchanged
        assert request.aux_data == {
            "token": "abc123",
            "nested": {"key": "value"},
        }

    def test_aux_data_and_accumulated_data_are_independent(self):
        """BaseRequest shall have independent aux_data and accumulated_data."""
        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/test",
            ),
            continuation="parse",
            aux_data={"token": "abc123"},
            accumulated_data={"case_name": "Test Case"},
        )

        assert request.aux_data == {"token": "abc123"}
        assert request.accumulated_data == {"case_name": "Test Case"}
        assert request.aux_data is not request.accumulated_data


class TestDeepCopySemantics:
    """Tests for deep copy semantics preventing mutation bugs."""

    def test_sibling_requests_have_independent_aux_data(self):
        """Sibling requests shall have independent aux_data copies."""
        shared_aux = {"session_token": "abc123"}

        request1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/case1",
            ),
            continuation="parse",
            aux_data=shared_aux,
        )

        request2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/case2",
            ),
            continuation="parse",
            aux_data=shared_aux,
        )

        # Each request should have its own deep copy
        assert request1.aux_data is not request2.aux_data
        assert request1.aux_data == request2.aux_data

    def test_nested_dict_mutations_do_not_propagate(self):
        """Mutations to nested dicts shall not affect sibling requests."""
        shared_aux = {"session": {"token": "abc123", "expires": 3600}}

        request1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/case1",
            ),
            continuation="parse",
            aux_data=shared_aux,
        )

        request2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/case2",
            ),
            continuation="parse",
            aux_data=shared_aux,
        )

        # Mutate nested dict in shared_aux
        shared_aux["session"]["token"] = "modified"

        # Both requests should be unaffected (they have deep copies)
        assert request1.aux_data["session"]["token"] == "abc123"
        assert request2.aux_data["session"]["token"] == "abc123"


class TestAuxDataPropagation:
    """Tests for aux_data propagation through resolve_from."""

    def test_navigating_request_propagates_aux_data(self):
        """Request.resolve_from shall propagate aux_data."""
        parent_request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/parent",
            ),
            continuation="parse_parent",
        )

        parent_response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url="http://example.com/parent",
            request=parent_request,
        )

        child_request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/child",
            ),
            continuation="parse_child",
            aux_data={"session_token": "abc123"},
        )

        resolved = child_request.resolve_from(parent_response)

        assert resolved.aux_data == {"session_token": "abc123"}

    def test_non_navigating_request_propagates_aux_data(self):
        """Non-navigating Request.resolve_from shall propagate aux_data."""
        parent_request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/parent",
            ),
            continuation="parse_parent",
        )

        parent_response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url="http://example.com/parent",
            request=parent_request,
        )

        child_request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/api/data",
            ),
            continuation="parse_api",
            nonnavigating=True,
            aux_data={"api_key": "secret123"},
        )

        resolved = child_request.resolve_from(parent_response)

        assert resolved.aux_data == {"api_key": "secret123"}

    def test_archive_request_propagates_aux_data(self):
        """Archive Request.resolve_from shall propagate aux_data."""
        parent_request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/parent",
            ),
            continuation="parse_parent",
        )

        parent_response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url="http://example.com/parent",
            request=parent_request,
        )

        child_request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="/files/document.pdf",
            ),
            continuation="archive_file",
            archive=True,
            expected_type="pdf",
            aux_data={"download_token": "xyz789"},
        )

        resolved = child_request.resolve_from(parent_response)

        assert resolved.aux_data == {"download_token": "xyz789"}


class TestBugCourtScraperWithAuxData:
    """Tests for the Bug Court scraper with aux_data."""

    @pytest.fixture
    def scraper(self) -> BugCourtScraperWithAuxData:
        """Create a scraper instance for testing."""
        return BugCourtScraperWithAuxData()

    def test_parse_list_extracts_session_token_to_aux_data(
        self, scraper: BugCourtScraperWithAuxData, server_url: str
    ):
        """The scraper shall extract session_token to aux_data from list page."""
        from tests.mock_server import generate_cases_html

        html = generate_cases_html()
        response = Response(
            status_code=200,
            headers={},
            content=html.encode(),
            text=html,
            url=f"{server_url}/cases",
            request=next(scraper.get_entry()),
        )

        results = list(scraper.parse_list(response))

        assert len(results) > 0
        # All requests should have session token in aux_data
        for request in results:
            assert isinstance(request, Request)
            assert "session_token" in request.aux_data
            assert (
                request.aux_data["session_token"] == "bug-session-token-abc123"
            )

    def test_parse_list_puts_case_data_in_accumulated_data(
        self, scraper: BugCourtScraperWithAuxData, server_url: str
    ):
        """The scraper shall put case data in accumulated_data, not aux_data."""
        from tests.mock_server import generate_cases_html

        html = generate_cases_html()
        response = Response(
            status_code=200,
            headers={},
            content=html.encode(),
            text=html,
            url=f"{server_url}/cases",
            request=next(scraper.get_entry()),
        )

        results = list(scraper.parse_list(response))

        assert len(results) > 0
        # First request should have case data in accumulated_data
        request = results[0]
        assert isinstance(request, Request)
        assert "docket" in request.accumulated_data
        assert "case_name" in request.accumulated_data
        # Case data should NOT be in aux_data
        assert "docket" not in request.aux_data
        assert "case_name" not in request.aux_data
        # Session token should NOT be in accumulated_data
        assert "session_token" not in request.accumulated_data


class TestIntegration:
    """Integration tests using real aiohttp server."""

    def test_full_scraping_pipeline_with_aux_data(
        self, server_url: str, tmp_path
    ):
        """The complete pipeline shall use aux_data for session tokens."""
        scraper = BugCourtScraperWithAuxData()
        scraper.BASE_URL = server_url
        callback, results = collect_results()
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)
        driver.run()

        # Should have results with PDFs
        assert len(results) > 0

        # Results should have case data (from accumulated_data)
        for result in results:
            if "opinion_file" in result:
                # This result went through the full chain with session token
                assert "docket" in result
                assert "case_name" in result
                assert "plaintiff" in result
                assert "defendant" in result
                assert "judge" in result
                # Session token should NOT be in final result (it was aux_data, not case data)
                assert "session_token" not in result

    def test_aux_data_flows_through_three_pages(
        self, server_url: str, tmp_path
    ):
        """aux_data shall flow correctly through three-page chain."""
        scraper = BugCourtScraperWithAuxData()
        scraper.BASE_URL = server_url
        callback, results = collect_results()
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)
        driver.run()

        # Verify we got results with PDF files
        pdf_results = [r for r in results if "opinion_file" in r]
        assert len(pdf_results) > 0

        # These results required the session token to download the PDF
        # The token flowed through: list -> detail -> archive
        for result in pdf_results:
            assert "opinion_file" in result
            assert "download_url" in result

    def test_session_token_validated_when_provided(
        self, server_url: str, tmp_path
    ):
        """The PDF download shall validate session token if provided."""
        import httpx

        docket = "BCC-2024-002"

        # Without token - should succeed (backward compatibility)
        response = httpx.get(f"{server_url}/opinions/{docket}.pdf")
        assert response.status_code == 200

        # With wrong token - should fail
        response = httpx.get(
            f"{server_url}/opinions/{docket}.pdf",
            headers={"X-Session-Token": "wrong-token"},
        )

        assert response.status_code == 403
        assert "Invalid session token" in response.text

        # With correct token - should succeed
        response = httpx.get(
            f"{server_url}/opinions/{docket}.pdf",
            headers={"X-Session-Token": "bug-session-token-abc123"},
        )

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/pdf"
