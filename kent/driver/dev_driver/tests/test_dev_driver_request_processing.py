"""Tests for request processing: status marking, data storage, validation."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any

import sqlalchemy as sa


class TestRequestStatusMarking:
    """Tests for completed and failed request status marking."""

    async def test_successful_request_marked_completed(
        self, db_path: Path
    ) -> None:
        """Test that successful requests are marked as completed."""

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            TestRequestManager,
            create_html_response,
        )

        class SimpleScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/page",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response) -> Generator[None, None, None]:
                yield None

        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/page",
            create_html_response("<html>Success</html>"),
        )

        scraper = SimpleScraper()
        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            await driver.run()

            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT status, completed_at FROM requests WHERE url = 'https://example.com/page'"
                    )
                )
                row = result.first()

            assert row is not None
            status, completed_at = row
            assert status == "completed", (
                f"Expected 'completed', got '{status}'"
            )
            assert completed_at is not None, "completed_at should be set"

    async def test_failed_request_marked_failed(self, db_path: Path) -> None:
        """Test that requests with errors are marked as failed."""

        from kent.common.exceptions import (
            HTMLStructuralAssumptionException,
        )
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            TestRequestManager,
            create_html_response,
        )

        class FailingScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/fail",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response) -> Generator[None, None, None]:
                # Simulate a structural error in parsing
                raise HTMLStructuralAssumptionException(
                    selector=".missing-element",
                    selector_type="css",
                    description="Element not found",
                    expected_min=1,
                    expected_max=None,
                    actual_count=0,
                    request_url=response.url,
                )

        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/fail",
            create_html_response("<html>No element here</html>"),
        )

        scraper = FailingScraper()
        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            await driver.run()

            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT status, last_error FROM requests WHERE url = 'https://example.com/fail'"
                    )
                )
                row = result.first()

            assert row is not None
            status, last_error = row
            assert status == "failed", f"Expected 'failed', got '{status}'"
            assert last_error is not None, "last_error should be set"
            assert "missing-element" in last_error


class TestDataStorage:
    """Tests for data storage in database."""

    async def test_parsed_data_stored_in_results(self, db_path: Path) -> None:
        """Test that ParsedData is correctly stored in results table."""
        from typing import Any

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            TestRequestManager,
            create_html_response,
        )

        # Use dicts instead of dataclasses since they're directly JSON serializable
        class DataScraper(BaseScraper[dict[str, Any]]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/case",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(
                self, response: Response
            ) -> Generator[ParsedData[dict[str, Any]], None, None]:
                # Yield some parsed data as dicts
                yield ParsedData(
                    {
                        "case_id": "2024-CV-001",
                        "title": "Smith v. Jones",
                        "date": "2024-01-15",
                    }
                )
                yield ParsedData(
                    {
                        "case_id": "2024-CV-002",
                        "title": "Doe v. Roe",
                        "date": "2024-02-20",
                    }
                )

        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/case",
            create_html_response("<html>Case data</html>"),
        )

        scraper = DataScraper()
        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            await driver.run()

            # Check results were stored
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT result_type, data_json, is_valid FROM results ORDER BY id"
                    )
                )
                rows = result.all()

            assert len(rows) == 2, f"Expected 2 results, got {len(rows)}"

            # Verify first result
            result_type, data_json, is_valid = rows[0]
            assert result_type == "dict"
            assert is_valid == 1
            assert "2024-CV-001" in data_json
            assert "Smith v. Jones" in data_json

            # Verify second result
            result_type, data_json, is_valid = rows[1]
            assert result_type == "dict"
            assert "2024-CV-002" in data_json


class TestHeadersOnlyResponse:
    """Tests for responses with headers but no body content."""

    async def test_headers_only_response_storage(self, db_path: Path) -> None:
        """Test storing and retrieving a response with no body (headers only)."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        class SimpleScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.HEAD,
                        url="https://example.com/resource",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response):
                # Just consume the HEAD response (no body)
                return []

        scraper = SimpleScraper()
        request_manager = TestRequestManager()

        # Add a mock response with no content (headers only)
        request_manager.add_response(
            "https://example.com/resource",
            MockResponse(
                content=b"",  # Empty body
                status_code=200,
                headers={
                    "Content-Length": "12345",
                    "Content-Type": "application/pdf",
                    "Last-Modified": "Wed, 21 Oct 2025 07:28:00 GMT",
                },
            ),
        )

        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            await driver.run()

            # Check the response was stored
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("""
                    SELECT response_status_code, response_headers_json, content_size_original, content_size_compressed
                    FROM requests
                    WHERE response_url = 'https://example.com/resource'
                    """)
                )
                row = result.first()

            assert row is not None, "Response should be stored"
            status_code, headers_json, size_original, size_compressed = row

            assert status_code == 200
            assert size_original == 0, (
                "Original size should be 0 for headers-only"
            )
            assert size_compressed == 0, (
                "Compressed size should be 0 for headers-only"
            )

            # Verify headers are stored correctly
            headers = json.loads(headers_json)
            assert headers["Content-Length"] == "12345"
            assert headers["Content-Type"] == "application/pdf"
            assert headers["Last-Modified"] == "Wed, 21 Oct 2025 07:28:00 GMT"

            # Verify we can retrieve the (empty) content
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT id FROM requests WHERE response_url = 'https://example.com/resource'"
                    )
                )
                resp_row = result.first()
            assert resp_row is not None
            response_id = resp_row[0]

            content = await driver.get_response_content(response_id)
            assert content == b"", (
                "Headers-only response should have empty content"
            )


class TestDeferredValidationHandling:
    """Tests for valid and invalid data handling with DeferredValidation."""

    async def test_valid_deferred_validation_stored_and_callback_called(
        self, db_path: Path
    ) -> None:
        """Test that valid DeferredValidation data is stored and on_data called."""
        from pydantic import BaseModel

        from kent.common.deferred_validation import (
            DeferredValidation,
        )
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        class CaseData(BaseModel):
            case_name: str
            docket_number: str
            court: str

            @classmethod
            def raw(cls, **data: Any) -> DeferredValidation[CaseData]:
                return DeferredValidation(cls, **data)

        received_data: list[CaseData] = []

        class ValidDataScraper(BaseScraper[CaseData]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/case",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response):
                # Yield valid deferred validation
                yield ParsedData(
                    CaseData.raw(
                        case_name="Smith v. Jones",
                        docket_number="2024-CV-001",
                        court="Supreme Court",
                    )
                )

        async def collect_result(data: CaseData) -> None:
            received_data.append(data)

        scraper = ValidDataScraper()
        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/case",
            MockResponse(
                content=b"<html>Case details</html>", status_code=200
            ),
        )

        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            driver.on_data = collect_result
            await driver.run()

            # Verify on_data was called with validated data
            assert len(received_data) == 1
            assert isinstance(received_data[0], CaseData)
            assert received_data[0].case_name == "Smith v. Jones"
            assert received_data[0].docket_number == "2024-CV-001"

            # Verify result stored as valid in database
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT is_valid, data_json FROM results")
                )
                row = result.first()
            assert row is not None
            is_valid, data_json = row
            assert is_valid == 1, "Result should be marked as valid"
            data = json.loads(data_json)
            assert data["case_name"] == "Smith v. Jones"

    async def test_invalid_deferred_validation_stored_as_invalid(
        self, db_path: Path
    ) -> None:
        """Test that invalid DeferredValidation data is stored with is_valid=False."""
        from pydantic import BaseModel, field_validator

        from kent.common.deferred_validation import (
            DeferredValidation,
        )
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        class StrictCaseData(BaseModel):
            case_name: str
            docket_number: str  # Required field

            @field_validator("docket_number")
            @classmethod
            def validate_docket(cls, v: str) -> str:
                if not v or len(v) < 5:
                    raise ValueError(
                        "Docket number must be at least 5 characters"
                    )
                return v

            @classmethod
            def raw(cls, **data: Any) -> DeferredValidation[StrictCaseData]:
                return DeferredValidation(cls, **data)

        invalid_data_received: list[Any] = []

        class InvalidDataScraper(BaseScraper[StrictCaseData]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/case",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response):
                # Yield INVALID deferred validation (docket too short)
                yield ParsedData(
                    StrictCaseData.raw(
                        case_name="Smith v. Jones",
                        docket_number="123",  # Too short, will fail validation
                    )
                )

        async def collect_invalid(data: Any) -> None:
            invalid_data_received.append(data)

        scraper = InvalidDataScraper()
        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/case",
            MockResponse(
                content=b"<html>Case details</html>", status_code=200
            ),
        )

        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            driver.on_invalid_data = collect_invalid
            await driver.run()

            # Verify on_invalid_data was called
            assert len(invalid_data_received) == 1

            # Verify result stored as invalid in database
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT is_valid, validation_errors_json, data_json FROM results"
                    )
                )
                row = result.first()
            assert row is not None
            is_valid, validation_errors_json, data_json = row

            assert is_valid == 0, "Result should be marked as invalid"
            assert validation_errors_json is not None, (
                "Should have validation errors"
            )

            # Verify the validation errors contain the expected message
            errors = json.loads(validation_errors_json)
            assert len(errors) > 0
            # The failed doc should still be stored
            data = json.loads(data_json)
            assert data["case_name"] == "Smith v. Jones"


class TestNonNavigatingHandling:
    """Tests for non-navigating Request handling by DevDriver."""

    async def test_non_navigating_request_processed(
        self, db_path: Path
    ) -> None:
        """Test that non-navigating Requests are processed without updating location."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        collected_data: list[dict] = []

        class NonNavScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/main",
                    ),
                    continuation="parse_main",
                    current_location="",
                )

            def parse_main(self, response: Response):
                # Yield a non-navigating Request for auxiliary data
                yield Request(
                    nonnavigating=True,
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/api/metadata",
                    ),
                    continuation="parse_metadata",
                    current_location=response.url,  # Keep same location
                    aux_data={"source": "main_page"},
                )

            def parse_metadata(self, response: Response):
                yield ParsedData(
                    {
                        "metadata": "fetched",
                        "source_location": response.request.current_location,
                        "aux_source": response.request.aux_data.get("source"),
                    }
                )

        async def collect_result(data: dict) -> None:
            collected_data.append(data)

        scraper = NonNavScraper()
        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/main",
            MockResponse(content=b"<html>Main page</html>", status_code=200),
        )
        request_manager.add_response(
            "https://example.com/api/metadata",
            MockResponse(
                content=b'{"status": "ok"}',
                status_code=200,
                headers={"Content-Type": "application/json"},
            ),
        )

        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            driver.on_data = collect_result
            await driver.run()

            # Verify data was collected
            assert len(collected_data) == 1
            assert collected_data[0]["metadata"] == "fetched"
            assert collected_data[0]["aux_source"] == "main_page"

            # Verify both requests are tracked in the database
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT url, request_type FROM requests ORDER BY id"
                    )
                )
                rows = result.all()

            assert len(rows) == 2
            # First request is navigating (entry point)
            assert rows[0][0] == "https://example.com/main"
            assert rows[0][1] == "navigating"
            # Second request is non-navigating
            assert rows[1][0] == "https://example.com/api/metadata"
            assert rows[1][1] == "non_navigating"

    async def test_non_navigating_request_preserves_accumulated_data(
        self, db_path: Path
    ) -> None:
        """Test that non-navigating Request preserves accumulated_data from parent."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        class AccumulatingScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/listing",
                    ),
                    continuation="parse_listing",
                    current_location="",
                    accumulated_data={"items": []},
                )

            def parse_listing(self, response: Response):
                # Add to accumulated data and fetch details
                accumulated = response.request.accumulated_data
                accumulated["items"].append("item1")

                yield Request(
                    nonnavigating=True,
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/detail/1",
                    ),
                    continuation="parse_detail",
                    current_location=response.url,
                    accumulated_data=accumulated,  # Pass accumulated data
                )

            def parse_detail(self, response: Response):
                accumulated = response.request.accumulated_data
                yield ParsedData(
                    {
                        "accumulated_items": accumulated["items"],
                        "detail_fetched": True,
                    }
                )

        results: list[dict] = []

        async def collect_result(data: dict) -> None:
            results.append(data)

        scraper = AccumulatingScraper()
        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/listing",
            MockResponse(content=b"<html>Listing</html>", status_code=200),
        )
        request_manager.add_response(
            "https://example.com/detail/1",
            MockResponse(content=b"<html>Detail</html>", status_code=200),
        )

        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            driver.on_data = collect_result
            await driver.run()

            # Verify accumulated data was preserved
            assert len(results) == 1
            assert results[0]["accumulated_items"] == ["item1"]
            assert results[0]["detail_fetched"] is True
