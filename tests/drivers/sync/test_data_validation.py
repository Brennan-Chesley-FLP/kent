"""Tests for Step 9: Data Validation Errors.

This module tests the data validation error handling introduced in Step 9:
1. DataFormatAssumptionException for schema mismatches
2. Pydantic model validation in scrapers
3. DeferredValidation for delayed validation
4. Integration with scraper flow

Tests use a real aiohttp server to verify actual HTTP behavior.
"""

from datetime import date

import pytest
from pydantic import ValidationError

from kent.common.deferred_validation import (
    DeferredValidation,
)
from kent.common.exceptions import (
    DataFormatAssumptionException,
)
from tests.scraper.example.bug_court import (
    BugCourtCaseData,
)


class TestDataFormatAssumptionException:
    """Tests for DataFormatAssumptionException."""

    def test_exception_has_required_attributes(self):
        """DataFormatAssumptionException shall have errors, failed_doc, and model_name attributes."""
        errors = [
            {
                "loc": ("field1",),
                "msg": "field required",
                "type": "value_error",
            }
        ]
        failed_doc = {"field2": "value"}

        exc = DataFormatAssumptionException(
            errors=errors,
            failed_doc=failed_doc,
            model_name="TestModel",
            request_url="http://example.com/test",
        )

        assert exc.errors == errors
        assert exc.failed_doc == failed_doc
        assert exc.model_name == "TestModel"
        assert exc.request_url == "http://example.com/test"

    def test_exception_formats_message_with_error_summary(self):
        """DataFormatAssumptionException shall include error summary in message."""
        errors = [
            {
                "loc": ("field1",),
                "msg": "field required",
                "type": "value_error",
            },
            {
                "loc": ("field2",),
                "msg": "invalid value",
                "type": "value_error",
            },
        ]

        exc = DataFormatAssumptionException(
            errors=errors,
            failed_doc={},
            model_name="TestModel",
            request_url="http://example.com/test",
        )

        formatted = str(exc)
        assert "TestModel" in formatted
        assert "field1: field required" in formatted
        assert "field2: invalid value" in formatted

    def test_exception_includes_context_dict(self):
        """DataFormatAssumptionException shall include validation details in context."""
        errors = [
            {
                "loc": ("field1",),
                "msg": "field required",
                "type": "value_error",
            }
        ]
        failed_doc = {"field2": "value"}

        exc = DataFormatAssumptionException(
            errors=errors,
            failed_doc=failed_doc,
            model_name="TestModel",
            request_url="http://example.com/test",
        )

        assert exc.context["model"] == "TestModel"
        assert exc.context["error_count"] == 1
        assert exc.context["errors"] == errors
        assert exc.context["failed_doc"] == failed_doc


class TestPydanticValidation:
    """Tests for Pydantic model validation."""

    def test_valid_data_passes_validation(self):
        """BugCourtCaseData shall accept valid data."""
        data = {
            "docket": "BCC-2024-001",
            "case_name": "Ant v. Grasshopper",
            "plaintiff": "Ant",
            "defendant": "Grasshopper",
            "date_filed": date(2024, 1, 15),
            "case_type": "Civil",
            "status": "Open",
            "judge": "Judge Mantis",
            "court_reporter": "Reporter Bee",
        }

        case = BugCourtCaseData(**data)  # ty: ignore[invalid-argument-type]
        assert case.docket == "BCC-2024-001"
        assert case.case_name == "Ant v. Grasshopper"

    def test_missing_required_field_raises_validation_error(self):
        """BugCourtCaseData shall raise ValidationError for missing required fields."""
        data = {
            "docket": "BCC-2024-001",
            # Missing case_name and other required fields
        }

        with pytest.raises(ValidationError) as exc_info:
            BugCourtCaseData(**data)  # ty: ignore[invalid-argument-type]

        errors = exc_info.value.errors()
        assert len(errors) > 0
        # Should have errors for missing fields
        missing_fields = {err["loc"][0] for err in errors}
        assert "case_name" in missing_fields

    def test_invalid_field_type_raises_validation_error(self):
        """BugCourtCaseData shall raise ValidationError for invalid field types."""
        data = {
            "docket": "BCC-2024-001",
            "case_name": "Ant v. Grasshopper",
            "plaintiff": "Ant",
            "defendant": "Grasshopper",
            "date_filed": "not-a-date",  # Should be date object
            "case_type": "Civil",
            "status": "Open",
            "judge": "Judge Mantis",
            "court_reporter": "Reporter Bee",
        }

        with pytest.raises(ValidationError) as exc_info:
            BugCourtCaseData(**data)  # ty: ignore[invalid-argument-type]

        errors = exc_info.value.errors()
        assert any(err["loc"][0] == "date_filed" for err in errors)


class TestDeferredValidation:
    """Tests for DeferredValidation class."""

    def test_deferred_validation_stores_data_and_model(self):
        """DeferredValidation shall store raw data and model type."""
        deferred = DeferredValidation(
            BugCourtCaseData,
            "http://example.com",
            docket="BCC-2024-001",
            case_name="Test Case",
        )

        assert deferred.raw_data["docket"] == "BCC-2024-001"
        assert deferred.raw_data["case_name"] == "Test Case"
        assert deferred.model_name == "BugCourtCaseData"

    def test_confirm_validates_valid_data(self):
        """DeferredValidation.confirm() shall validate and return model for valid data."""
        deferred = DeferredValidation(
            BugCourtCaseData,
            "http://example.com",
            docket="BCC-2024-001",
            case_name="Ant v. Grasshopper",
            plaintiff="Ant",
            defendant="Grasshopper",
            date_filed=date(2024, 1, 15),
            case_type="Civil",
            status="Open",
            judge="Judge Mantis",
            court_reporter="Reporter Bee",
        )
        validated = deferred.confirm()

        assert isinstance(validated, BugCourtCaseData)
        assert validated.docket == "BCC-2024-001"

    def test_confirm_raises_data_format_exception_for_invalid_data(self):
        """DeferredValidation.confirm() shall raise DataFormatAssumptionException for invalid data."""
        deferred = DeferredValidation(
            BugCourtCaseData,
            "http://example.com",
            docket="BCC-2024-001",  # Missing required fields
        )

        with pytest.raises(DataFormatAssumptionException) as exc_info:
            deferred.confirm()

        exc = exc_info.value
        assert exc.model_name == "BugCourtCaseData"
        assert exc.failed_doc["docket"] == "BCC-2024-001"
        assert len(exc.errors) > 0

    def test_raw_data_returns_copy(self):
        """DeferredValidation.raw_data shall return a copy of the data."""
        deferred = DeferredValidation(
            BugCourtCaseData,
            "http://example.com",
            docket="BCC-2024-001",
        )

        raw = deferred.raw_data
        raw["modified"] = "value"

        # Original data should not be modified
        assert "modified" not in deferred.raw_data


class TestIntegrationWithScraper:
    """Integration tests for data validation in scrapers."""

    def test_scraper_validates_and_yields_valid_data(
        self, server_url: str, tmp_path
    ):
        """The driver shall validate data and yield it when valid."""
        from kent.driver.sync_driver import SyncDriver
        from tests.scraper.example.bug_court import (
            BugCourtCaseData,
            BugCourtScraperWithValidation,
        )
        from tests.utils import collect_results

        scraper = BugCourtScraperWithValidation()
        scraper.BASE_URL = server_url

        callback, results = collect_results()
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)

        # Should complete without exceptions
        driver.run()

        # Should have validated results
        assert len(results) > 0
        # Results should be validated BugCourtCaseData models
        assert all(isinstance(r, BugCourtCaseData) for r in results)
        assert all(r.docket for r in results)
        assert all(r.case_name for r in results)

    def test_driver_raises_exception_for_invalid_data(
        self, server_url: str, tmp_path
    ):
        """The driver shall raise DataFormatAssumptionException for invalid deferred data."""
        from collections.abc import Generator

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver

        # Create a scraper that yields deferred validation with invalid data
        class InvalidDataScraper(BaseScraper[BugCourtCaseData]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001",
                    ),
                    continuation="parse_invalid",
                )

            def parse_invalid(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                # Yield deferred validation with invalid data (missing required fields)
                yield ParsedData(
                    BugCourtCaseData.raw(
                        request_url=response.url,
                        docket="BCC-2024-001",  # Missing case_name, etc.
                    )
                )

        scraper = InvalidDataScraper()
        # No callbacks - exception should propagate
        driver = SyncDriver(scraper, storage_dir=tmp_path)

        # Should raise DataFormatAssumptionException when driver validates
        with pytest.raises(DataFormatAssumptionException) as exc_info:
            driver.run()

        exc = exc_info.value
        assert exc.model_name == "BugCourtCaseData"
        assert exc.failed_doc["docket"] == "BCC-2024-001"
        assert len(exc.errors) > 0

    def test_on_invalid_data_callback_receives_invalid_data(
        self, server_url: str, tmp_path
    ):
        """The driver shall call on_invalid_data callback when validation fails."""
        from collections.abc import Generator

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
        from kent.driver.sync_driver import SyncDriver

        # Create a scraper that yields deferred validation with invalid data
        class InvalidDataScraper(BaseScraper[BugCourtCaseData]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001",
                    ),
                    continuation="parse_invalid",
                )

            def parse_invalid(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                # Yield deferred validation with invalid data
                yield ParsedData(
                    BugCourtCaseData.raw(
                        request_url=response.url,
                        docket="BCC-2024-001",  # Missing required fields
                    )
                )

        scraper = InvalidDataScraper()
        invalid_results = []

        def on_invalid_data(data):
            invalid_results.append(data)

        driver = SyncDriver(
            scraper, storage_dir=tmp_path, on_invalid_data=on_invalid_data
        )

        # Should not raise - invalid data goes to callback
        driver.run()

        # Should have received invalid data in callback
        assert len(invalid_results) == 1
        assert isinstance(invalid_results[0], DeferredValidation)
        assert invalid_results[0].raw_data["docket"] == "BCC-2024-001"

    def test_default_invalid_data_callback_logs_error(
        self, server_url: str, tmp_path, caplog
    ):
        """The default log_and_validate_invalid_data callback shall log validation errors."""
        from collections.abc import Generator

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.sync_driver import (
            SyncDriver,
            log_and_validate_invalid_data,
        )

        # Create a scraper that yields deferred validation with invalid data
        class InvalidDataScraper(BaseScraper[BugCourtCaseData]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001",
                    ),
                    continuation="parse_invalid",
                )

            def parse_invalid(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                # Yield deferred validation with invalid data
                yield ParsedData(
                    BugCourtCaseData.raw(
                        request_url=response.url,
                        docket="BCC-2024-001",  # Missing required fields
                    )
                )

        scraper = InvalidDataScraper()
        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_invalid_data=log_and_validate_invalid_data,
        )

        # Run should not raise - callback handles the error
        driver.run()

        # Should have logged the error
        assert len(caplog.records) > 0
        # Find the error log record
        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(error_records) > 0
        log_record = error_records[0]
        assert (
            "Data validation failed for model 'BugCourtCaseData'"
            in log_record.message
        )
        assert log_record.model_name == "BugCourtCaseData"
        assert log_record.error_count > 0
