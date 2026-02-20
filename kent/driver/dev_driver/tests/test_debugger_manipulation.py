"""Tests for LocalDevDriverDebugger write operations and related features.

Tests for read-only mode enforcement, manipulation methods (cancel, requeue,
resolve), export, diagnose, response search, and seed speculative requests.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kent.driver.dev_driver.debugger import (
    LocalDevDriverDebugger,
)
from kent.driver.dev_driver.sql_manager import SQLManager


class TestReadOnlyModeEnforcement:
    """Tests for read-only mode enforcement."""

    async def test_cancel_request_read_only(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that cancel_request raises error in read-only mode."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError, match="write mode"):
                await debugger.cancel_request(1)

    async def test_cancel_requests_by_continuation_read_only(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that cancel_requests_by_continuation raises error in read-only mode."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError, match="write mode"):
                await debugger.cancel_requests_by_continuation("step1")

    async def test_requeue_request_read_only(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that requeue_request raises error in read-only mode."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError, match="write mode"):
                await debugger.requeue_request(2)

    async def test_requeue_continuation_read_only(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that requeue_continuation raises error in read-only mode."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError, match="write mode"):
                await debugger.requeue_continuation("step1")

    async def test_resolve_error_read_only(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that resolve_error raises error in read-only mode."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError, match="write mode"):
                await debugger.resolve_error(1)

    async def test_requeue_error_read_only(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that requeue_error raises error in read-only mode."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError, match="write mode"):
                await debugger.requeue_error(1)

    async def test_batch_requeue_errors_read_only(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that batch_requeue_errors raises error in read-only mode."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError, match="write mode"):
                await debugger.batch_requeue_errors(error_type="xpath")

    async def test_train_compression_dict_read_only(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that train_compression_dict raises error in read-only mode."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError, match="write mode"):
                await debugger.train_compression_dict("step1")

    async def test_recompress_responses_read_only(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that recompress_responses raises error in read-only mode."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError, match="write mode"):
                await debugger.recompress_responses("step1")


class TestManipulationMethods:
    """Tests for manipulation methods in write mode."""

    async def test_cancel_request(self, db_path: Path, populated_db) -> None:
        """Test cancelling a request."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            # Cancel a pending request
            result = await debugger.cancel_request(1)
            assert result is True

            # Verify it's marked as failed
            request = await debugger.get_request(1)
            assert request is not None
            assert request.status == "failed"

    async def test_cancel_requests_by_continuation(
        self, db_path: Path, populated_db
    ) -> None:
        """Test cancelling all requests for a continuation."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            # Cancel all pending/held requests for step2
            count = await debugger.cancel_requests_by_continuation("step2")
            assert count == 1  # Only the held request

    async def test_requeue_request_with_downstream_clear(
        self, db_path: Path, populated_db
    ) -> None:
        """Test requeuing a request with downstream cleanup."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            # Requeue a completed request
            new_id = await debugger.requeue_request(2, clear_downstream=True)
            assert new_id > 0

            # Verify new request exists
            new_request = await debugger.get_request(new_id)
            assert new_request is not None
            assert new_request.url == "https://example.com/page2"
            assert new_request.status == "pending"

    async def test_requeue_request_without_downstream_clear(
        self, db_path: Path, populated_db
    ) -> None:
        """Test requeuing a request without downstream cleanup."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            # Requeue without clearing downstream
            new_id = await debugger.requeue_request(2, clear_downstream=False)
            assert new_id > 0

            # Verify new request exists
            new_request = await debugger.get_request(new_id)
            assert new_request is not None

    async def test_requeue_continuation(
        self, db_path: Path, populated_db
    ) -> None:
        """Test requeuing all requests for a continuation."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            # Requeue all completed requests for step1
            count = await debugger.requeue_continuation(
                "step1", status="completed"
            )
            assert count == 2  # Two completed requests in step1

    async def test_resolve_error(self, db_path: Path, populated_db) -> None:
        """Test resolving an error."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            # Resolve an unresolved error
            result = await debugger.resolve_error(1, "Fixed the selector")
            assert result is True

            # Verify it's resolved
            error = await debugger.get_error(1)
            assert error is not None
            assert error["is_resolved"] is True
            assert error["resolution_notes"] == "Fixed the selector"

    async def test_requeue_error(self, db_path: Path, populated_db) -> None:
        """Test requeuing an error."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            # Requeue an error
            new_id = await debugger.requeue_error(1, "Trying again")
            assert new_id > 0

            # Verify error is resolved
            error = await debugger.get_error(1)
            assert error is not None
            assert error["is_resolved"] is True

    async def test_batch_requeue_errors(
        self, db_path: Path, populated_db
    ) -> None:
        """Test batch requeuing errors."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            # Batch requeue xpath errors
            count = await debugger.batch_requeue_errors(error_type="xpath")
            assert count == 1  # One unresolved xpath error


class TestExportMethods:
    """Tests for export methods."""

    async def test_export_results_jsonl(
        self, db_path: Path, populated_db, tmp_path: Path
    ) -> None:
        """Test exporting results to JSONL."""
        engine, _ = populated_db
        await engine.dispose()

        output_path = tmp_path / "results.jsonl"

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            count = await debugger.export_results_jsonl(output_path)

            assert count == 2
            assert output_path.exists()

            # Verify content
            lines = output_path.read_text().strip().split("\n")
            assert len(lines) == 2

            # Parse first line
            result = json.loads(lines[0])
            assert "id" in result
            assert "result_type" in result
            assert "data" in result
            assert result["result_type"] == "TestResult"

    async def test_export_results_jsonl_filtered(
        self, db_path: Path, populated_db, tmp_path: Path
    ) -> None:
        """Test exporting filtered results to JSONL."""
        engine, _ = populated_db
        await engine.dispose()

        output_path = tmp_path / "valid_results.jsonl"

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            count = await debugger.export_results_jsonl(
                output_path, is_valid=True
            )

            assert count == 1
            assert output_path.exists()

    async def test_preview_warc_export(
        self, db_path: Path, populated_db
    ) -> None:
        """Test previewing WARC export."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            preview = await debugger.preview_warc_export()

            assert "record_count" in preview
            assert "estimated_size" in preview
            assert preview["record_count"] == 2  # Two responses


class TestDiagnoseMethods:
    """Tests for diagnosis methods."""

    async def test_diagnose_error(self, db_path: Path, populated_db) -> None:
        """Test diagnosing an error."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Diagnose the xpath error
            # Note: Full diagnosis requires scraper class, so we test partial functionality
            with pytest.raises(ValueError, match="No response found"):
                # Error ID 1 is for request_id 3, which has no response
                await debugger.diagnose(1)

    async def test_diagnose_error_not_found(
        self, db_path: Path, populated_db
    ) -> None:
        """Test diagnosing a non-existent error."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            with pytest.raises(ValueError, match="Error .* not found"):
                await debugger.diagnose(9999)


class TestResponseSearch:
    """Tests for response search methods."""

    async def test_search_text_match(
        self, db_path: Path, populated_db
    ) -> None:
        """Test text search that finds matches."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            matches = await debugger.search_responses(text="Response")

            assert len(matches) == 2
            assert all("response_id" in m for m in matches)
            assert all("request_id" in m for m in matches)

    async def test_search_text_case_insensitive(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that text search is case insensitive."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            matches = await debugger.search_responses(text="RESPONSE")

            assert len(matches) == 2

    async def test_search_text_no_match(
        self, db_path: Path, populated_db
    ) -> None:
        """Test text search that finds no matches."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            matches = await debugger.search_responses(text="nonexistent")

            assert len(matches) == 0

    async def test_search_regex_match(
        self, db_path: Path, populated_db
    ) -> None:
        """Test regex search that finds matches."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            matches = await debugger.search_responses(regex=r"Response \d")

            assert len(matches) == 2

    async def test_search_regex_no_match(
        self, db_path: Path, populated_db
    ) -> None:
        """Test regex search that finds no matches."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            matches = await debugger.search_responses(regex=r"Response \d{5}")

            assert len(matches) == 0

    async def test_search_xpath_match(
        self, db_path: Path, populated_db
    ) -> None:
        """Test XPath search that finds matches."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            matches = await debugger.search_responses(xpath="//html")

            assert len(matches) == 2

    async def test_search_xpath_no_match(
        self, db_path: Path, populated_db
    ) -> None:
        """Test XPath search that finds no matches."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            matches = await debugger.search_responses(
                xpath="//div[@class='nonexistent']"
            )

            assert len(matches) == 0

    async def test_search_with_continuation_filter(
        self, db_path: Path, populated_db
    ) -> None:
        """Test search with continuation filter."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Both responses are in step1
            matches = await debugger.search_responses(
                text="Response", continuation="step1"
            )

            assert len(matches) == 2

            # No responses in step2
            matches = await debugger.search_responses(
                text="Response", continuation="step2"
            )

            assert len(matches) == 0

    async def test_search_requires_exactly_one_pattern(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that exactly one search pattern must be provided."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # No pattern provided
            with pytest.raises(ValueError, match="Exactly one"):
                await debugger.search_responses()

            # Multiple patterns provided
            with pytest.raises(ValueError, match="Exactly one"):
                await debugger.search_responses(text="foo", regex="bar")

    async def test_search_returns_correct_ids(
        self, db_path: Path, populated_db
    ) -> None:
        """Test that search returns correct response and request IDs."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Search for "Response 1" - should only match first response
            matches = await debugger.search_responses(text="Response 1")

            assert len(matches) == 1
            assert matches[0]["response_id"] == 1
            # Request IDs are 2 and 5 for the two completed requests
            assert matches[0]["request_id"] == 2


class TestSeedSpeculativeRequests:
    """Tests for seed_speculative_requests method."""

    async def test_seed_speculative_requests_creates_pending_requests(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test that seed_speculative_requests creates pending requests in the database."""
        from kent.common.decorators import entry
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
        )

        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create run metadata pointing to our test scraper
        await sql_manager.init_run_metadata(
            scraper_name="test_module.TestSpeculateScraper",
            scraper_version="1.0.0",
            num_workers=1,
            max_backoff_time=60.0,
        )

        await engine.dispose()

        # Create a simple test scraper with a speculative @entry function
        from kent.common.speculation_types import SimpleSpeculation

        class TestSpeculateScraper(BaseScraper):
            @entry(dict, speculative=SimpleSpeculation(highest_observed=100))
            def fetch_item(self, item_id: int) -> Request:
                return Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"https://example.com/items/{item_id}",
                    ),
                    continuation="parse_item",
                )

        # Mock the registry
        mock_scraper_info = MagicMock()
        mock_scraper_info.module_path = "test_module.TestSpeculateScraper"
        mock_scraper_info.full_path = "test_module:TestSpeculateScraper"
        mock_scraper_info.class_name = "TestSpeculateScraper"

        mock_registry = MagicMock()
        mock_registry.list_scrapers.return_value = [mock_scraper_info]
        mock_registry.instantiate_scraper.return_value = TestSpeculateScraper()

        with patch(
            "kent.driver.dev_driver.web.scraper_registry.get_registry",
            return_value=mock_registry,
        ):
            async with LocalDevDriverDebugger.open(
                db_path, read_only=False
            ) as debugger:
                # Seed requests for IDs 1-5
                count = await debugger.seed_speculative_requests(
                    step_name="fetch_item",
                    from_id=1,
                    to_id=5,
                )

        assert count == 5

        # Verify requests are in the database
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_requests(status="pending")
            assert page.total == 5
            assert len(page.items) == 5

            # Check the URLs are correct
            urls = {r.url for r in page.items}
            expected_urls = {
                f"https://example.com/items/{i}" for i in range(1, 6)
            }
            assert urls == expected_urls

            # Verify continuation is set
            for r in page.items:
                assert r.continuation == "parse_item"

    async def test_seed_speculative_requests_requires_write_mode(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test that seed_speculative_requests fails in read-only mode."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        await sql_manager.init_run_metadata(
            scraper_name="test_module.TestScraper",
            scraper_version="1.0.0",
            num_workers=1,
            max_backoff_time=60.0,
        )
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            with pytest.raises(PermissionError):
                await debugger.seed_speculative_requests(
                    step_name="fetch_item",
                    from_id=1,
                    to_id=5,
                )

    async def test_seed_speculative_requests_fails_for_non_speculate_function(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test that seed_speculative_requests fails for non-speculative functions."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
        )

        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        await sql_manager.init_run_metadata(
            scraper_name="test_module.TestNonSpeculateScraper",
            scraper_version="1.0.0",
            num_workers=1,
            max_backoff_time=60.0,
        )
        await engine.dispose()

        # Create a scraper without speculative @entry decorator
        class TestNonSpeculateScraper(BaseScraper):
            def fetch_item(self, item_id: int) -> Request:
                return Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"https://example.com/items/{item_id}",
                    ),
                    continuation="parse_item",
                )

        mock_scraper_info = MagicMock()
        mock_scraper_info.module_path = "test_module.TestNonSpeculateScraper"
        mock_scraper_info.full_path = "test_module:TestNonSpeculateScraper"
        mock_scraper_info.class_name = "TestNonSpeculateScraper"

        mock_registry = MagicMock()
        mock_registry.list_scrapers.return_value = [mock_scraper_info]
        mock_registry.instantiate_scraper.return_value = (
            TestNonSpeculateScraper()
        )

        with patch(
            "kent.driver.dev_driver.web.scraper_registry.get_registry",
            return_value=mock_registry,
        ):
            async with LocalDevDriverDebugger.open(
                db_path, read_only=False
            ) as debugger:
                with pytest.raises(
                    ValueError, match="is not a speculative entry function"
                ):
                    await debugger.seed_speculative_requests(
                        step_name="fetch_item",
                        from_id=1,
                        to_id=5,
                    )

    async def test_seed_speculative_requests_fails_for_nonexistent_step(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test that seed_speculative_requests fails when step doesn't exist."""
        from kent.data_types import BaseScraper

        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        await sql_manager.init_run_metadata(
            scraper_name="test_module.TestEmptyScraper",
            scraper_version="1.0.0",
            num_workers=1,
            max_backoff_time=60.0,
        )
        await engine.dispose()

        class TestEmptyScraper(BaseScraper):
            pass

        mock_scraper_info = MagicMock()
        mock_scraper_info.module_path = "test_module.TestEmptyScraper"
        mock_scraper_info.full_path = "test_module:TestEmptyScraper"
        mock_scraper_info.class_name = "TestEmptyScraper"

        mock_registry = MagicMock()
        mock_registry.list_scrapers.return_value = [mock_scraper_info]
        mock_registry.instantiate_scraper.return_value = TestEmptyScraper()

        with patch(
            "kent.driver.dev_driver.web.scraper_registry.get_registry",
            return_value=mock_registry,
        ):
            async with LocalDevDriverDebugger.open(
                db_path, read_only=False
            ) as debugger:
                with pytest.raises(ValueError, match="not found on scraper"):
                    await debugger.seed_speculative_requests(
                        step_name="nonexistent_step",
                        from_id=1,
                        to_id=5,
                    )

    async def test_seed_speculative_requests_fails_for_unknown_scraper(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test that seed_speculative_requests fails when scraper not in registry."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        await sql_manager.init_run_metadata(
            scraper_name="unknown_module.UnknownScraper",
            scraper_version="1.0.0",
            num_workers=1,
            max_backoff_time=60.0,
        )
        await engine.dispose()

        mock_registry = MagicMock()
        mock_registry.list_scrapers.return_value = []  # No scrapers registered

        with patch(
            "kent.driver.dev_driver.web.scraper_registry.get_registry",
            return_value=mock_registry,
        ):
            async with LocalDevDriverDebugger.open(
                db_path, read_only=False
            ) as debugger:
                with pytest.raises(ValueError, match="not found in registry"):
                    await debugger.seed_speculative_requests(
                        step_name="fetch_item",
                        from_id=1,
                        to_id=5,
                    )
