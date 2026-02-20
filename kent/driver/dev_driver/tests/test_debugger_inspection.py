"""Tests for LocalDevDriverDebugger read-only inspection methods.

Tests for context manager, run metadata/stats, and inspection of
requests, responses, errors, results, speculation, rate limiter,
and compression.
"""

from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa

from kent.driver.dev_driver.debugger import (
    LocalDevDriverDebugger,
)


class TestDebuggerContextManager:
    """Tests for LocalDevDriverDebugger context manager."""

    async def test_open_read_only(self, db_path: Path, initialized_db) -> None:
        """Test opening debugger in read-only mode."""
        engine, session_factory = initialized_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=True
        ) as debugger:
            assert debugger.read_only is True
            assert debugger.sql is not None

    async def test_open_write_mode(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test opening debugger in write mode."""
        engine, session_factory = initialized_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            assert debugger.read_only is False
            assert debugger.sql is not None

    async def test_open_with_string_path(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test opening debugger with string path."""
        engine, session_factory = initialized_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(str(db_path)) as debugger:
            assert debugger.sql is not None


class TestRunMetadataAndStats:
    """Tests for run metadata and statistics methods."""

    async def test_get_run_metadata(self, db_path: Path, populated_db) -> None:
        """Test getting run metadata."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            metadata = await debugger.get_run_metadata()

            assert metadata is not None
            assert metadata["scraper_name"] == "test.scraper"
            assert metadata["scraper_version"] == "1.0.0"
            assert metadata["status"] == "running"
            assert metadata["base_delay"] == 0.5

    async def test_get_run_metadata_empty_db(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test getting run metadata from empty database."""
        engine, _ = initialized_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            metadata = await debugger.get_run_metadata()
            assert metadata is None

    async def test_get_run_status_running(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting run status for a running scraper."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            status = await debugger.get_run_status()

            assert status["status"] == "running"
            assert status["is_running"] is True
            assert "pending_count" in status
            # From the populated_db fixture, there's 1 pending request
            assert status["pending_count"] == 1

    async def test_get_run_status_completed(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test getting run status for a completed scraper."""
        engine, session_factory = initialized_db
        # Insert run metadata with completed status
        async with session_factory() as session:
            await session.execute(
                sa.text(
                    """
                    INSERT INTO run_metadata (
                        scraper_name, scraper_version, status, created_at,
                        base_delay, jitter, num_workers, max_backoff_time, speculation_config_json
                    ) VALUES (:scraper_name, :scraper_version, :status, datetime('now'), :base_delay, :jitter, :num_workers, :max_backoff_time, :speculation_config_json)
                    """
                ),
                {
                    "scraper_name": "test.scraper",
                    "scraper_version": "1.0.0",
                    "status": "completed",
                    "base_delay": 0.5,
                    "jitter": 0.2,
                    "num_workers": 4,
                    "max_backoff_time": 300.0,
                    "speculation_config_json": "{}",
                },
            )
            await session.commit()
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            status = await debugger.get_run_status()

            assert status["status"] == "completed"
            assert status["is_running"] is False
            # pending_count should not be present for completed runs
            assert "pending_count" not in status

    async def test_get_run_status_empty_db(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test getting run status from empty database."""
        engine, _ = initialized_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            status = await debugger.get_run_status()

            assert status["status"] == "unknown"
            assert status["is_running"] is False

    async def test_get_stats(self, db_path: Path, populated_db) -> None:
        """Test getting comprehensive statistics."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            stats = await debugger.get_stats()

            assert "queue" in stats
            assert "throughput" in stats
            assert "compression" in stats
            assert "results" in stats
            assert "errors" in stats

            # Verify queue stats
            assert stats["queue"]["total"] == 5


class TestRequestInspection:
    """Tests for request inspection methods."""

    async def test_list_requests_no_filter(
        self, db_path: Path, populated_db
    ) -> None:
        """Test listing all requests."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_requests()

            assert page.total == 5
            assert len(page.items) == 5
            assert not page.has_more

    async def test_list_requests_filter_by_status(
        self, db_path: Path, populated_db
    ) -> None:
        """Test filtering requests by status."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Get completed requests
            page = await debugger.list_requests(status="completed")
            assert page.total == 2
            assert all(req.status == "completed" for req in page.items)

            # Get pending requests
            page = await debugger.list_requests(status="pending")
            assert page.total == 1

            # Get failed requests
            page = await debugger.list_requests(status="failed")
            assert page.total == 1

    async def test_list_requests_filter_by_continuation(
        self, db_path: Path, populated_db
    ) -> None:
        """Test filtering requests by continuation."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_requests(continuation="step1")
            assert page.total == 3
            assert all(req.continuation == "step1" for req in page.items)

    async def test_list_requests_pagination(
        self, db_path: Path, populated_db
    ) -> None:
        """Test request pagination."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # First page
            page1 = await debugger.list_requests(limit=2, offset=0)
            assert len(page1.items) == 2
            assert page1.total == 5
            assert page1.has_more

            # Second page
            page2 = await debugger.list_requests(limit=2, offset=2)
            assert len(page2.items) == 2
            assert page2.has_more

            # Last page
            page3 = await debugger.list_requests(limit=2, offset=4)
            assert len(page3.items) == 1
            assert not page3.has_more

    async def test_get_request(self, db_path: Path, populated_db) -> None:
        """Test getting a single request."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            request = await debugger.get_request(1)

            assert request is not None
            assert request.id == 1
            assert request.url == "https://example.com/page1"
            assert request.continuation == "step1"

    async def test_get_request_not_found(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting a non-existent request."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            request = await debugger.get_request(9999)
            assert request is None

    async def test_get_request_summary(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting request summary."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            summary = await debugger.get_request_summary()

            assert "all" in summary
            assert "step1" in summary
            assert "step2" in summary

            # Check totals
            assert summary["all"]["completed"] == 2
            assert summary["all"]["pending"] == 1
            assert summary["all"]["failed"] == 1
            assert summary["all"]["held"] == 1


class TestResponseInspection:
    """Tests for response inspection methods."""

    async def test_list_responses(self, db_path: Path, populated_db) -> None:
        """Test listing responses."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_responses()

            assert page.total == 2
            assert len(page.items) == 2

    async def test_list_responses_filter_by_continuation(
        self, db_path: Path, populated_db
    ) -> None:
        """Test filtering responses by continuation."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_responses(continuation="step1")
            assert page.total == 2
            assert all(resp.continuation == "step1" for resp in page.items)

    async def test_get_response(self, db_path: Path, populated_db) -> None:
        """Test getting a single response."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            response = await debugger.get_response(2)

            assert response is not None
            assert response.id == 2
            assert response.status_code == 200

    async def test_get_response_content(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting decompressed response content."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            content = await debugger.get_response_content(2)

            assert content is not None
            assert b"Response 1" in content


class TestErrorInspection:
    """Tests for error inspection methods."""

    async def test_list_errors(self, db_path: Path, populated_db) -> None:
        """Test listing errors."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_errors()

            assert page.total == 2
            assert len(page.items) == 2

    async def test_list_errors_filter_by_type(
        self, db_path: Path, populated_db
    ) -> None:
        """Test filtering errors by type."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_errors(error_type="xpath")
            assert page.total == 1
            assert page.items[0]["error_type"] == "xpath"

    async def test_list_errors_filter_by_resolution(
        self, db_path: Path, populated_db
    ) -> None:
        """Test filtering errors by resolution status."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Unresolved errors
            page = await debugger.list_errors(is_resolved=False)
            assert page.total == 1

            # Resolved errors
            page = await debugger.list_errors(is_resolved=True)
            assert page.total == 1

    async def test_get_error(self, db_path: Path, populated_db) -> None:
        """Test getting a single error."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            error = await debugger.get_error(1)

            assert error is not None
            assert error["error_type"] == "xpath"
            assert error["message"] == "XPath not found"
            assert error["selector"] == "//*[@id='test']"

    async def test_get_error_summary(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting error summary."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            summary = await debugger.get_error_summary()

            assert "by_type" in summary
            assert "by_continuation" in summary
            assert "totals" in summary

            # Check totals
            assert summary["totals"]["total"] == 2
            assert summary["totals"]["resolved"] == 1
            assert summary["totals"]["unresolved"] == 1


class TestResultInspection:
    """Tests for result inspection methods."""

    async def test_list_results(self, db_path: Path, populated_db) -> None:
        """Test listing results."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_results()

            assert page.total == 2
            assert len(page.items) == 2

    async def test_list_results_filter_by_validity(
        self, db_path: Path, populated_db
    ) -> None:
        """Test filtering results by validity."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Valid results
            page = await debugger.list_results(is_valid=True)
            assert page.total == 1

            # Invalid results
            page = await debugger.list_results(is_valid=False)
            assert page.total == 1

    async def test_get_result(self, db_path: Path, populated_db) -> None:
        """Test getting a single result."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_result(1)

            assert result is not None
            assert result.result_type == "TestResult"
            assert result.data is not None
            assert result.data["title"] == "Result 1"
            assert result.is_valid is True

    async def test_get_result_summary(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting result summary."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            summary = await debugger.get_result_summary()

            assert "TestResult" in summary
            assert summary["TestResult"]["valid"] == 1
            assert summary["TestResult"]["invalid"] == 1
            assert summary["TestResult"]["total"] == 2


class TestSpeculationInspection:
    """Tests for speculation inspection methods."""

    async def test_get_speculation_summary(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting speculation summary."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            summary = await debugger.get_speculation_summary()

            assert "config" in summary
            assert "progress" in summary
            assert "tracking" in summary

    async def test_get_speculative_progress(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting speculative progress."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            progress = await debugger.get_speculative_progress()

            # Empty progress in test database
            assert isinstance(progress, dict)


class TestRateLimiterInspection:
    """Tests for rate limiter inspection methods."""

    async def test_get_rate_limiter_state(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting rate limiter state."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            state = await debugger.get_rate_limiter_state()

            assert state is not None
            assert state["tokens"] == 10.0
            assert state["rate"] == 2.0
            assert state["bucket_size"] == 20.0
            assert state["total_requests"] == 100

    async def test_get_throughput_stats(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting throughput statistics."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            stats = await debugger.get_throughput_stats()

            assert isinstance(stats, dict)


class TestCompressionInspection:
    """Tests for compression inspection methods."""

    async def test_get_compression_stats(
        self, db_path: Path, populated_db
    ) -> None:
        """Test getting compression statistics."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            stats = await debugger.get_compression_stats()

            assert isinstance(stats, dict)
            assert "total" in stats

    async def test_list_compression_dicts(
        self, db_path: Path, populated_db
    ) -> None:
        """Test listing compression dictionaries."""
        engine, _ = populated_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            dicts = await debugger.list_compression_dicts()

            assert len(dicts) == 1
            assert dicts[0]["continuation"] == "step1"
            assert dicts[0]["version"] == 1
            assert dicts[0]["sample_count"] == 100
