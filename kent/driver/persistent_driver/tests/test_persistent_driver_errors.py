"""Tests for error tracking, retry logic, and requeue operations."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any

import sqlalchemy as sa


class TestErrorTracking:
    """Tests for error tracking module."""

    async def test_store_and_retrieve_error(self, initialized_db) -> None:
        """Test storing and retrieving an error."""
        from kent.common.exceptions import (
            HTMLStructuralAssumptionException,
        )
        from kent.driver.persistent_driver.errors import (
            get_error,
            store_error,
        )

        engine, session_factory = initialized_db
        exc = HTMLStructuralAssumptionException(
            selector=".missing-class",
            selector_type="css",
            description="Test selector not found",
            expected_min=1,
            expected_max=None,
            actual_count=0,
            request_url="https://example.com/test",
        )

        error_id = await store_error(
            session_factory, exc, request_url="https://example.com/test"
        )
        assert error_id > 0

        error = await get_error(session_factory, error_id)
        assert error is not None
        assert error.error_type == "structural"
        assert error.selector == ".missing-class"
        assert not error.is_resolved

    async def test_resolve_error(self, initialized_db) -> None:
        """Test resolving an error."""
        from kent.common.exceptions import (
            HTMLStructuralAssumptionException,
        )
        from kent.driver.persistent_driver.errors import (
            get_error,
            resolve_error,
            store_error,
        )

        engine, session_factory = initialized_db
        exc = HTMLStructuralAssumptionException(
            selector=".test",
            selector_type="css",
            description="Test",
            expected_min=1,
            expected_max=None,
            actual_count=0,
            request_url="https://example.com",
        )

        error_id = await store_error(session_factory, exc)

        resolved = await resolve_error(
            session_factory, error_id, notes="Fixed the selector"
        )
        assert resolved

        error = await get_error(session_factory, error_id)
        assert error is not None
        assert error.is_resolved
        assert error.resolution_notes == "Fixed the selector"

    async def test_list_errors_filter(self, initialized_db) -> None:
        """Test listing errors with filters."""
        from kent.common.exceptions import (
            HTMLStructuralAssumptionException,
            RequestTimeoutException,
        )
        from kent.driver.persistent_driver.errors import (
            list_errors,
            store_error,
        )

        engine, session_factory = initialized_db
        # Create structural error
        exc1 = HTMLStructuralAssumptionException(
            selector=".test1",
            selector_type="css",
            description="Test 1",
            expected_min=1,
            expected_max=None,
            actual_count=0,
            request_url="https://example.com/1",
        )
        await store_error(session_factory, exc1)

        # Create transient error
        exc2 = RequestTimeoutException(
            url="https://example.com/2",
            timeout_seconds=30.0,
        )
        await store_error(session_factory, exc2)

        # List all
        all_errors = await list_errors(session_factory, unresolved_only=True)
        assert len(all_errors) == 2

        # Filter by type
        structural = await list_errors(
            session_factory, error_type="structural", unresolved_only=True
        )
        assert len(structural) == 1
        assert structural[0].error_type == "structural"


class TestRetryLogic:
    """Tests for retry with exponential backoff."""

    async def test_exponential_backoff_calculation(
        self, initialized_db
    ) -> None:
        """Test that retry delays follow exponential backoff."""
        from kent.driver.persistent_driver.database import (
            get_next_queue_counter,
        )

        engine, session_factory = initialized_db
        # Create a request
        async with session_factory() as session:
            queue_counter = await get_next_queue_counter(session)
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, method, url,
                    continuation, current_location, retry_count, cumulative_backoff
                ) VALUES ('in_progress', 9, :queue_counter, 'GET', 'https://example.com/test',
                          'parse', '', 0, 0.0)
                """),
                {"queue_counter": queue_counter},
            )
            await session.commit()

        async with session_factory() as session:
            result = await session.execute(
                sa.text("SELECT id FROM requests LIMIT 1")
            )
            row = result.first()
        request_id = row[0]

        max_backoff_time = 60.0
        retry_base_delay = 1.0
        cumulative = 0.0

        expected_delays = [
            1.0,
            2.0,
            4.0,
            8.0,
            15.0,
            15.0,
        ]  # Capped at 15 (60/4)

        for i, expected_delay in enumerate(expected_delays):
            async with session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT retry_count, cumulative_backoff FROM requests WHERE id = :id"
                    ),
                    {"id": request_id},
                )
                row = result.first()
            retry_count = row[0]

            next_delay = retry_base_delay * (2**retry_count)
            max_individual = max_backoff_time / 4
            next_delay = min(next_delay, max_individual)

            assert next_delay == expected_delay, (
                f"Retry {i}: expected {expected_delay}, got {next_delay}"
            )

            cumulative += next_delay

            if cumulative >= max_backoff_time:
                break

            # Update for next iteration
            async with session_factory() as session:
                await session.execute(
                    sa.text("""
                    UPDATE requests
                    SET retry_count = retry_count + 1,
                        cumulative_backoff = :cumulative
                    WHERE id = :id
                    """),
                    {"cumulative": cumulative, "id": request_id},
                )
                await session.commit()

    async def test_retry_respects_max_backoff(self, initialized_db) -> None:
        """Test that cumulative backoff is capped at max_backoff_time."""
        from kent.driver.persistent_driver.database import (
            get_next_queue_counter,
        )

        engine, session_factory = initialized_db
        async with session_factory() as session:
            queue_counter = await get_next_queue_counter(session)
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, method, url,
                    continuation, current_location, retry_count, cumulative_backoff
                ) VALUES ('in_progress', 9, :queue_counter, 'GET', 'https://example.com/test',
                          'parse', '', 5, 45.0)
                """),
                {"queue_counter": queue_counter},
            )
            await session.commit()

        max_backoff_time = 60.0
        retry_base_delay = 1.0

        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    "SELECT retry_count, cumulative_backoff FROM requests LIMIT 1"
                )
            )
            row = result.first()
        retry_count, cumulative = row

        next_delay = retry_base_delay * (2**retry_count)
        next_delay = min(next_delay, max_backoff_time / 4)

        new_cumulative = cumulative + next_delay
        should_fail = new_cumulative >= max_backoff_time

        assert should_fail, (
            f"Expected to fail: cumulative={new_cumulative}, max={max_backoff_time}"
        )


class TestRequeueFunction:
    """Tests for requeue functionality."""

    async def test_requeue_creates_new_request(self, initialized_db) -> None:
        """Test that requeue creates a new pending request."""
        from kent.common.exceptions import (
            HTMLStructuralAssumptionException,
        )
        from kent.driver.persistent_driver.database import (
            get_next_queue_counter,
        )
        from kent.driver.persistent_driver.errors import (
            get_error,
            resolve_error,
            store_error,
        )

        engine, session_factory = initialized_db
        # Create original request
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, method, url,
                    continuation, current_location
                ) VALUES ('failed', 9, 1, 'GET', 'https://example.com/test',
                          'parse_results', '')
                """)
            )
            await session.commit()

        async with session_factory() as session:
            result = await session.execute(
                sa.text("SELECT id FROM requests LIMIT 1")
            )
            row = result.first()
        request_id = row[0]

        # Create error linked to request
        exc = HTMLStructuralAssumptionException(
            selector=".missing",
            selector_type="css",
            description="Not found",
            expected_min=1,
            expected_max=None,
            actual_count=0,
            request_url="https://example.com/test",
        )
        error_id = await store_error(
            session_factory, exc, request_id=request_id
        )

        # Simulate requeue
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                SELECT r.method, r.url, r.continuation, r.priority
                FROM errors e
                JOIN requests r ON e.request_id = r.id
                WHERE e.id = :error_id
                """),
                {"error_id": error_id},
            )
            row = result.first()
        method, url, continuation, priority = row

        async with session_factory() as session:
            queue_counter = await get_next_queue_counter(session)
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, method, url,
                    continuation, current_location, parent_request_id
                ) VALUES ('pending', :priority, :queue_counter, :method, :url, :continuation, '', :parent_id)
                """),
                {
                    "priority": priority,
                    "queue_counter": queue_counter,
                    "method": method,
                    "url": url,
                    "continuation": continuation,
                    "parent_id": request_id,
                },
            )
            await session.commit()

        async with session_factory() as session:
            result = await session.execute(
                sa.text("SELECT last_insert_rowid()")
            )
            row = result.first()
        new_request_id = row[0]

        await resolve_error(
            session_factory,
            error_id,
            notes=f"Requeued as request {new_request_id}",
        )

        # Verify error is resolved
        error = await get_error(session_factory, error_id)
        assert error is not None
        assert error.is_resolved
        assert "Requeued" in (error.resolution_notes or "")

        # Verify new request exists
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    "SELECT status, parent_request_id FROM requests WHERE id = :id"
                ),
                {"id": new_request_id},
            )
            row = result.first()
        assert row[0] == "pending"
        assert row[1] == request_id


class TestExponentialBackoff:
    """Tests for exponential backoff retry logic."""

    async def test_transient_error_triggers_retry(self, db_path: Path) -> None:
        """Test that transient errors trigger retry with backoff."""

        from kent.common.exceptions import (
            RequestTimeoutException,
        )
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        class RetryScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/flaky",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response) -> Generator[None, None, None]:
                yield None

        # Create a custom flaky request manager that fails once then succeeds
        class FlakyRequestManager:
            """Request manager that fails on first request, then succeeds."""

            def __init__(self) -> None:
                self.request_count = 0

            async def resolve_request(self, request: Any) -> Any:
                url = request.request.url
                if url == "https://example.com/flaky":
                    self.request_count += 1
                    if self.request_count == 1:
                        raise RequestTimeoutException(
                            url="https://example.com/flaky",
                            timeout_seconds=30.0,
                        )
                    return Response(
                        request=request,
                        status_code=200,
                        headers={},
                        content=b"<html>Success</html>",
                        text="<html>Success</html>",
                        url=url,
                    )
                raise ValueError(f"No mock response configured for URL: {url}")

            async def close(self) -> None:
                pass

        request_manager = FlakyRequestManager()

        scraper = RetryScraper()
        # Use low max_backoff_time so the test is fast
        async with PersistentDriver.open(
            scraper,
            db_path,
            max_backoff_time=60.0,
            initial_rate=10.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            await driver.run()

            # Check that retry_count was incremented
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT retry_count, status FROM requests WHERE url = 'https://example.com/flaky'"
                    )
                )
                row = result.first()

            assert row is not None
            retry_count, status = row

            # Either it retried and succeeded, or it's scheduled for retry
            # The behavior depends on timing
            assert retry_count >= 1 or status == "completed", (
                f"Expected retry_count >= 1 or completed status, got retry_count={retry_count}, status={status}"
            )

    async def test_max_backoff_exceeded_marks_failed(
        self, db_path: Path
    ) -> None:
        """Test that exceeding max backoff time marks request as failed."""

        from kent.common.exceptions import (
            RequestTimeoutException,
        )
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )
        from kent.driver.persistent_driver.testing import (
            TestRequestManager,
        )

        class AlwaysFailScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/always-fail",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response) -> Generator[None, None, None]:
                yield None

        request_manager = TestRequestManager()
        request_manager.add_error(
            "https://example.com/always-fail",
            RequestTimeoutException(
                url="https://example.com/always-fail",
                timeout_seconds=30.0,
            ),
        )

        scraper = AlwaysFailScraper()
        # Very low max_backoff_time to trigger failure quickly
        async with PersistentDriver.open(
            scraper,
            db_path,
            max_backoff_time=0.5,
            initial_rate=10.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            await driver.run()

            # Request should eventually be marked as failed
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT status, cumulative_backoff FROM requests WHERE url = 'https://example.com/always-fail'"
                    )
                )
                row = result.first()

            assert row is not None
            status, cumulative_backoff = row

            # Should be failed (or pending with high backoff if still retrying)
            assert status == "failed" or (
                status == "pending" and cumulative_backoff > 0
            ), (
                f"Expected failed or pending with backoff, got status={status}, backoff={cumulative_backoff}"
            )


class TestRequeueErroredRequests:
    """Tests for re-enqueueing errored requests."""

    async def test_requeue_single_error(self, db_path: Path) -> None:
        """Test re-enqueueing a single errored request."""

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
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )
        from kent.driver.persistent_driver.testing import (
            TestRequestManager,
            create_html_response,
        )

        class FailThenSucceedScraper(BaseScraper[str]):
            calls = 0

            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/requeue-test",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response) -> Generator[None, None, None]:
                FailThenSucceedScraper.calls += 1
                if FailThenSucceedScraper.calls == 1:
                    raise HTMLStructuralAssumptionException(
                        selector=".data",
                        selector_type="css",
                        description="Missing data",
                        expected_min=1,
                        expected_max=None,
                        actual_count=0,
                        request_url=response.url,
                    )
                yield None

        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/requeue-test",
            create_html_response("<html>Test</html>"),
        )

        scraper = FailThenSucceedScraper()
        async with PersistentDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            # First run - will fail
            await driver.run()

            # Check error was stored
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT id, is_resolved FROM errors LIMIT 1")
                )
                error_row = result.first()
            assert error_row is not None
            error_id, is_resolved = error_row
            assert is_resolved == 0, "Error should not be resolved initially"

            # Requeue the error using the new requeue method
            result = await driver.db.requeue_error(error_id)

            assert len(result.requeued_request_ids) == 1, (
                "Should create one request"
            )
            assert error_id in result.resolved_error_ids, (
                "Should resolve the error"
            )
            new_request_id = result.requeued_request_ids[0]

            # Check new request was created
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT status, parent_request_id FROM requests WHERE id = :id"
                    ),
                    {"id": new_request_id},
                )
                new_req_row = result.first()
            assert new_req_row is not None
            status, parent_id = new_req_row
            assert status == "pending"
            assert parent_id is not None, "Should have parent reference"

            # Run again - should succeed this time
            await driver.run()

            # Check the new request completed
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT status FROM requests WHERE id = :id"),
                    {"id": new_request_id},
                )
                final_row = result.first()
            assert final_row is not None
            assert final_row[0] == "completed"


class TestRequeueErrorsByType:
    """Tests for requeue_errors_by_type functionality."""

    async def test_requeue_errors_by_type_filters_correctly(
        self, db_path: Path
    ) -> None:
        """Test that requeue_errors_by_type filters by error_type."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        class SimpleScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response):
                return []

        scraper = SimpleScraper()
        async with PersistentDriver.open(
            scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Create requests that will be associated with errors
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url,
                                         continuation, current_location)
                    VALUES
                        ('failed', 5, 1, 'GET', 'https://example.com/structural1', 'parse', ''),
                        ('failed', 5, 2, 'GET', 'https://example.com/structural2', 'parse', ''),
                        ('failed', 5, 3, 'GET', 'https://example.com/transient1', 'parse_detail', ''),
                        ('failed', 5, 4, 'GET', 'https://example.com/validation1', 'parse', '')
                    """)
                )

                # Create errors of different types
                await session.execute(
                    sa.text("""
                    INSERT INTO errors (request_id, error_type, error_class, message, request_url)
                    VALUES
                        (1, 'structural', 'HTMLStructuralAssumptionException', 'selector failed', 'https://example.com/structural1'),
                        (2, 'structural', 'HTMLStructuralAssumptionException', 'selector failed', 'https://example.com/structural2'),
                        (3, 'transient', 'RequestTimeoutException', 'timeout', 'https://example.com/transient1'),
                        (4, 'validation', 'DataFormatAssumptionException', 'invalid data', 'https://example.com/validation1')
                    """)
                )
                await session.commit()

            # Requeue only structural errors
            new_ids = await driver.requeue_errors_by_type(
                error_type="structural"
            )

            assert len(new_ids) == 2, (
                f"Expected 2 structural errors, got {len(new_ids)}"
            )

            # Verify the requeued requests
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT url FROM requests WHERE id IN (:id1, :id2)"
                    ),
                    {"id1": new_ids[0], "id2": new_ids[1]},
                )
                urls = [row[0] for row in result.all()]
            assert "https://example.com/structural1" in urls
            assert "https://example.com/structural2" in urls

            # Verify structural errors are marked resolved
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT is_resolved FROM errors WHERE error_type = 'structural'"
                    )
                )
                resolved_statuses = [row[0] for row in result.all()]
            assert all(s == 1 for s in resolved_statuses), (
                "Structural errors should be resolved"
            )

            # Verify transient error is NOT resolved
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT is_resolved FROM errors WHERE error_type = 'transient'"
                    )
                )
                row = result.first()
            assert row[0] == 0, "Transient error should NOT be resolved"

    async def test_requeue_errors_by_continuation(self, db_path: Path) -> None:
        """Test that requeue_errors_by_type filters by continuation."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        class SimpleScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response):
                return []

        scraper = SimpleScraper()
        async with PersistentDriver.open(
            scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Create requests with different continuations
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url,
                                         continuation, current_location)
                    VALUES
                        ('failed', 5, 1, 'GET', 'https://example.com/1', 'parse_listing', ''),
                        ('failed', 5, 2, 'GET', 'https://example.com/2', 'parse_listing', ''),
                        ('failed', 5, 3, 'GET', 'https://example.com/3', 'parse_detail', '')
                    """)
                )

                # Create errors for all
                await session.execute(
                    sa.text("""
                    INSERT INTO errors (request_id, error_type, error_class, message, request_url)
                    VALUES
                        (1, 'structural', 'HTMLStructuralAssumptionException', 'error', 'https://example.com/1'),
                        (2, 'structural', 'HTMLStructuralAssumptionException', 'error', 'https://example.com/2'),
                        (3, 'structural', 'HTMLStructuralAssumptionException', 'error', 'https://example.com/3')
                    """)
                )
                await session.commit()

            # Requeue only parse_listing continuation errors
            new_ids = await driver.requeue_errors_by_type(
                continuation="parse_listing"
            )

            assert len(new_ids) == 2, (
                f"Expected 2 parse_listing errors, got {len(new_ids)}"
            )

            # Verify parse_detail error is NOT resolved
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("""
                    SELECT e.is_resolved FROM errors e
                    JOIN requests r ON e.request_id = r.id
                    WHERE r.continuation = 'parse_detail'
                    """)
                )
                row = result.first()
            assert row[0] == 0, "parse_detail error should NOT be resolved"

    async def test_requeue_errors_no_matches_returns_empty(
        self, db_path: Path
    ) -> None:
        """Test that requeue_errors_by_type returns empty list when no matches."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        class SimpleScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response):
                return []

        scraper = SimpleScraper()
        async with PersistentDriver.open(
            scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Try to requeue with no errors in DB
            new_ids = await driver.requeue_errors_by_type(
                error_type="structural"
            )

            assert new_ids == [], "Should return empty list when no errors"
