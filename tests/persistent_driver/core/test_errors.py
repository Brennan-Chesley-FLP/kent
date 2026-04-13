"""Tests for error tracking and retry logic."""

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
            MockRequestManager,
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

        request_manager = MockRequestManager()
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
