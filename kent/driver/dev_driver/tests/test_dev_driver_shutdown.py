"""Tests for graceful shutdown and resume."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa


class TestGracefulShutdownAndResume:
    """Tests for graceful shutdown and resume functionality."""

    @pytest.fixture
    def mock_scraper(self) -> Any:
        """Create a mock scraper for testing."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
        )

        class MockScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Any) -> list:
                return []

        return MockScraper()

    async def test_shutdown_resets_in_progress_to_pending(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that closing the driver resets in_progress requests to pending."""
        from kent.driver.dev_driver.database import (
            init_database,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        # Initialize database and add an in_progress request
        engine, session_factory = await init_database(db_path)
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                VALUES ('in_progress', 5, 1, 'GET', 'https://example.com/page1', 'parse', 'https://example.com')
                """)
            )
            await session.execute(
                sa.text("""
                INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                VALUES ('pending', 5, 2, 'GET', 'https://example.com/page2', 'parse', 'https://example.com')
                """)
            )
            await session.commit()

        # Verify setup
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    "SELECT COUNT(*) FROM requests WHERE status = 'in_progress'"
                )
            )
            assert result.first()[0] == 1

            result = await session.execute(
                sa.text(
                    "SELECT COUNT(*) FROM requests WHERE status = 'pending'"
                )
            )
            assert result.first()[0] == 1

        await engine.dispose()

        # Now open driver and close it (simulating graceful shutdown)
        async with LocalDevDriver.open(
            mock_scraper, db_path, resume=False, enable_monitor=False
        ):
            # Driver is open - in_progress should still be in_progress
            # (resume=False means we don't reset on open)
            pass  # Just close immediately

        # Now check that in_progress was reset to pending
        engine, session_factory = await init_database(db_path)
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    "SELECT COUNT(*) FROM requests WHERE status = 'in_progress'"
                )
            )
            in_progress_count = result.first()[0]

            result = await session.execute(
                sa.text(
                    "SELECT COUNT(*) FROM requests WHERE status = 'pending'"
                )
            )
            pending_count = result.first()[0]

        await engine.dispose()

        assert in_progress_count == 0, "in_progress requests should be reset"
        assert pending_count == 2, "Both requests should be pending now"

    async def test_resume_restores_pending_requests(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that resume=True restores in_progress requests to pending on open."""
        from kent.driver.dev_driver.database import (
            init_database,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        # Initialize database with an in_progress request (simulating interrupted run)
        engine, session_factory = await init_database(db_path)
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                VALUES ('in_progress', 5, 1, 'GET', 'https://example.com/interrupted', 'parse', 'https://example.com')
                """)
            )
            await session.execute(
                sa.text("""
                INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                VALUES ('pending', 5, 2, 'GET', 'https://example.com/pending', 'parse', 'https://example.com')
                """)
            )
            await session.commit()
        await engine.dispose()

        # Open with resume=True (default)
        async with LocalDevDriver.open(
            mock_scraper,
            db_path,
            resume=True,
            initial_rate=100.0,
            enable_monitor=False,
        ) as driver:
            # Check that in_progress was reset to pending on open
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE status = 'in_progress'"
                    )
                )
                in_progress_count = result.first()[0]

                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE status = 'pending'"
                    )
                )
                pending_count = result.first()[0]

            assert in_progress_count == 0, (
                "resume=True should reset in_progress to pending"
            )
            assert pending_count == 2, "Both requests should be pending"

    async def test_full_shutdown_and_resume_cycle(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test a complete shutdown and resume cycle preserves all requests."""
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        # First run: Open driver, add requests, then close
        async with LocalDevDriver.open(
            mock_scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Manually add some requests in different states
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                    VALUES
                        ('pending', 5, 10, 'GET', 'https://example.com/page1', 'parse', 'https://example.com'),
                        ('pending', 5, 11, 'GET', 'https://example.com/page2', 'parse', 'https://example.com'),
                        ('in_progress', 5, 12, 'GET', 'https://example.com/page3', 'parse', 'https://example.com'),
                        ('completed', 5, 13, 'GET', 'https://example.com/page4', 'parse', 'https://example.com')
                    """)
                )
                await session.commit()

            # Verify initial state
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT status, COUNT(*) FROM requests GROUP BY status ORDER BY status"
                    )
                )
                counts_before = dict(result.all())

            assert counts_before.get("pending", 0) >= 2
            assert counts_before.get("in_progress", 0) == 1
            assert counts_before.get("completed", 0) == 1

        # Second run: Resume and verify state
        async with LocalDevDriver.open(
            mock_scraper,
            db_path,
            resume=True,
            initial_rate=100.0,
            enable_monitor=False,
        ) as driver:
            # Check counts after resume
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT status, COUNT(*) FROM requests GROUP BY status ORDER BY status"
                    )
                )
                counts_after = dict(result.all())

            # in_progress should have been converted to pending
            assert counts_after.get("in_progress", 0) == 0, (
                "No requests should be in_progress after resume"
            )
            assert counts_after.get("pending", 0) == counts_before.get(
                "pending", 0
            ) + counts_before.get("in_progress", 0), (
                "in_progress should be converted to pending"
            )
            assert counts_after.get("completed", 0) == counts_before.get(
                "completed", 0
            ), "Completed requests should be preserved"

    async def test_stop_event_signals_workers_to_stop(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that setting stop_event causes workers to exit gracefully."""
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        async with LocalDevDriver.open(
            mock_scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # stop_event should be created
            assert driver.stop_event is not None
            assert not driver.stop_event.is_set()

            # Call stop()
            driver.stop()

            # stop_event should now be set
            assert driver.stop_event.is_set()

    async def test_run_metadata_status_transitions(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that run metadata status is updated correctly during lifecycle."""
        from kent.driver.dev_driver.database import (
            init_database,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        # First open creates metadata with 'created' status
        async with LocalDevDriver.open(
            mock_scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT status FROM run_metadata WHERE id = 1")
                )
                row = result.first()
            assert row is not None
            assert row[0] == "created"

        # After close, check if it was updated (if it was running)
        engine, session_factory = await init_database(db_path)
        async with session_factory() as session:
            result = await session.execute(
                sa.text("SELECT status FROM run_metadata WHERE id = 1")
            )
            row = result.first()
        assert row is not None
        # Status should still be 'created' since run() wasn't called
        # The status only changes to 'interrupted' if status was 'running'
        assert row[0] == "created"
        await engine.dispose()

    async def test_no_data_loss_on_shutdown(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that no requests are lost during shutdown cycle."""
        from kent.driver.dev_driver.database import (
            init_database,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        # Create a run with multiple requests
        request_urls = [f"https://example.com/page{i}" for i in range(10)]

        async with LocalDevDriver.open(
            mock_scraper, db_path, enable_monitor=False
        ) as driver:
            # Add requests
            async with driver.db._session_factory() as session:
                for i, url in enumerate(request_urls):
                    await session.execute(
                        sa.text("""
                        INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                        VALUES ('pending', 5, :queue_counter, 'GET', :url, 'parse', 'https://example.com')
                        """),
                        {"queue_counter": i + 10, "url": url},
                    )
                await session.commit()

            # Mark some as in_progress (simulating work being done)
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text(
                        "UPDATE requests SET status = 'in_progress' WHERE url LIKE '%page5%' OR url LIKE '%page6%'"
                    )
                )
                await session.commit()

            # Count total before shutdown
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT COUNT(*) FROM requests")
                )
                total_before = result.first()[0]

        # After shutdown, verify no loss
        engine, session_factory = await init_database(db_path)
        async with session_factory() as session:
            result = await session.execute(
                sa.text("SELECT COUNT(*) FROM requests")
            )
            total_after = result.first()[0]

        # All requests should still be present
        assert total_after == total_before, (
            f"Expected {total_before} requests, got {total_after}"
        )

        # Verify all URLs are still there
        async with session_factory() as session:
            result = await session.execute(
                sa.text("SELECT url FROM requests ORDER BY url")
            )
            urls_in_db = [row[0] for row in result.all()]

        for url in request_urls:
            assert url in urls_in_db, f"Missing URL: {url}"

        await engine.dispose()

    async def test_status_method_reflects_queue_state(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that status() correctly reflects the queue state."""
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        async with LocalDevDriver.open(
            mock_scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Initially, no requests (entry point not added yet if no run())
            # But the entry point request is added by run(), so status depends on
            # whether any requests exist
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT COUNT(*) FROM requests")
                )
                count = result.first()[0]

            if count == 0:
                status = await driver.status()
                assert status == "unstarted"

            # Add pending requests
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                    VALUES ('pending', 5, 1, 'GET', 'https://example.com/page1', 'parse', 'https://example.com')
                    """)
                )
                await session.commit()

            status = await driver.status()
            assert status == "in_progress"

            # Mark all as completed
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("UPDATE requests SET status = 'completed'")
                )
                await session.commit()

            status = await driver.status()
            assert status == "done"

    async def test_get_next_request_returns_pending_only(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that _get_next_request only returns pending requests."""
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        async with LocalDevDriver.open(
            mock_scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Add requests in different states
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                    VALUES
                        ('completed', 5, 1, 'GET', 'https://example.com/completed', 'parse', 'https://example.com'),
                        ('failed', 5, 2, 'GET', 'https://example.com/failed', 'parse', 'https://example.com'),
                        ('held', 5, 3, 'GET', 'https://example.com/held', 'parse', 'https://example.com'),
                        ('pending', 5, 4, 'GET', 'https://example.com/pending', 'parse', 'https://example.com')
                    """)
                )
                await session.commit()

            # Get next request - should only return pending
            result = await driver._get_next_request()

            assert result is not None
            request_id, deserialized = result
            # NavigatingRequest returns BaseRequest directly
            request = (
                deserialized
                if not isinstance(deserialized, tuple)
                else deserialized[0]
            )
            assert request.request.url == "https://example.com/pending"

            # The pending request should now be marked in_progress
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT status FROM requests WHERE id = :id"),
                    {"id": request_id},
                )
                row = result.first()  # type: ignore[union-attr]
            assert row is not None
            assert row[0] == "in_progress"

    async def test_held_requests_not_returned(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that held requests are skipped by _get_next_request."""
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        async with LocalDevDriver.open(
            mock_scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Add only held requests
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                    VALUES
                        ('held', 5, 1, 'GET', 'https://example.com/held1', 'parse', 'https://example.com'),
                        ('held', 5, 2, 'GET', 'https://example.com/held2', 'parse', 'https://example.com')
                    """)
                )
                await session.commit()

            # Get next request - should return None
            result = await driver._get_next_request()
            assert result is None

    async def test_pause_and_resume_step(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test pause_step and resume_step functionality."""
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        async with LocalDevDriver.open(
            mock_scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Add requests with different continuations
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                    VALUES
                        ('pending', 5, 1, 'GET', 'https://example.com/page1', 'parse_list', 'https://example.com'),
                        ('pending', 5, 2, 'GET', 'https://example.com/page2', 'parse_list', 'https://example.com'),
                        ('pending', 5, 3, 'GET', 'https://example.com/page3', 'parse_detail', 'https://example.com')
                    """)
                )
                await session.commit()

            # Pause 'parse_list' continuation
            held_count = await driver.pause_step("parse_list")
            assert held_count == 2

            # Verify held count
            assert await driver.get_held_count("parse_list") == 2
            assert await driver.get_held_count("parse_detail") == 0
            assert await driver.get_held_count() == 2  # Total held

            # Resume 'parse_list' continuation
            resumed_count = await driver.resume_step("parse_list")
            assert resumed_count == 2

            # Verify all back to pending
            assert await driver.get_held_count() == 0


class TestGracefulShutdownSigterm:
    """Tests for graceful shutdown via SIGTERM/SIGINT."""

    async def test_stop_event_stops_workers(self, db_path: Path) -> None:
        """Test that setting stop_event causes workers to exit gracefully."""
        import asyncio

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        # Track how many requests were processed
        processed_count = 0

        class MultiPageScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/page1",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response):
                nonlocal processed_count
                processed_count += 1

                # Yield more requests to keep driver busy
                for i in range(2, 20):
                    yield NavigatingRequest(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"https://example.com/page{i}",
                        ),
                        continuation="parse_page",
                        current_location="",
                    )

            def parse_page(self, response: Response):
                nonlocal processed_count
                processed_count += 1
                return []

        scraper = MultiPageScraper()
        request_manager = TestRequestManager()

        # Add mock responses for many pages
        for i in range(1, 20):
            request_manager.add_response(
                f"https://example.com/page{i}",
                MockResponse(
                    content=f"<html>Page {i}</html>".encode(),
                    status_code=200,
                ),
            )

        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            # Start the driver and stop it after a short delay
            async def stop_after_delay():
                await asyncio.sleep(0.1)  # Let some requests process
                driver.stop()

            # Run driver and stop concurrently
            await asyncio.gather(
                driver.run(setup_signal_handlers=False),
                stop_after_delay(),
            )

            # Verify some but not all requests were processed
            assert processed_count > 0, (
                "Should have processed at least 1 request"
            )
            assert processed_count < 20, (
                f"Should have stopped before all 20, processed {processed_count}"
            )

            # Verify run metadata shows interrupted status
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT status FROM run_metadata WHERE id = 1")
                )
                row = result.first()
            assert row is not None
            assert row[0] == "interrupted", (
                f"Expected 'interrupted', got '{row[0]}'"
            )

            # Verify any in_progress requests were reset to pending for resume
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE status = 'in_progress'"
                    )
                )
                in_progress_row = result.first()
            assert in_progress_row[0] == 0, (
                "Should have no in_progress requests after shutdown"
            )

    async def test_signal_handler_setup_and_teardown(
        self, db_path: Path
    ) -> None:
        """Test that signal handlers are set up and torn down properly."""
        import signal

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        class SimpleScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
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
        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com",
            MockResponse(content=b"<html></html>", status_code=200),
        )

        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            # Verify _setup_signal_handlers and _restore_signal_handlers work
            # Set known handlers first
            signal.signal(signal.SIGTERM, signal.SIG_DFL)

            # Call setup directly
            driver._setup_signal_handlers()

            # After setup, handlers should be custom functions, not SIG_DFL
            sigterm_handler = signal.getsignal(signal.SIGTERM)
            assert sigterm_handler != signal.SIG_DFL, (
                "SIGTERM handler should be custom after setup"
            )

            # Restore handlers
            driver._restore_signal_handlers()

            # After restore, handlers should be SIG_DFL
            sigterm_handler_after = signal.getsignal(signal.SIGTERM)
            assert sigterm_handler_after == signal.SIG_DFL, (
                "SIGTERM handler should be SIG_DFL after restore"
            )

    async def test_resume_after_interrupt(self, db_path: Path) -> None:
        """Test that interrupted requests can be resumed on next run."""
        import asyncio

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        completed_urls: list[str] = []

        class MultiStepScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/start",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response):
                completed_urls.append(response.url)
                # Queue up several child requests
                for i in range(5):
                    yield NavigatingRequest(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"https://example.com/item{i}",
                        ),
                        continuation="parse_item",
                        current_location="",
                    )

            def parse_item(self, response: Response):
                completed_urls.append(response.url)
                return []

        scraper = MultiStepScraper()
        request_manager = TestRequestManager()

        # Add responses
        request_manager.add_response(
            "https://example.com/start",
            MockResponse(content=b"<html>Start</html>", status_code=200),
        )
        for i in range(5):
            request_manager.add_response(
                f"https://example.com/item{i}",
                MockResponse(
                    content=f"<html>Item {i}</html>".encode(), status_code=200
                ),
            )

        # First run - interrupt early
        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:

            async def stop_early():
                await asyncio.sleep(0.05)
                driver.stop()

            await asyncio.gather(
                driver.run(setup_signal_handlers=False),
                stop_early(),
            )

        initial_count = len(completed_urls)
        assert initial_count > 0, (
            "Should have processed at least the entry point"
        )

        # Clear completed list for second run
        completed_urls.clear()

        # Second run - should pick up where we left off
        # Need a fresh scraper instance
        scraper2 = MultiStepScraper()
        request_manager2 = TestRequestManager()

        request_manager2.add_response(
            "https://example.com/start",
            MockResponse(content=b"<html>Start</html>", status_code=200),
        )
        for i in range(5):
            request_manager2.add_response(
                f"https://example.com/item{i}",
                MockResponse(
                    content=f"<html>Item {i}</html>".encode(), status_code=200
                ),
            )

        async with LocalDevDriver.open(
            scraper2,
            db_path,
            resume=True,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager2,
        ) as driver2:
            await driver2.run(setup_signal_handlers=False)

            # Verify all requests are now completed
            async with driver2.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE status = 'completed'"
                    )
                )
                row = result.first()
            total_completed = row[0] if row else 0

            # Should have completed all 6 requests (1 entry + 5 items)
            assert total_completed == 6, (
                f"Expected 6 completed, got {total_completed}"
            )
