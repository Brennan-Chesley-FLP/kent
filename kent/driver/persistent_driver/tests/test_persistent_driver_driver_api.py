"""Tests for high-level driver API: comparison, getters, cancellation."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

import sqlalchemy as sa


class TestDevDriverVsOtherDrivers:
    """Tests comparing DevDriver output to SyncDriver/AsyncDriver."""

    async def test_same_results_as_async_driver(
        self, db_path: Path, tmp_path: Path
    ) -> None:
        """Test that DevDriver produces the same results as AsyncDriver."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.async_driver import AsyncDriver
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )
        from kent.driver.persistent_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        # Track results from each driver
        async_driver_results: list[dict] = []
        persistent_driver_results: list[dict] = []

        class TestScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/cases",
                    ),
                    continuation="parse_listing",
                    current_location="",
                )

            def parse_listing(self, response: Response):
                # Yield requests for detail pages
                for i in range(3):
                    yield Request(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"https://example.com/case/{i}",
                        ),
                        continuation="parse_detail",
                        current_location="",
                    )

            def parse_detail(self, response: Response):
                # Extract a "case" from the response
                case_id = response.url.split("/")[-1]
                yield ParsedData(
                    {
                        "case_id": case_id,
                        "title": f"Case {case_id}",
                        "url": response.url,
                    }
                )

        def create_request_manager() -> TestRequestManager:
            request_manager = TestRequestManager()
            request_manager.add_response(
                "https://example.com/cases",
                MockResponse(
                    content=b"<html><body>Case Listing</body></html>",
                    status_code=200,
                ),
            )
            for i in range(3):
                request_manager.add_response(
                    f"https://example.com/case/{i}",
                    MockResponse(
                        content=f"<html><body>Case {i} Details</body></html>".encode(),
                        status_code=200,
                    ),
                )
            return request_manager

        # Run with AsyncDriver
        scraper1 = TestScraper()

        async def collect_async_result(data: dict) -> None:
            async_driver_results.append(data)

        async_driver = AsyncDriver(
            scraper=scraper1, request_manager=create_request_manager()
        )
        async_driver.on_data = collect_async_result
        await async_driver.run()

        # Run with LocalDevDriver
        scraper2 = TestScraper()

        async def collect_dev_result(data: dict) -> None:
            persistent_driver_results.append(data)

        async with PersistentDriver.open(
            scraper2,
            db_path,
            enable_monitor=False,
            request_manager=create_request_manager(),
        ) as persistent_driver:
            persistent_driver.on_data = collect_dev_result
            await persistent_driver.run()

        # Compare results
        assert (
            len(async_driver_results) == len(persistent_driver_results) == 3
        ), (
            f"Expected 3 results each, got async={len(async_driver_results)}, "
            f"dev={len(persistent_driver_results)}"
        )

        # Sort by case_id for comparison
        async_sorted = sorted(async_driver_results, key=lambda x: x["case_id"])
        dev_sorted = sorted(
            persistent_driver_results, key=lambda x: x["case_id"]
        )

        for async_result, dev_result in zip(async_sorted, dev_sorted):
            assert async_result == dev_result, (
                f"Results differ: async={async_result}, dev={dev_result}"
            )

    async def test_persistent_driver_persists_results(
        self, db_path: Path
    ) -> None:
        """Test that DevDriver stores results in the database."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            ParsedData,
            Request,
            Response,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )
        from kent.driver.persistent_driver.testing import (
            MockResponse,
            TestRequestManager,
        )

        class ResultProducingScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/data",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response):
                for i in range(5):
                    yield ParsedData({"id": i, "value": f"item_{i}"})

        scraper = ResultProducingScraper()
        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/data",
            MockResponse(content=b"<html>Data</html>", status_code=200),
        )

        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            await driver.run()

            # Check results are persisted in database
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT COUNT(*) FROM results")
                )
                row = result.first()
            result_count = row[0] if row else 0

            assert result_count == 5, (
                f"Expected 5 results in DB, got {result_count}"
            )

            # Verify result content
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT data_json FROM results ORDER BY id")
                )
                rows = result.all()

            for i, row in enumerate(rows):
                data = json.loads(row[0])
                assert data["id"] == i
                assert data["value"] == f"item_{i}"

            # Verify requests are also tracked
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT COUNT(*) FROM requests")
                )
                req_row = result.first()
            assert req_row[0] >= 1, "Should have at least 1 request tracked"

            # Verify responses are stored
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE response_status_code IS NOT NULL"
                    )
                )
                resp_row = result.first()
            assert resp_row[0] >= 1, "Should have at least 1 response stored"


class TestGetterMethods:
    """Tests for get_response and get_result methods."""

    async def test_get_response_found(self, db_path: Path) -> None:
        """Test get_response returns response when found."""
        import uuid

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.compression import (
            compress,
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            # Create request and response
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url,
                                         continuation, current_location)
                    VALUES ('completed', 5, 1, 'GET', 'https://example.com/test', 'parse', '')
                    """)
                )
                content = b"Test content"
                compressed = compress(content)
                warc_id = str(uuid.uuid4())
                await session.execute(
                    sa.text("""
                    UPDATE requests SET
                        response_status_code = 200,
                        response_headers_json = '{"Content-Type": "text/html"}',
                        response_url = 'https://example.com/test',
                        content_compressed = :content_compressed,
                        content_size_original = :content_size_original,
                        content_size_compressed = :content_size_compressed,
                        warc_record_id = :warc_record_id
                    WHERE id = 1
                    """),
                    {
                        "content_compressed": compressed,
                        "content_size_original": len(content),
                        "content_size_compressed": len(compressed),
                        "warc_record_id": warc_id,
                    },
                )
                await session.commit()

            # Get response by ID
            response = await driver.get_response(1)

            assert response is not None
            assert response.id == 1
            assert response.status_code == 200
            assert response.url == "https://example.com/test"
            assert response.content_size_original == len(content)

    async def test_get_response_not_found(self, db_path: Path) -> None:
        """Test get_response returns None when not found."""
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            response = await driver.get_response(999)
            assert response is None

    async def test_get_result_found(self, db_path: Path) -> None:
        """Test get_result returns result when found."""
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            # Create a result
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO results (result_type, data_json, is_valid)
                    VALUES ('CaseData', '{"case_name": "Smith v. Jones", "id": 123}', 1)
                    """)
                )
                await session.commit()

            # Get result by ID
            result = await driver.get_result(1)

            assert result is not None
            assert result.id == 1
            assert result.result_type == "CaseData"
            assert result.is_valid  # Truthy check (SQLite returns 1)
            # Verify data can be parsed
            data = json.loads(result.data_json)
            assert data["case_name"] == "Smith v. Jones"

    async def test_get_result_not_found(self, db_path: Path) -> None:
        """Test get_result returns None when not found."""
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            result = await driver.get_result(999)
            assert result is None


class TestCancellationMethods:
    """Tests for cancel_request and cancel_requests_by_continuation."""

    async def test_cancel_request_pending(self, db_path: Path) -> None:
        """Test cancelling a pending request."""
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            # Create a pending request
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url,
                                         continuation, current_location)
                    VALUES ('pending', 5, 1, 'GET', 'https://example.com/test', 'parse', '')
                    """)
                )
                await session.commit()

            # Cancel the request
            cancelled = await driver.cancel_request(1)

            assert cancelled is True

            # Verify status changed
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT status, last_error FROM requests WHERE id = 1"
                    )
                )
                row = result.first()
            assert row[0] == "failed"
            assert "Cancelled" in row[1]

    async def test_cancel_request_held(self, db_path: Path) -> None:
        """Test cancelling a held request."""
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            # Create a held request
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url,
                                         continuation, current_location)
                    VALUES ('held', 5, 1, 'GET', 'https://example.com/test', 'parse', '')
                    """)
                )
                await session.commit()

            # Cancel the request
            cancelled = await driver.cancel_request(1)

            assert cancelled is True

            # Verify status changed
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT status FROM requests WHERE id = 1")
                )
                row = result.first()
            assert row[0] == "failed"

    async def test_cancel_request_in_progress_fails(
        self, db_path: Path
    ) -> None:
        """Test that cancelling an in_progress request fails."""
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            # Create an in_progress request
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url,
                                         continuation, current_location)
                    VALUES ('in_progress', 5, 1, 'GET', 'https://example.com/test', 'parse', '')
                    """)
                )
                await session.commit()

            # Try to cancel - should fail
            cancelled = await driver.cancel_request(1)

            assert cancelled is False

            # Verify status unchanged
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT status FROM requests WHERE id = 1")
                )
                row = result.first()
            assert row[0] == "in_progress"

    async def test_cancel_request_not_found(self, db_path: Path) -> None:
        """Test cancelling a non-existent request returns False."""
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            cancelled = await driver.cancel_request(999)
            assert cancelled is False

    async def test_cancel_requests_by_continuation(
        self, db_path: Path
    ) -> None:
        """Test cancelling all requests by continuation."""
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            # Create requests with different continuations and statuses
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url,
                                         continuation, current_location)
                    VALUES
                        ('pending', 5, 1, 'GET', 'https://example.com/1', 'parse_detail', ''),
                        ('pending', 5, 2, 'GET', 'https://example.com/2', 'parse_detail', ''),
                        ('held', 5, 3, 'GET', 'https://example.com/3', 'parse_detail', ''),
                        ('in_progress', 5, 4, 'GET', 'https://example.com/4', 'parse_detail', ''),
                        ('pending', 5, 5, 'GET', 'https://example.com/5', 'parse_listing', '')
                    """)
                )
                await session.commit()

            # Cancel all parse_detail requests
            count = await driver.cancel_requests_by_continuation(
                "parse_detail"
            )

            # Should cancel 3 (2 pending + 1 held, not in_progress)
            assert count == 3

            # Verify statuses
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("""
                    SELECT id, status FROM requests WHERE continuation = 'parse_detail'
                    ORDER BY id
                    """)
                )
                rows = result.all()

            # First 3 should be failed
            assert rows[0][1] == "failed"
            assert rows[1][1] == "failed"
            assert rows[2][1] == "failed"
            # Fourth (in_progress) should be unchanged
            assert rows[3][1] == "in_progress"

            # parse_listing should be unchanged
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT status FROM requests WHERE continuation = 'parse_listing'"
                    )
                )
                row = result.first()
            assert row[0] == "pending"

    async def test_cancel_requests_by_continuation_empty(
        self, db_path: Path
    ) -> None:
        """Test cancelling by continuation when none exist."""
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
            scraper, db_path, enable_monitor=False
        ) as driver:
            count = await driver.cancel_requests_by_continuation("nonexistent")
            assert count == 0
