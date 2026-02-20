"""Tests for listing and filtering requests, responses, and results."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

import sqlalchemy as sa


class TestListingMethods:
    """Tests for web interface listing methods."""

    async def test_list_requests(self, initialized_db) -> None:
        """Test listing requests with filters and pagination."""
        from kent.driver.persistent_driver.persistent_driver import (
            Page,
            RequestRecord,
        )

        engine, session_factory = initialized_db
        # Create requests with various statuses
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (status, priority, queue_counter, request_type, method, url, continuation, current_location)
                VALUES
                ('pending', 9, 1, 'navigating', 'GET', 'https://example.com/1', 'parse', ''),
                ('pending', 9, 2, 'navigating', 'GET', 'https://example.com/2', 'parse', ''),
                ('completed', 9, 3, 'navigating', 'GET', 'https://example.com/3', 'parse', ''),
                ('failed', 9, 4, 'navigating', 'GET', 'https://example.com/4', 'process', '')
                """)
            )
            await session.commit()

        # Test helper to simulate list_requests
        async def list_requests(
            status: str | None = None,
            continuation: str | None = None,
            offset: int = 0,
            limit: int = 50,
        ) -> Page[RequestRecord]:
            conditions = []
            bind_params: dict = {}

            if status:
                conditions.append("status = :status")
                bind_params["status"] = status
            if continuation:
                conditions.append("continuation = :continuation")
                bind_params["continuation"] = continuation

            where_clause = (
                f"WHERE {' AND '.join(conditions)}" if conditions else ""
            )

            async with session_factory() as session:
                result = await session.execute(
                    sa.text(f"SELECT COUNT(*) FROM requests {where_clause}"),
                    bind_params,
                )
                row = result.first()
            total = row[0] if row else 0

            bind_params["limit"] = limit
            bind_params["offset"] = offset
            async with session_factory() as session:
                result = await session.execute(
                    sa.text(f"""
                    SELECT id, status, priority, queue_counter, method, url,
                           continuation, current_location, created_at, started_at,
                           completed_at, retry_count, cumulative_backoff, last_error
                    FROM requests
                    {where_clause}
                    ORDER BY priority ASC, queue_counter ASC
                    LIMIT :limit OFFSET :offset
                    """),
                    bind_params,
                )
                rows = result.all()

            items = [
                RequestRecord(
                    id=r[0],
                    status=r[1],
                    priority=r[2],
                    queue_counter=r[3],
                    method=r[4],
                    url=r[5],
                    continuation=r[6],
                    current_location=r[7],
                    created_at=r[8],
                    started_at=r[9],
                    completed_at=r[10],
                    retry_count=r[11],
                    cumulative_backoff=r[12],
                    last_error=r[13],
                )
                for r in rows
            ]

            return Page(items=items, total=total, offset=offset, limit=limit)

        # List all
        page = await list_requests()
        assert page.total == 4
        assert len(page.items) == 4

        # Filter by status
        page = await list_requests(status="pending")
        assert page.total == 2
        assert all(r.status == "pending" for r in page.items)

        # Filter by continuation
        page = await list_requests(continuation="process")
        assert page.total == 1
        assert page.items[0].continuation == "process"

        # Pagination
        page = await list_requests(offset=0, limit=2)
        assert page.total == 4
        assert len(page.items) == 2

        page = await list_requests(offset=2, limit=2)
        assert page.total == 4
        assert len(page.items) == 2

    async def test_list_responses(self, initialized_db) -> None:
        """Test listing responses with filters."""
        from kent.driver.persistent_driver.compression import (
            compress,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            Page,
            ResponseRecord,
        )

        engine, session_factory = initialized_db
        # Create request
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (id, status, priority, queue_counter, method, url,
                                      continuation, current_location)
                VALUES (1, 'completed', 9, 1, 'GET', 'https://example.com',
                        'parse', '')
                """)
            )

            # Store response data on request rows
            content = b"<html>Test</html>"
            compressed = compress(content)

            # Need separate request rows since response data is on the request row
            # Update the existing request to have parse continuation and response
            await session.execute(
                sa.text("""
                UPDATE requests SET
                    response_status_code = 200,
                    response_url = :url,
                    content_compressed = :compressed,
                    content_size_original = :original_size,
                    content_size_compressed = :compressed_size,
                    warc_record_id = 'uuid1'
                WHERE id = 1
                """),
                {
                    "url": "https://example.com/1",
                    "compressed": compressed,
                    "original_size": len(content),
                    "compressed_size": len(compressed),
                },
            )
            # Insert a second request with process continuation and response
            await session.execute(
                sa.text("""
                INSERT INTO requests (id, status, priority, queue_counter, method, url,
                                      continuation, current_location,
                                      response_status_code, response_url,
                                      content_compressed, content_size_original,
                                      content_size_compressed, warc_record_id)
                VALUES (2, 'completed', 9, 2, 'GET', 'https://example.com/2',
                        'process', '',
                        200, 'https://example.com/2',
                        :compressed, :original_size, :compressed_size, 'uuid2')
                """),
                {
                    "compressed": compressed,
                    "original_size": len(content),
                    "compressed_size": len(compressed),
                },
            )
            await session.commit()

        # Test helper
        async def list_responses(
            continuation: str | None = None,
        ) -> Page[ResponseRecord]:
            conditions = []
            bind_params: dict = {}

            if continuation:
                conditions.append("continuation = :continuation")
                bind_params["continuation"] = continuation

            base_where = "WHERE response_status_code IS NOT NULL"
            if conditions:
                base_where += " AND " + " AND ".join(conditions)

            async with session_factory() as session:
                result = await session.execute(
                    sa.text(f"SELECT COUNT(*) FROM requests {base_where}"),
                    bind_params,
                )
                row = result.first()
            total = row[0] if row else 0

            async with session_factory() as session:
                result = await session.execute(
                    sa.text(f"""
                    SELECT id, response_status_code, response_url, content_size_original,
                           content_size_compressed, continuation, response_created_at,
                           compression_dict_id
                    FROM requests
                    {base_where}
                    """),
                    bind_params,
                )
                rows = result.all()

            items = [
                ResponseRecord(
                    id=r[0],
                    status_code=r[1],
                    url=r[2],
                    content_size_original=r[3],
                    content_size_compressed=r[4],
                    continuation=r[5],
                    created_at=r[6],
                    compression_dict_id=r[7],
                )
                for r in rows
            ]

            return Page(items=items, total=total, offset=0, limit=50)

        # List all
        page = await list_responses()
        assert page.total == 2

        # Filter by continuation
        page = await list_responses(continuation="parse")
        assert page.total == 1
        assert page.items[0].continuation == "parse"

    async def test_record_to_json(self, initialized_db) -> None:
        """Test that records can be serialized to JSON."""
        from kent.driver.persistent_driver.persistent_driver import (
            Page,
            RequestRecord,
            ResponseRecord,
            ResultRecord,
        )

        # Test RequestRecord
        req = RequestRecord(
            id=1,
            status="pending",
            priority=9,
            queue_counter=1,
            method="GET",
            url="https://example.com",
            continuation="parse",
            current_location="",
            created_at="2024-01-01",
            started_at=None,
            completed_at=None,
            retry_count=0,
            cumulative_backoff=0.0,
            last_error=None,
        )
        req_json = req.to_json()
        parsed = json.loads(req_json)
        assert parsed["id"] == 1
        assert parsed["status"] == "pending"

        # Test ResponseRecord
        resp = ResponseRecord(
            id=1,
            status_code=200,
            url="https://example.com",
            content_size_original=1000,
            content_size_compressed=100,
            continuation="parse",
            created_at="2024-01-01",
            compression_dict_id=None,
        )
        resp_dict = resp.to_dict()
        assert resp_dict["compression_ratio"] == 10.0

        # Test ResultRecord
        result = ResultRecord(
            id=1,
            request_id=1,
            result_type="TestModel",
            data_json='{"name": "test"}',
            is_valid=True,
            validation_errors_json=None,
            created_at="2024-01-01",
        )
        result_dict = result.to_dict()
        assert result_dict["data"] == {"name": "test"}

        # Test Page
        page = Page(
            items=[req],
            total=10,
            offset=0,
            limit=1,
        )
        page_json = page.to_json()
        parsed_page = json.loads(page_json)
        assert parsed_page["total"] == 10
        assert parsed_page["has_more"] is True

    async def test_cancel_request(self, initialized_db) -> None:
        """Test cancelling a request."""
        engine, session_factory = initialized_db
        # Create pending request
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (id, status, priority, queue_counter, method, url,
                                      continuation, current_location)
                VALUES (1, 'pending', 9, 1, 'GET', 'https://example.com', 'parse', '')
                """)
            )
            await session.commit()

        # Cancel it
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                UPDATE requests
                SET status = 'failed', completed_at = CURRENT_TIMESTAMP,
                    last_error = 'Cancelled by user'
                WHERE id = 1 AND status IN ('pending', 'held')
                """)
            )
            await session.commit()
            cancelled = result.rowcount > 0
        assert cancelled

        # Verify status
        async with session_factory() as session:
            result = await session.execute(
                sa.text("SELECT status, last_error FROM requests WHERE id = 1")
            )
            row = result.first()
        assert row[0] == "failed"
        assert row[1] == "Cancelled by user"

    async def test_cancel_requests_by_continuation(
        self, initialized_db
    ) -> None:
        """Test batch cancelling requests by continuation."""
        engine, session_factory = initialized_db
        # Create multiple pending requests
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (status, priority, queue_counter, method, url,
                                      continuation, current_location)
                VALUES
                ('pending', 9, 1, 'GET', 'https://example.com/1', 'parse', ''),
                ('pending', 9, 2, 'GET', 'https://example.com/2', 'parse', ''),
                ('pending', 9, 3, 'GET', 'https://example.com/3', 'process', '')
                """)
            )
            await session.commit()

        # Cancel all parse requests
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                UPDATE requests
                SET status = 'failed', completed_at = CURRENT_TIMESTAMP,
                    last_error = 'Cancelled by user (batch)'
                WHERE continuation = 'parse' AND status IN ('pending', 'held')
                """)
            )
            await session.commit()
            count = result.rowcount
        assert count == 2

        # Verify 'process' request is still pending
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    "SELECT status FROM requests WHERE continuation = 'process'"
                )
            )
            row = result.first()
        assert row[0] == "pending"


class TestListRequestsFiltering:
    """Tests for list_requests with various status filters."""

    async def test_list_requests_by_status(self, db_path: Path) -> None:
        """Test that list_requests correctly filters by status."""
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
            # Create requests with various statuses
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                    VALUES
                        ('pending', 5, 1, 'GET', 'https://example.com/pending1', 'parse', ''),
                        ('pending', 5, 2, 'GET', 'https://example.com/pending2', 'parse', ''),
                        ('in_progress', 5, 3, 'GET', 'https://example.com/in_progress', 'parse', ''),
                        ('completed', 5, 4, 'GET', 'https://example.com/completed1', 'parse', ''),
                        ('completed', 5, 5, 'GET', 'https://example.com/completed2', 'parse', ''),
                        ('completed', 5, 6, 'GET', 'https://example.com/completed3', 'parse', ''),
                        ('failed', 5, 7, 'GET', 'https://example.com/failed', 'parse', ''),
                        ('held', 5, 8, 'GET', 'https://example.com/held', 'parse', '')
                    """)
                )
                await session.commit()

            # Test filtering by 'pending' status
            pending_page = await driver.list_requests(status="pending")
            assert pending_page.total == 2
            assert all(r.status == "pending" for r in pending_page.items)

            # Test filtering by 'completed' status
            completed_page = await driver.list_requests(status="completed")
            assert completed_page.total == 3
            assert all(r.status == "completed" for r in completed_page.items)

            # Test filtering by 'failed' status
            failed_page = await driver.list_requests(status="failed")
            assert failed_page.total == 1
            assert failed_page.items[0].status == "failed"

            # Test filtering by 'held' status
            held_page = await driver.list_requests(status="held")
            assert held_page.total == 1
            assert held_page.items[0].status == "held"

            # Test filtering by 'in_progress' status
            in_progress_page = await driver.list_requests(status="in_progress")
            assert in_progress_page.total == 1
            assert in_progress_page.items[0].status == "in_progress"

            # Test getting all (no filter)
            all_page = await driver.list_requests()
            assert all_page.total == 8

    async def test_list_requests_by_continuation(self, db_path: Path) -> None:
        """Test that list_requests correctly filters by continuation."""
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
                    INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                    VALUES
                        ('pending', 5, 1, 'GET', 'https://example.com/1', 'parse_listing', ''),
                        ('pending', 5, 2, 'GET', 'https://example.com/2', 'parse_listing', ''),
                        ('pending', 5, 3, 'GET', 'https://example.com/3', 'parse_detail', ''),
                        ('pending', 5, 4, 'GET', 'https://example.com/4', 'parse_detail', ''),
                        ('pending', 5, 5, 'GET', 'https://example.com/5', 'parse_detail', '')
                    """)
                )
                await session.commit()

            # Filter by parse_listing
            listing_page = await driver.list_requests(
                continuation="parse_listing"
            )
            assert listing_page.total == 2
            assert all(
                r.continuation == "parse_listing" for r in listing_page.items
            )

            # Filter by parse_detail
            detail_page = await driver.list_requests(
                continuation="parse_detail"
            )
            assert detail_page.total == 3
            assert all(
                r.continuation == "parse_detail" for r in detail_page.items
            )

    async def test_list_requests_pagination(self, db_path: Path) -> None:
        """Test that list_requests correctly handles pagination."""
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
            # Create 10 requests
            async with driver.db._session_factory() as session:
                for i in range(10):
                    await session.execute(
                        sa.text("""
                        INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                        VALUES ('pending', 5, :queue_counter, 'GET', :url, 'parse', '')
                        """),
                        {
                            "queue_counter": i,
                            "url": f"https://example.com/page{i}",
                        },
                    )
                await session.commit()

            # Get first page (limit=3)
            page1 = await driver.list_requests(limit=3, offset=0)
            assert page1.total == 10
            assert len(page1.items) == 3
            assert page1.offset == 0
            assert page1.limit == 3
            # has_more can be computed: offset + len(items) < total
            assert (
                page1.offset + len(page1.items) < page1.total
            )  # More pages exist

            # Get second page
            page2 = await driver.list_requests(limit=3, offset=3)
            assert page2.total == 10
            assert len(page2.items) == 3
            assert page2.offset == 3
            assert (
                page2.offset + len(page2.items) < page2.total
            )  # More pages exist

            # Get last page (partial)
            page4 = await driver.list_requests(limit=3, offset=9)
            assert page4.total == 10
            assert len(page4.items) == 1
            # No more items after this page
            assert page4.offset + len(page4.items) == page4.total


class TestResponsesAndResultsListing:
    """Tests for list_responses and list_results methods."""

    async def test_list_responses_filtering(self, db_path: Path) -> None:
        """Test list_responses with continuation filter."""
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
            scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Create requests
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url,
                                         continuation, current_location)
                    VALUES
                        ('completed', 5, 1, 'GET', 'https://example.com/1', 'parse_listing', ''),
                        ('completed', 5, 2, 'GET', 'https://example.com/2', 'parse_detail', ''),
                        ('completed', 5, 3, 'GET', 'https://example.com/3', 'parse_detail', '')
                    """)
                )

                # Create responses
                for req_id, _cont, url in [
                    (1, "parse_listing", "https://example.com/1"),
                    (2, "parse_detail", "https://example.com/2"),
                    (3, "parse_detail", "https://example.com/3"),
                ]:
                    content = f"Content {req_id}".encode()
                    compressed = compress(content)
                    await session.execute(
                        sa.text("""
                        UPDATE requests SET
                            response_status_code = 200,
                            response_headers_json = '{}',
                            response_url = :url,
                            content_compressed = :content_compressed,
                            content_size_original = :content_size_original,
                            content_size_compressed = :content_size_compressed,
                            warc_record_id = :warc_record_id
                        WHERE id = :request_id
                        """),
                        {
                            "request_id": req_id,
                            "url": url,
                            "content_compressed": compressed,
                            "content_size_original": len(content),
                            "content_size_compressed": len(compressed),
                            "warc_record_id": str(uuid.uuid4()),
                        },
                    )
                await session.commit()

            # Test filtering by continuation
            listing_page = await driver.list_responses(
                continuation="parse_listing"
            )
            assert listing_page.total == 1
            assert listing_page.items[0].continuation == "parse_listing"

            detail_page = await driver.list_responses(
                continuation="parse_detail"
            )
            assert detail_page.total == 2
            assert all(
                r.continuation == "parse_detail" for r in detail_page.items
            )

            # Test getting all
            all_page = await driver.list_responses()
            assert all_page.total == 3

    async def test_list_responses_pagination(self, db_path: Path) -> None:
        """Test list_responses pagination."""
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
            scraper, db_path, initial_rate=100.0, enable_monitor=False
        ) as driver:
            # Create 10 requests and responses
            async with driver.db._session_factory() as session:
                for i in range(10):
                    await session.execute(
                        sa.text("""
                        INSERT INTO requests (status, priority, queue_counter, method, url,
                                             continuation, current_location)
                        VALUES ('completed', 5, :queue_counter, 'GET', :url, 'parse', '')
                        """),
                        {
                            "queue_counter": i,
                            "url": f"https://example.com/{i}",
                        },
                    )
                    content = f"Content {i}".encode()
                    compressed = compress(content)
                    await session.execute(
                        sa.text("""
                        UPDATE requests SET
                            response_status_code = 200,
                            response_headers_json = '{}',
                            response_url = :url,
                            content_compressed = :content_compressed,
                            content_size_original = :content_size_original,
                            content_size_compressed = :content_size_compressed,
                            warc_record_id = :warc_record_id
                        WHERE id = :request_id
                        """),
                        {
                            "request_id": i + 1,
                            "url": f"https://example.com/{i}",
                            "content_compressed": compressed,
                            "content_size_original": len(content),
                            "content_size_compressed": len(compressed),
                            "warc_record_id": str(uuid.uuid4()),
                        },
                    )
                await session.commit()

            # Test pagination
            page1 = await driver.list_responses(limit=3, offset=0)
            assert page1.total == 10
            assert len(page1.items) == 3

            page2 = await driver.list_responses(limit=3, offset=3)
            assert len(page2.items) == 3
            assert page2.offset == 3

    async def test_list_results_filtering(self, db_path: Path) -> None:
        """Test list_results with filters."""
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
            # Create results of different types and validity
            async with driver.db._session_factory() as session:
                await session.execute(
                    sa.text("""
                    INSERT INTO results (result_type, data_json, is_valid, validation_errors_json)
                    VALUES
                        ('CaseData', '{"id": 1}', 1, NULL),
                        ('CaseData', '{"id": 2}', 1, NULL),
                        ('CaseData', '{"id": 3}', 0, '[{"error": "bad"}]'),
                        ('DocumentData', '{"id": 4}', 1, NULL)
                    """)
                )
                await session.commit()

            # Filter by result_type
            case_results = await driver.list_results(result_type="CaseData")
            assert case_results.total == 3

            doc_results = await driver.list_results(result_type="DocumentData")
            assert doc_results.total == 1

            # Filter by is_valid
            valid_results = await driver.list_results(is_valid=True)
            assert valid_results.total == 3

            invalid_results = await driver.list_results(is_valid=False)
            assert invalid_results.total == 1
            assert not invalid_results.items[0].is_valid
