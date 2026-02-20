"""Tests for request deduplication and lineage tracking."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa


class TestDeduplication:
    """Tests for request deduplication key checking."""

    @pytest.fixture
    def mock_scraper(self) -> Any:
        """Create a mock scraper for testing."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
        )

        class MockScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
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

    async def test_duplicate_requests_are_skipped(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that requests with the same deduplication_key are skipped.

        This simulates a scraper that would generate redundant data if
        deduplication wasn't working - e.g., a scraper that yields the same
        request multiple times from parsing the same page.
        """
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        async with PersistentDriver.open(
            mock_scraper, db_path, enable_monitor=False
        ) as driver:
            # Create a fake response to use as context for queueing
            parent_request = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/listing",
                ),
                continuation="parse_listing",
                current_location="https://example.com",
            )
            response = Response(
                request=parent_request,
                status_code=200,
                headers={},
                content=b"<html></html>",
                text="<html></html>",
                url="https://example.com/listing",
            )

            # Create multiple requests to the same URL - they should have
            # the same deduplication key by default (based on URL + method)
            request1 = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/detail/123",
                ),
                continuation="parse_detail",
                current_location="",
            )

            # Second request to exact same URL - should be deduplicated
            request2 = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/detail/123",
                ),
                continuation="parse_detail",
                current_location="",
            )

            # Third request also to same URL - should also be deduplicated
            request3 = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/detail/123",
                ),
                continuation="parse_detail",
                current_location="",
            )

            # Queue all three requests
            await driver.enqueue_request(request1, response)
            await driver.enqueue_request(request2, response)
            await driver.enqueue_request(request3, response)

            # Only ONE request should be in the queue due to deduplication
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE url = 'https://example.com/detail/123'"
                    )
                )
                count = result.first()[0]

            assert count == 1, (
                f"Expected 1 request due to deduplication, got {count}. "
                "Duplicate requests should be skipped."
            )

    async def test_different_urls_are_not_deduplicated(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that requests to different URLs are not deduplicated."""
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        async with PersistentDriver.open(
            mock_scraper, db_path, enable_monitor=False
        ) as driver:
            parent_request = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/listing",
                ),
                continuation="parse_listing",
                current_location="https://example.com",
            )
            response = Response(
                request=parent_request,
                status_code=200,
                headers={},
                content=b"<html></html>",
                text="<html></html>",
                url="https://example.com/listing",
            )

            # Create requests to DIFFERENT URLs - none should be deduplicated
            urls = [
                "https://example.com/detail/1",
                "https://example.com/detail/2",
                "https://example.com/detail/3",
            ]

            for url in urls:
                request = Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=url,
                    ),
                    continuation="parse_detail",
                    current_location="",
                )
                await driver.enqueue_request(request, response)

            # All three should be in the queue
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE url LIKE 'https://example.com/detail/%'"
                    )
                )
                count = result.first()[0]

            assert count == 3, (
                f"Expected 3 different requests, got {count}. "
                "Requests with different URLs should not be deduplicated."
            )

    async def test_cycle_prevention_via_deduplication(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that deduplication prevents cycles (A -> B -> A).

        This simulates a scraper where:
        - Page A links to Page B
        - Page B links back to Page A

        Without deduplication, this would create an infinite loop.
        With deduplication, the second request to A should be skipped.
        """
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        async with PersistentDriver.open(
            mock_scraper, db_path, enable_monitor=False
        ) as driver:
            url_a = "https://example.com/page-a"
            url_b = "https://example.com/page-b"

            # First: Simulate page A being visited and requesting page B
            request_a = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=url_a,
                ),
                continuation="parse",
                current_location="https://example.com",
            )
            response_a = Response(
                request=request_a,
                status_code=200,
                headers={},
                content=b"<html></html>",
                text="<html></html>",
                url=url_a,
            )

            # Queue the initial request to page A (entry point)
            await driver.enqueue_request(request_a, response_a)

            # Verify page A is queued
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT COUNT(*) FROM requests WHERE url = :url"),
                    {"url": url_a},
                )
                assert result.first()[0] == 1

            # Now simulate: parsing page A yields a request to page B
            request_b = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=url_b,
                ),
                continuation="parse",
                current_location=url_a,
            )
            await driver.enqueue_request(request_b, response_a)

            # Verify page B is queued
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT COUNT(*) FROM requests WHERE url = :url"),
                    {"url": url_b},
                )
                assert result.first()[0] == 1

            # Now simulate: parsing page B yields a request BACK to page A
            # This is where the cycle would happen without deduplication
            response_b = Response(
                request=request_b,
                status_code=200,
                headers={},
                content=b"<html></html>",
                text="<html></html>",
                url=url_b,
            )
            request_a_again = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=url_a,  # Same URL as before!
                ),
                continuation="parse",
                current_location=url_b,
            )
            await driver.enqueue_request(request_a_again, response_b)

            # Page A should STILL have only 1 request due to deduplication
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT COUNT(*) FROM requests WHERE url = :url"),
                    {"url": url_a},
                )
                count_a = result.first()[0]

            assert count_a == 1, (
                f"Expected 1 request to page A (cycle prevented), got {count_a}. "
                "Deduplication should prevent the cycle by skipping the second request to A."
            )

            # Total requests should be exactly 2 (A and B)
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("SELECT COUNT(*) FROM requests")
                )
                total = result.first()[0]

            assert total == 2, (
                f"Expected exactly 2 requests (A and B), got {total}. "
                "The cycle A -> B -> A should have been prevented."
            )

    async def test_custom_deduplication_key(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that custom deduplication keys work correctly.

        Sometimes scrapers need to define custom deduplication logic -
        for example, when the same URL with different query params
        should be considered duplicates.
        """
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        async with PersistentDriver.open(
            mock_scraper, db_path, enable_monitor=False
        ) as driver:
            parent_request = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/search",
                ),
                continuation="parse",
                current_location="https://example.com",
            )
            response = Response(
                request=parent_request,
                status_code=200,
                headers={},
                content=b"<html></html>",
                text="<html></html>",
                url="https://example.com/search",
            )

            # Two requests with DIFFERENT URLs but SAME custom dedup key
            # This simulates e.g. pagination where page=1 and page=2 should
            # still dedupe based on the item ID, not the page number
            request1 = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/item/123?page=1",
                ),
                continuation="parse_item",
                current_location="",
                deduplication_key="item-123",  # Custom key based on item ID
            )

            request2 = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/item/123?page=2",  # Different URL
                ),
                continuation="parse_item",
                current_location="",
                deduplication_key="item-123",  # Same custom key
            )

            await driver.enqueue_request(request1, response)
            await driver.enqueue_request(request2, response)

            # Only ONE request should be queued due to same custom dedup key
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE url LIKE 'https://example.com/item/123%'"
                    )
                )
                count = result.first()[0]

            assert count == 1, (
                f"Expected 1 request (custom dedup key), got {count}. "
                "Requests with the same custom deduplication_key should be deduplicated."
            )

    async def test_skip_deduplication_check(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that SkipDeduplicationCheck allows duplicate requests.

        Some scrapers need to intentionally make the same request multiple
        times (e.g., polling endpoints). SkipDeduplicationCheck allows this.
        """
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
            SkipDeduplicationCheck,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        async with PersistentDriver.open(
            mock_scraper, db_path, enable_monitor=False
        ) as driver:
            parent_request = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/poll",
                ),
                continuation="parse",
                current_location="https://example.com",
            )
            response = Response(
                request=parent_request,
                status_code=200,
                headers={},
                content=b"<html></html>",
                text="<html></html>",
                url="https://example.com/poll",
            )

            # Create requests with SkipDeduplicationCheck - duplicates allowed
            for _ in range(3):
                request = Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/status/check",  # Same URL
                    ),
                    continuation="check_status",
                    current_location="",
                    deduplication_key=SkipDeduplicationCheck(),
                )
                await driver.enqueue_request(request, response)

            # All THREE requests should be in the queue
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE url = 'https://example.com/status/check'"
                    )
                )
                count = result.first()[0]

            assert count == 3, (
                f"Expected 3 requests (SkipDeduplicationCheck), got {count}. "
                "SkipDeduplicationCheck should allow duplicate requests."
            )

    async def test_dedup_with_post_data(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that POST requests with same URL but different body are not deduplicated.

        The default deduplication includes the request body, so same URL
        with different POST data should be considered different requests.
        """
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        async with PersistentDriver.open(
            mock_scraper, db_path, enable_monitor=False
        ) as driver:
            parent_request = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/form",
                ),
                continuation="parse",
                current_location="https://example.com",
            )
            response = Response(
                request=parent_request,
                status_code=200,
                headers={},
                content=b"<html></html>",
                text="<html></html>",
                url="https://example.com/form",
            )

            # POST requests to same URL with different data
            request1 = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.POST,
                    url="https://example.com/submit",
                    data={"action": "search", "query": "first"},
                ),
                continuation="parse_results",
                current_location="",
            )

            request2 = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.POST,
                    url="https://example.com/submit",  # Same URL
                    data={
                        "action": "search",
                        "query": "second",
                    },  # Different data
                ),
                continuation="parse_results",
                current_location="",
            )

            await driver.enqueue_request(request1, response)
            await driver.enqueue_request(request2, response)

            # Both should be queued - different body means different dedup key
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE url = 'https://example.com/submit'"
                    )
                )
                count = result.first()[0]

            assert count == 2, (
                f"Expected 2 requests (different POST data), got {count}. "
                "POST requests with different body should not be deduplicated."
            )

    async def test_dedup_with_same_post_data(
        self, db_path: Path, mock_scraper: Any
    ) -> None:
        """Test that POST requests with same URL AND same body ARE deduplicated."""
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        async with PersistentDriver.open(
            mock_scraper, db_path, enable_monitor=False
        ) as driver:
            parent_request = Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url="https://example.com/form",
                ),
                continuation="parse",
                current_location="https://example.com",
            )
            response = Response(
                request=parent_request,
                status_code=200,
                headers={},
                content=b"<html></html>",
                text="<html></html>",
                url="https://example.com/form",
            )

            # POST requests to same URL with SAME data (identical requests)
            for _ in range(3):
                request = Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.POST,
                        url="https://example.com/submit",
                        data={"action": "search", "query": "same"},
                    ),
                    continuation="parse_results",
                    current_location="",
                )
                await driver.enqueue_request(request, response)

            # Only ONE should be queued - same URL + same body = same dedup key
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM requests WHERE url = 'https://example.com/submit'"
                    )
                )
                count = result.first()[0]

            assert count == 1, (
                f"Expected 1 request (same POST data = deduped), got {count}. "
                "Identical POST requests should be deduplicated."
            )


class TestRequestLineageTracking:
    """Tests for request lineage tracking (parent_request_id)."""

    async def test_child_requests_track_parent(self, db_path: Path) -> None:
        """Test that child requests properly track their parent request.

        This simulates a multi-step scraper where:
        - Entry request goes to /listing
        - /listing yields requests to /detail/1, /detail/2
        - Each detail request should track /listing as parent
        """

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

        class MultiStepScraper(BaseScraper[str]):
            """Scraper that navigates from listing to detail pages."""

            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/listing",
                    ),
                    continuation="parse_listing",
                    current_location="https://example.com",
                )

            def parse_listing(
                self, response: Response
            ) -> Generator[Request, None, None]:
                """Parse listing page and yield detail requests."""
                for i in range(3):
                    yield Request(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"https://example.com/detail/{i}",
                        ),
                        continuation="parse_detail",
                        current_location=response.url,
                    )

            def parse_detail(
                self, response: Response
            ) -> Generator[None, None, None]:
                """Parse detail page (no further requests)."""
                yield None

        # Create request manager with mock responses
        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/listing",
            create_html_response("<html>Listing</html>"),
        )
        for i in range(3):
            request_manager.add_response(
                f"https://example.com/detail/{i}",
                create_html_response(f"<html>Detail {i}</html>"),
            )

        scraper = MultiStepScraper()
        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            # Run the scraper
            await driver.run()

            # Verify parent-child relationships

            # Get the listing request ID
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT id FROM requests WHERE url = 'https://example.com/listing'"
                    )
                )
                listing_row = result.first()
            assert listing_row is not None
            listing_id = listing_row[0]

            # Check that all detail requests have listing as parent
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("""
                    SELECT url, parent_request_id FROM requests
                    WHERE url LIKE 'https://example.com/detail/%'
                    ORDER BY url
                    """)
                )
                detail_rows = result.all()

            assert len(detail_rows) == 3, "Should have 3 detail requests"

            for url, parent_id in detail_rows:
                assert parent_id == listing_id, (
                    f"Request to {url} should have parent_request_id={listing_id}, "
                    f"got {parent_id}"
                )

    async def test_archive_request_tracks_parent(self, db_path: Path) -> None:
        """Test that archive Request yields properly track their parent.

        This verifies the fix for JURI-gih9 where archive Requests (like PDF downloads)
        were not having their parent_request_id populated.
        """
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
            MockResponse,
            TestRequestManager,
            create_html_response,
        )

        class PDFDownloadScraper(BaseScraper[str]):
            """Scraper that yields archive Requests for PDFs."""

            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/index",
                    ),
                    continuation="parse_index",
                    current_location="https://example.com",
                )

            def parse_index(
                self, response: Response
            ) -> Generator[Request, None, None]:
                """Parse index and yield archive requests for PDFs."""
                for i in range(3):
                    yield Request(
                        archive=True,
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"https://example.com/doc{i}.pdf",
                        ),
                        continuation="handle_pdf",
                        current_location=response.url,
                    )

            def handle_pdf(
                self, response: Response
            ) -> Generator[None, None, None]:
                """Handle PDF download (no further requests)."""
                yield None

        # Create request manager with mock responses
        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/index",
            create_html_response("<html>Index</html>"),
        )
        for i in range(3):
            # Create PDF response manually
            request_manager.add_response(
                f"https://example.com/doc{i}.pdf",
                MockResponse(
                    content=b"fake pdf content",
                    text="",
                    status_code=200,
                    headers={"Content-Type": "application/pdf"},
                ),
            )

        scraper = PDFDownloadScraper()
        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            # Run the scraper
            await driver.run()

            # Verify parent-child relationships

            # Get the index request ID
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT id FROM requests WHERE url = 'https://example.com/index'"
                    )
                )
                index_row = result.first()
            assert index_row is not None
            index_id = index_row[0]

            # Check that all PDF requests have index as parent
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text("""
                    SELECT url, parent_request_id FROM requests
                    WHERE url LIKE 'https://example.com/doc%.pdf'
                    ORDER BY url
                    """)
                )
                pdf_rows = result.all()

            assert len(pdf_rows) == 3, "Should have 3 PDF archive requests"

            for url, parent_id in pdf_rows:
                assert parent_id == index_id, (
                    f"Archive request to {url} should have parent_request_id={index_id}, "
                    f"got {parent_id}"
                )
