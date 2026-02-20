"""Tests for LocalDevDriverDebugger comparison methods.

Tests for compare_continuation, get_child_requests_transitive,
get_results_for_request, and sample_terminal_requests.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa

from kent.driver.dev_driver.compression import compress
from kent.driver.dev_driver.debugger import (
    LocalDevDriverDebugger,
)
from kent.driver.dev_driver.sql_manager import SQLManager


class TestComparisonMethods:
    """Tests for comparison-related methods."""

    async def test_get_child_requests_transitive(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test getting child requests transitively."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a parent request
        parent_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/parent",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="step1",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Create a child request
        child_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/child",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="step2",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=parent_id,
        )

        # Create a grandchild request
        grandchild_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/grandchild",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="step3",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=child_id,
        )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Get all transitive children of parent
            children = await debugger.get_child_requests_transitive(parent_id)

            # Should get both child and grandchild
            assert len(children) == 2
            child_ids = {r.id for r in children}
            assert child_id in child_ids
            assert grandchild_id in child_ids

    async def test_get_results_for_request(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test getting results for a request."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a request
        request_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/test",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="step1",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Store some results
        await sql_manager.store_result(
            request_id=request_id,
            result_type="TestData",
            data_json='{"field": "value1"}',
            is_valid=True,
        )

        await sql_manager.store_result(
            request_id=request_id,
            result_type="TestData",
            data_json='{"field": "value2"}',
            is_valid=True,
        )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            results = await debugger.get_results_for_request(request_id)

            assert len(results) == 2
            assert all(r.request_id == request_id for r in results)
            assert all(r.result_type == "TestData" for r in results)

    async def test_sample_terminal_requests(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test sampling terminal requests (requests with no children)."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create some terminal requests (no children)
        terminal_ids = []
        for i in range(5):
            req_id = await sql_manager.insert_request(
                priority=1,
                request_type="navigating",
                method="GET",
                url=f"https://example.com/terminal{i}",
                headers_json="{}",
                cookies_json="{}",
                body=None,
                continuation="step1",
                current_location="",
                accumulated_data_json="{}",
                aux_data_json="{}",
                permanent_json="{}",
                expected_type=None,
                dedup_key=None,
                parent_id=None,
            )
            # Mark as completed
            async with session_factory() as session:
                await session.execute(
                    sa.text(
                        "UPDATE requests SET status = :status WHERE id = :id"
                    ),
                    {"status": "completed", "id": req_id},
                )
                await session.commit()
            terminal_ids.append(req_id)

        # Create a non-terminal request (has children)
        parent_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/parent",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="step1",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": parent_id},
            )
            await session.commit()

        # Add a child to make it non-terminal
        await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/child",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="step2",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=parent_id,
        )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Sample 3 terminal requests
            sampled = await debugger.sample_terminal_requests("step1", 3)

            # Should get exactly 3
            assert len(sampled) == 3
            # All should be from our terminal requests
            assert all(req_id in terminal_ids for req_id in sampled)
            # Parent should not be sampled (it has children)
            assert parent_id not in sampled

    @pytest.mark.asyncio
    async def test_compare_continuation_identical_output(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test compare_continuation with identical outputs."""
        from kent.common.data_models import ScrapedData
        from kent.data_types import (
            BaseScraper,
            ParsedData,
            Request,
            Response,
        )

        class SampleData(ScrapedData):
            title: str
            value: int

        class TestScraper(BaseScraper[SampleData]):
            def get_entry(self):
                yield Request(
                    request={"method": "GET", "url": "https://example.com"},
                    continuation="parse_index",
                )

            def parse_index(self, response: Response):
                # Yield same data as stored
                yield ParsedData(SampleData(title="Test Item", value=100))

        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a request
        req_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/index",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_index",
            current_location="https://example.com",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Create a response
        content = b"<html>Test</html>"
        compressed_content = compress(content)
        await sql_manager.store_response(
            request_id=req_id,
            status_code=200,
            headers_json='{"Content-Type": "text/html"}',
            url="https://example.com/index",
            compressed_content=compressed_content,
            content_size_original=len(content),
            content_size_compressed=len(compressed_content),
            dict_id=None,
            continuation="parse_index",
            warc_record_id=str(uuid.uuid4()),
            speculation_outcome=None,
        )

        # Mark request as completed
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": req_id},
            )
            await session.commit()

        # Insert result (same as what scraper will yield)
        await sql_manager.store_result(
            request_id=req_id,
            result_type="SampleData",
            data_json='{"title": "Test Item", "value": 100}',
            is_valid=True,
            validation_errors_json=None,
        )
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.compare_continuation(req_id, TestScraper)

            # Outputs should be identical
            assert result.is_identical
            assert not result.has_changes
            assert result.data_diff.identical_pairs == 1
            assert len(result.data_diff.changed_pairs) == 0

    @pytest.mark.asyncio
    async def test_compare_continuation_with_data_changes(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test compare_continuation when new code yields different data."""
        from kent.common.data_models import ScrapedData
        from kent.data_types import (
            BaseScraper,
            ParsedData,
            Request,
            Response,
        )

        class SampleData(ScrapedData):
            title: str
            value: int

        class TestScraper(BaseScraper[SampleData]):
            def get_entry(self):
                yield Request(
                    request={"method": "GET", "url": "https://example.com"},
                    continuation="parse_index",
                )

            def parse_index(self, response: Response):
                # Yield different data than stored
                yield ParsedData(SampleData(title="Updated Item", value=200))

        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a request
        req_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/index",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_index",
            current_location="https://example.com",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Create a response
        content = b"<html>Test</html>"
        compressed_content = compress(content)
        await sql_manager.store_response(
            request_id=req_id,
            status_code=200,
            headers_json='{"Content-Type": "text/html"}',
            url="https://example.com/index",
            compressed_content=compressed_content,
            content_size_original=len(content),
            content_size_compressed=len(compressed_content),
            dict_id=None,
            continuation="parse_index",
            warc_record_id=str(uuid.uuid4()),
            speculation_outcome=None,
        )

        # Mark request as completed
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": req_id},
            )
            await session.commit()

        # Insert original result (different from what scraper will yield)
        await sql_manager.store_result(
            request_id=req_id,
            result_type="SampleData",
            data_json='{"title": "Test Item", "value": 100}',
            is_valid=True,
            validation_errors_json=None,
        )
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.compare_continuation(req_id, TestScraper)

            # Should detect changes
            assert not result.is_identical
            assert result.has_changes
            assert result.data_diff.has_changes
            assert len(result.data_diff.changed_pairs) == 1

            # Check field-level diffs
            orig, new, field_diffs = result.data_diff.changed_pairs[0]
            assert "title" in field_diffs
            assert field_diffs["title"] == ("Test Item", "Updated Item")
            assert "value" in field_diffs
            assert field_diffs["value"] == (100, 200)

    @pytest.mark.asyncio
    async def test_compare_continuation_with_request_changes(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test compare_continuation when new code yields different child requests."""
        from kent.common.data_models import ScrapedData
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )

        class SampleData(ScrapedData):
            title: str

        class TestScraper(BaseScraper[SampleData]):
            def get_entry(self):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url="https://example.com"
                    ),
                    continuation="parse_index",
                )

            def parse_index(self, response: Response):
                # Yield different child requests than originally stored
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/new_page",
                    ),
                    continuation="parse_detail",
                )

        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a parent request
        parent_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/index",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_index",
            current_location="https://example.com",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Create a response
        content = b"<html>Test</html>"
        compressed_content = compress(content)
        await sql_manager.store_response(
            request_id=parent_id,
            status_code=200,
            headers_json='{"Content-Type": "text/html"}',
            url="https://example.com/index",
            compressed_content=compressed_content,
            content_size_original=len(content),
            content_size_compressed=len(compressed_content),
            dict_id=None,
            continuation="parse_index",
            warc_record_id=str(uuid.uuid4()),
            speculation_outcome=None,
        )

        # Mark parent as completed
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": parent_id},
            )
            await session.commit()

        # Create an ORIGINAL child request (different from what new code will yield)
        await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/old_page",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_detail",
            current_location="https://example.com",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=parent_id,
        )
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.compare_continuation(
                parent_id, TestScraper
            )

            # Should detect request tree changes
            assert not result.is_identical
            assert result.has_changes
            assert result.request_diff.has_changes

            # Should have one removed (old_page) and one added (new_page)
            assert len(result.request_diff.removed) == 1
            assert (
                result.request_diff.removed[0].url
                == "https://example.com/old_page"
            )
            assert len(result.request_diff.added) == 1
            assert (
                result.request_diff.added[0].url
                == "https://example.com/new_page"
            )

    @pytest.mark.asyncio
    async def test_compare_continuation_with_error(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test compare_continuation when new code raises an error."""
        from kent.common.data_models import ScrapedData
        from kent.data_types import (
            BaseScraper,
            Request,
            Response,
        )

        class SampleData(ScrapedData):
            title: str

        class TestScraper(BaseScraper[SampleData]):
            def get_entry(self):
                yield Request(
                    request={"method": "GET", "url": "https://example.com"},
                    continuation="parse_index",
                )

            def parse_index(self, response: Response):
                # Raise an error
                raise ValueError("Test error from new code")

        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a request
        req_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/index",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_index",
            current_location="https://example.com",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Create a response
        content = b"<html>Test</html>"
        compressed_content = compress(content)
        await sql_manager.store_response(
            request_id=req_id,
            status_code=200,
            headers_json='{"Content-Type": "text/html"}',
            url="https://example.com/index",
            compressed_content=compressed_content,
            content_size_original=len(content),
            content_size_compressed=len(compressed_content),
            dict_id=None,
            continuation="parse_index",
            warc_record_id=str(uuid.uuid4()),
            speculation_outcome=None,
        )

        # Mark request as completed (original execution succeeded)
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": req_id},
            )
            await session.commit()
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.compare_continuation(req_id, TestScraper)

            # Should detect error introduced
            assert not result.is_identical
            assert result.has_changes
            assert result.error_diff.has_change
            assert result.error_diff.status == "introduced"
            assert result.error_diff.new_error.error_type == "ValueError"
            assert (
                "Test error from new code"
                in result.error_diff.new_error.error_message
            )

    @pytest.mark.asyncio
    async def test_compare_continuation_missing_response(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test compare_continuation raises error when response is missing."""
        from kent.common.data_models import ScrapedData
        from kent.data_types import BaseScraper

        class SampleData(ScrapedData):
            title: str

        class TestScraper(BaseScraper[SampleData]):
            def get_entry(self):
                pass

        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a request WITHOUT a response
        req_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/index",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_index",
            current_location="https://example.com",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Should raise error for missing response
            with pytest.raises(ValueError, match="No response found"):
                await debugger.compare_continuation(req_id, TestScraper)
