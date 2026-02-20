"""Tests for requeue operations (_requeue.py)."""

from __future__ import annotations

import json
import uuid

import sqlalchemy as sa

from kent.driver.persistent_driver.compression import compress
from kent.driver.persistent_driver.sql_manager import SQLManager


class TestRequeueOperations:
    """Tests for enhanced requeue operations."""

    async def test_requeue_requests_basic(
        self, sql_manager: SQLManager
    ) -> None:
        """Test basic requeue without clearing anything."""
        # Create a request
        req_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test",
            headers_json=json.dumps({"Accept": "text/html"}),
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="https://example.com",
            accumulated_data_json=json.dumps({"count": 1}),
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key="test",
            parent_id=None,
        )

        # Mark as completed
        await sql_manager.mark_request_in_progress(req_id)
        await sql_manager.mark_request_completed(req_id)

        # Requeue without clearing anything
        result = await sql_manager.requeue_requests([req_id])

        assert not result.dry_run
        assert len(result.requeued_request_ids) == 1
        assert result.cleared_response_ids == []
        assert result.cleared_downstream_request_ids == []
        assert result.cleared_result_ids == []
        assert result.cleared_error_ids == []

        # Verify new request was created with same parameters
        new_req_id = result.requeued_request_ids[0]
        new_req = await sql_manager.get_request(new_req_id)
        assert new_req is not None
        assert new_req.url == "https://example.com/test"
        assert new_req.method == "GET"
        assert new_req.continuation == "parse"
        assert new_req.status == "pending"

    async def test_requeue_requests_clear_responses(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue with clear_responses=True."""
        # Create request and response
        req_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        content = b"<html>Test</html>"
        compressed = compress(content)
        resp_id = await sql_manager.store_response(
            request_id=req_id,
            status_code=200,
            headers_json=None,
            url="https://example.com/test",
            compressed_content=compressed,
            content_size_original=len(content),
            content_size_compressed=len(compressed),
            dict_id=None,
            continuation="parse",
            warc_record_id=str(uuid.uuid4()),
        )

        # Requeue with clear_responses
        result = await sql_manager.requeue_requests(
            [req_id], clear_responses=True
        )

        assert len(result.requeued_request_ids) == 1
        assert result.cleared_response_ids == [resp_id]

        # Verify response was deleted
        response = await sql_manager.get_response(resp_id)
        assert response is None

    async def test_requeue_requests_clear_downstream(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue with clear_downstream=True."""
        # Create parent request
        parent_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/parent",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse_listing",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key="parent",
            parent_id=None,
        )

        # Create child request
        child_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/child",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse_detail",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key="child",
            parent_id=parent_id,
        )

        # Create grandchild request
        grandchild_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/grandchild",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse_item",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key="grandchild",
            parent_id=child_id,
        )

        # Add result to child
        child_result_id = await sql_manager.store_result(
            request_id=child_id,
            result_type="CaseData",
            data_json=json.dumps({"id": 1}),
            is_valid=True,
        )

        # Requeue parent with clear_downstream
        result = await sql_manager.requeue_requests(
            [parent_id], clear_downstream=True
        )

        assert len(result.requeued_request_ids) == 1
        # Should have cleared child and grandchild (but not parent)
        assert set(result.cleared_downstream_request_ids) == {
            child_id,
            grandchild_id,
        }
        assert child_result_id in result.cleared_result_ids

        # Verify downstream requests were deleted
        assert await sql_manager.get_request(child_id) is None
        assert await sql_manager.get_request(grandchild_id) is None

        # Verify parent still exists (not deleted, just requeued)
        assert await sql_manager.get_request(parent_id) is not None

    async def test_requeue_requests_clear_both(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue with both clear_responses and clear_downstream."""
        # Create parent with response
        parent_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/parent",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        content = b"Parent content"
        compressed = compress(content)
        parent_resp_id = await sql_manager.store_response(
            request_id=parent_id,
            status_code=200,
            headers_json=None,
            url="https://example.com/parent",
            compressed_content=compressed,
            content_size_original=len(content),
            content_size_compressed=len(compressed),
            dict_id=None,
            continuation="parse",
            warc_record_id=str(uuid.uuid4()),
        )

        # Create child with response
        child_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/child",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=parent_id,
        )

        child_content = b"Child content"
        child_compressed = compress(child_content)
        child_resp_id = await sql_manager.store_response(
            request_id=child_id,
            status_code=200,
            headers_json=None,
            url="https://example.com/child",
            compressed_content=child_compressed,
            content_size_original=len(child_content),
            content_size_compressed=len(child_compressed),
            dict_id=None,
            continuation="parse",
            warc_record_id=str(uuid.uuid4()),
        )

        # Requeue with both flags
        result = await sql_manager.requeue_requests(
            [parent_id], clear_responses=True, clear_downstream=True
        )

        # Should clear both parent and child responses
        assert set(result.cleared_response_ids) == {
            parent_resp_id,
            child_resp_id,
        }
        assert result.cleared_downstream_request_ids == [child_id]

        # Verify all responses deleted
        assert await sql_manager.get_response(parent_resp_id) is None
        assert await sql_manager.get_response(child_resp_id) is None

    async def test_requeue_requests_dry_run(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue with dry_run=True doesn't make changes."""
        req_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        content = b"Test"
        compressed = compress(content)
        resp_id = await sql_manager.store_response(
            request_id=req_id,
            status_code=200,
            headers_json=None,
            url="https://example.com/test",
            compressed_content=compressed,
            content_size_original=len(content),
            content_size_compressed=len(compressed),
            dict_id=None,
            continuation="parse",
            warc_record_id=str(uuid.uuid4()),
        )

        # Dry run
        result = await sql_manager.requeue_requests(
            [req_id], clear_responses=True, dry_run=True
        )

        assert result.dry_run
        assert result.cleared_response_ids == [resp_id]
        # Placeholder IDs in dry run
        assert len(result.requeued_request_ids) > 0

        # Verify nothing was actually changed
        response = await sql_manager.get_response(resp_id)
        assert response is not None  # Still exists

        # No new request created
        initial_count = await sql_manager.count_all_requests()
        assert initial_count == 1

    async def test_requeue_requests_empty_list(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue with empty request list."""
        result = await sql_manager.requeue_requests([])

        assert result.requeued_request_ids == []
        assert result.cleared_response_ids == []

    async def test_requeue_error_with_mark_resolved(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue_error with mark_resolved=True (default)."""
        # Create request
        req_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Insert error
        async with sql_manager._session_factory() as session:
            result = await session.execute(
                sa.text("""
                INSERT INTO errors (
                    request_id, error_type, error_class, message, request_url,
                    traceback
                ) VALUES (:request_id, :error_type, :error_class, :message, :request_url,
                    :traceback)
                """),
                {
                    "request_id": req_id,
                    "error_type": "structural",
                    "error_class": "ValueError",
                    "message": "Test error",
                    "request_url": "https://example.com/test",
                    "traceback": "Traceback...",
                },
            )
            await session.commit()
            error_id = result.lastrowid

        # Requeue error (mark_resolved defaults to True)
        requeue_result = await sql_manager.requeue_error(error_id)

        assert len(requeue_result.requeued_request_ids) == 1
        assert requeue_result.resolved_error_ids == [error_id]

        # Verify error was marked as resolved
        async with sql_manager._session_factory() as session:
            result = await session.execute(
                sa.text(
                    "SELECT is_resolved, resolution_notes FROM errors WHERE id = :id"
                ),
                {"id": error_id},
            )
            row = result.first()
        assert row[0] == 1  # is_resolved
        assert "Requeued as request" in row[1]

    async def test_requeue_error_without_mark_resolved(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue_error with mark_resolved=False."""
        req_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        async with sql_manager._session_factory() as session:
            result = await session.execute(
                sa.text("""
                INSERT INTO errors (
                    request_id, error_type, error_class, message, request_url,
                    traceback
                ) VALUES (:request_id, :error_type, :error_class, :message, :request_url,
                    :traceback)
                """),
                {
                    "request_id": req_id,
                    "error_type": "validation",
                    "error_class": "TypeError",
                    "message": "Test error",
                    "request_url": "https://example.com/test",
                    "traceback": "Traceback...",
                },
            )
            await session.commit()
            error_id = result.lastrowid

        # Requeue without marking resolved
        requeue_result = await sql_manager.requeue_error(
            error_id, mark_resolved=False
        )

        assert len(requeue_result.requeued_request_ids) == 1
        assert requeue_result.resolved_error_ids == []

        # Verify error was NOT marked as resolved
        async with sql_manager._session_factory() as session:
            result = await session.execute(
                sa.text("SELECT is_resolved FROM errors WHERE id = :id"),
                {"id": error_id},
            )
            row = result.first()
        assert row[0] == 0

    async def test_requeue_error_not_found(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue_error with nonexistent error ID."""
        result = await sql_manager.requeue_error(999)

        # Should return empty result
        assert result.requeued_request_ids == []
        assert result.resolved_error_ids == []

    async def test_requeue_error_dry_run(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue_error with dry_run=True."""
        req_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        async with sql_manager._session_factory() as session:
            result = await session.execute(
                sa.text("""
                INSERT INTO errors (
                    request_id, error_type, error_class, message, request_url,
                    traceback
                ) VALUES (:request_id, :error_type, :error_class, :message, :request_url,
                    :traceback)
                """),
                {
                    "request_id": req_id,
                    "error_type": "structural",
                    "error_class": "ValueError",
                    "message": "Test error",
                    "request_url": "https://example.com/test",
                    "traceback": "Traceback...",
                },
            )
            await session.commit()
            error_id = result.lastrowid

        # Dry run
        requeue_result = await sql_manager.requeue_error(
            error_id, dry_run=True
        )

        assert requeue_result.dry_run
        assert len(requeue_result.requeued_request_ids) > 0  # Placeholder
        assert (
            requeue_result.resolved_error_ids == []
        )  # Not resolved in dry run

        # Verify error still unresolved
        async with sql_manager._session_factory() as session:
            result = await session.execute(
                sa.text("SELECT is_resolved FROM errors WHERE id = :id"),
                {"id": error_id},
            )
            row = result.first()
        assert row[0] == 0

    async def test_requeue_continuation_basic(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue_continuation without filters."""
        # Create multiple completed requests
        for i in range(3):
            req_id = await sql_manager.insert_request(
                priority=5,
                request_type="navigating",
                method="GET",
                url=f"https://example.com/test{i}",
                headers_json=None,
                cookies_json=None,
                body=None,
                continuation="parse_listing",
                current_location="",
                accumulated_data_json=None,
                aux_data_json=None,
                permanent_json=None,
                expected_type=None,
                dedup_key=f"test{i}",
                parent_id=None,
            )
            await sql_manager.mark_request_in_progress(req_id)
            await sql_manager.mark_request_completed(req_id)

        # Add one with different continuation
        other_req_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/other",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse_detail",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key="other",
            parent_id=None,
        )
        await sql_manager.mark_request_in_progress(other_req_id)
        await sql_manager.mark_request_completed(other_req_id)

        # Requeue by continuation
        result = await sql_manager.requeue_continuation("parse_listing")

        # Should requeue all 3 parse_listing requests
        assert len(result.requeued_request_ids) == 3
        assert result.resolved_error_ids == []

    async def test_requeue_continuation_with_error_type_filter(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue_continuation with error_type filter."""
        # Create requests with different error types
        req1 = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test1",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key="1",
            parent_id=None,
        )

        req2 = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test2",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key="2",
            parent_id=None,
        )

        # Add structural error to req1
        async with sql_manager._session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO errors (
                    request_id, error_type, error_class, message, request_url,
                    traceback
                ) VALUES (:request_id, :error_type, :error_class, :message, :request_url,
                    :traceback)
                """),
                {
                    "request_id": req1,
                    "error_type": "structural",
                    "error_class": "ValueError",
                    "message": "Error 1",
                    "request_url": "url1",
                    "traceback": "tb1",
                },
            )

            # Add validation error to req2
            await session.execute(
                sa.text("""
                INSERT INTO errors (
                    request_id, error_type, error_class, message, request_url,
                    traceback
                ) VALUES (:request_id, :error_type, :error_class, :message, :request_url,
                    :traceback)
                """),
                {
                    "request_id": req2,
                    "error_type": "validation",
                    "error_class": "TypeError",
                    "message": "Error 2",
                    "request_url": "url2",
                    "traceback": "tb2",
                },
            )
            await session.commit()

        # Requeue only structural errors
        result = await sql_manager.requeue_continuation(
            "parse", error_type="structural"
        )

        # Should only requeue req1
        assert len(result.requeued_request_ids) == 1
        assert len(result.resolved_error_ids) == 1

    async def test_requeue_continuation_with_traceback_filter(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue_continuation with traceback_contains filter."""
        # Create requests
        req1 = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test1",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        req2 = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test2",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Add error with KeyError in traceback
        async with sql_manager._session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO errors (
                    request_id, error_type, error_class, message, request_url,
                    traceback
                ) VALUES (:request_id, :error_type, :error_class, :message, :request_url,
                    :traceback)
                """),
                {
                    "request_id": req1,
                    "error_type": "structural",
                    "error_class": "KeyError",
                    "message": "Error 1",
                    "request_url": "url1",
                    "traceback": "KeyError: 'missing_key'",
                },
            )

            # Add error with different traceback
            await session.execute(
                sa.text("""
                INSERT INTO errors (
                    request_id, error_type, error_class, message, request_url,
                    traceback
                ) VALUES (:request_id, :error_type, :error_class, :message, :request_url,
                    :traceback)
                """),
                {
                    "request_id": req2,
                    "error_type": "structural",
                    "error_class": "ValueError",
                    "message": "Error 2",
                    "request_url": "url2",
                    "traceback": "ValueError: invalid value",
                },
            )
            await session.commit()

        # Requeue only requests with KeyError in traceback
        result = await sql_manager.requeue_continuation(
            "parse", traceback_contains="KeyError"
        )

        # Should only requeue req1
        assert len(result.requeued_request_ids) == 1

    async def test_requeue_continuation_combined_filters(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue_continuation with both error_type and traceback filters."""
        req_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Add matching error
        async with sql_manager._session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO errors (
                    request_id, error_type, error_class, message, request_url,
                    traceback
                ) VALUES (:request_id, :error_type, :error_class, :message, :request_url,
                    :traceback)
                """),
                {
                    "request_id": req_id,
                    "error_type": "validation",
                    "error_class": "TypeError",
                    "message": "Error",
                    "request_url": "url",
                    "traceback": "expected str, got int",
                },
            )
            await session.commit()

        # Requeue with both filters matching
        result = await sql_manager.requeue_continuation(
            "parse", error_type="validation", traceback_contains="expected str"
        )

        assert len(result.requeued_request_ids) == 1
        assert len(result.resolved_error_ids) == 1

    async def test_requeue_continuation_no_matches(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeue_continuation with no matching requests."""
        result = await sql_manager.requeue_continuation("nonexistent")

        assert result.requeued_request_ids == []

    async def test_requeue_response(self, sql_manager: SQLManager) -> None:
        """Test requeue_response helper."""
        req_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/test",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        content = b"Test"
        compressed = compress(content)
        resp_id = await sql_manager.store_response(
            request_id=req_id,
            status_code=200,
            headers_json=None,
            url="https://example.com/test",
            compressed_content=compressed,
            content_size_original=len(content),
            content_size_compressed=len(compressed),
            dict_id=None,
            continuation="parse",
            warc_record_id=str(uuid.uuid4()),
        )

        # Requeue via response
        result = await sql_manager.requeue_response(resp_id)

        assert len(result.requeued_request_ids) == 1

    async def test_requeue_multiple_requests(
        self, sql_manager: SQLManager
    ) -> None:
        """Test requeuing multiple requests at once."""
        req_ids = []
        for i in range(5):
            req_id = await sql_manager.insert_request(
                priority=5,
                request_type="navigating",
                method="GET",
                url=f"https://example.com/test{i}",
                headers_json=None,
                cookies_json=None,
                body=None,
                continuation="parse",
                current_location="",
                accumulated_data_json=None,
                aux_data_json=None,
                permanent_json=None,
                expected_type=None,
                dedup_key=f"test{i}",
                parent_id=None,
            )
            req_ids.append(req_id)

        # Requeue all at once
        result = await sql_manager.requeue_requests(req_ids)

        assert len(result.requeued_request_ids) == 5
