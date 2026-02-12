"""Tests for LocalDevDriverDebugger integrity check and ghost request detection.

Tests for check_integrity, get_orphan_details, get_ghost_requests, and
check_estimates methods.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import sqlalchemy as sa

from kent.driver.dev_driver.compression import compress
from kent.driver.dev_driver.debugger import (
    LocalDevDriverDebugger,
)
from kent.driver.dev_driver.sql_manager import SQLManager


class TestIntegrityChecks:
    """Tests for integrity check methods."""

    async def test_check_integrity_no_issues(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_integrity when database has no issues."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a request with matching response (no orphans)
        req_id = await sql_manager.insert_request(
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

        # Mark as completed
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": req_id},
            )
            await session.commit()

        # Add matching response
        content = b"<html>Test</html>"
        compressed_content = compress(content)
        await sql_manager.store_response(
            request_id=req_id,
            status_code=200,
            headers_json="{}",
            url="https://example.com/test",
            compressed_content=compressed_content,
            content_size_original=len(content),
            content_size_compressed=len(compressed_content),
            dict_id=None,
            continuation="step1",
            warc_record_id=str(uuid.uuid4()),
            speculation_outcome=None,
        )
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_integrity()

            assert result["has_issues"] is False
            assert result["orphaned_requests"]["count"] == 0
            assert result["orphaned_responses"]["count"] == 0

    async def test_check_integrity_orphaned_request(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_integrity detects orphaned requests."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a completed request WITHOUT a response
        req_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/orphan",
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

        # Mark as completed (but no response exists)
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": req_id},
            )
            await session.commit()
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_integrity()

            assert result["has_issues"] is True
            assert result["orphaned_requests"]["count"] == 1
            assert req_id in result["orphaned_requests"]["ids"]
            assert result["orphaned_responses"]["count"] == 0

    async def test_check_integrity_orphaned_response(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_integrity detects orphaned responses."""
        engine, session_factory = initialized_db

        # Insert a response WITHOUT a matching request
        # (We'll temporarily disable foreign keys to allow this)
        content = b"<html>Orphan</html>"
        compressed_content = compress(content)

        async with session_factory() as session:
            await session.execute(sa.text("PRAGMA foreign_keys = OFF"))
            await session.execute(
                sa.text(
                    """
                    INSERT INTO responses (
                        request_id, status_code, headers_json, url,
                        content_compressed, content_size_original,
                        content_size_compressed, compression_dict_id, continuation,
                        warc_record_id, created_at
                    ) VALUES (:request_id, :status_code, :headers_json, :url,
                        :content_compressed, :content_size_original,
                        :content_size_compressed, :compression_dict_id, :continuation,
                        :warc_record_id, datetime('now'))
                    """
                ),
                {
                    "request_id": 9999,  # Non-existent request ID
                    "status_code": 200,
                    "headers_json": "{}",
                    "url": "https://example.com/orphan",
                    "content_compressed": compressed_content,
                    "content_size_original": len(content),
                    "content_size_compressed": len(compressed_content),
                    "compression_dict_id": None,
                    "continuation": "step1",
                    "warc_record_id": str(uuid.uuid4()),
                },
            )
            await session.execute(sa.text("PRAGMA foreign_keys = ON"))
            await session.commit()
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_integrity()

            assert result["has_issues"] is True
            assert result["orphaned_requests"]["count"] == 0
            assert result["orphaned_responses"]["count"] == 1
            # Response ID should be 1 (first response in the table)
            assert 1 in result["orphaned_responses"]["ids"]

    async def test_check_integrity_multiple_issues(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_integrity detects multiple types of issues."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create orphaned request
        orphan_req_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/orphan_req",
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
                {"status": "completed", "id": orphan_req_id},
            )
            await session.commit()

        # Create orphaned response (non-existent request)
        content = b"<html>Orphan Response</html>"
        compressed_content = compress(content)
        async with session_factory() as session:
            await session.execute(sa.text("PRAGMA foreign_keys = OFF"))
            await session.execute(
                sa.text(
                    """
                    INSERT INTO responses (
                        request_id, status_code, headers_json, url,
                        content_compressed, content_size_original,
                        content_size_compressed, compression_dict_id, continuation,
                        warc_record_id, created_at
                    ) VALUES (:request_id, :status_code, :headers_json, :url,
                        :content_compressed, :content_size_original,
                        :content_size_compressed, :compression_dict_id, :continuation,
                        :warc_record_id, datetime('now'))
                    """
                ),
                {
                    "request_id": 9999,
                    "status_code": 200,
                    "headers_json": "{}",
                    "url": "https://example.com/orphan_resp",
                    "content_compressed": compressed_content,
                    "content_size_original": len(content),
                    "content_size_compressed": len(compressed_content),
                    "compression_dict_id": None,
                    "continuation": "step2",
                    "warc_record_id": str(uuid.uuid4()),
                },
            )
            await session.execute(sa.text("PRAGMA foreign_keys = ON"))
            await session.commit()
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_integrity()

            assert result["has_issues"] is True
            assert result["orphaned_requests"]["count"] == 1
            assert result["orphaned_responses"]["count"] == 1

    async def test_get_orphan_details_no_orphans(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test get_orphan_details when there are no orphans."""
        engine, _ = initialized_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_orphan_details()

            assert len(result["orphaned_requests"]) == 0
            assert len(result["orphaned_responses"]) == 0

    async def test_get_orphan_details_with_orphaned_request(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test get_orphan_details includes request details."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create orphaned request
        req_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/orphan",
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
                {"status": "completed", "id": req_id},
            )
            await session.commit()
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_orphan_details()

            assert len(result["orphaned_requests"]) == 1
            req = result["orphaned_requests"][0]
            assert req["id"] == req_id
            assert req["url"] == "https://example.com/orphan"
            assert req["continuation"] == "step1"

    async def test_get_orphan_details_with_orphaned_response(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test get_orphan_details includes response details."""
        engine, session_factory = initialized_db

        # Create orphaned response
        content = b"<html>Orphan</html>"
        compressed_content = compress(content)
        async with session_factory() as session:
            await session.execute(sa.text("PRAGMA foreign_keys = OFF"))
            await session.execute(
                sa.text(
                    """
                    INSERT INTO responses (
                        request_id, status_code, headers_json, url,
                        content_compressed, content_size_original,
                        content_size_compressed, compression_dict_id, continuation,
                        warc_record_id, created_at
                    ) VALUES (:request_id, :status_code, :headers_json, :url,
                        :content_compressed, :content_size_original,
                        :content_size_compressed, :compression_dict_id, :continuation,
                        :warc_record_id, datetime('now'))
                    """
                ),
                {
                    "request_id": 9999,
                    "status_code": 404,
                    "headers_json": "{}",
                    "url": "https://example.com/orphan_response",
                    "content_compressed": compressed_content,
                    "content_size_original": len(content),
                    "content_size_compressed": len(compressed_content),
                    "compression_dict_id": None,
                    "continuation": "step2",
                    "warc_record_id": str(uuid.uuid4()),
                },
            )
            await session.execute(sa.text("PRAGMA foreign_keys = ON"))
            await session.commit()
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_orphan_details()

            assert len(result["orphaned_responses"]) == 1
            resp = result["orphaned_responses"][0]
            assert resp["id"] == 1
            assert resp["url"] == "https://example.com/orphan_response"


class TestGhostRequestDetection:
    """Tests for ghost request detection methods."""

    async def test_get_ghost_requests_no_ghosts(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test get_ghost_requests when there are no ghost requests."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a completed request with a result (not a ghost)
        req_id = await sql_manager.insert_request(
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

        # Mark as completed
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": req_id},
            )
            await session.commit()

        # Add a result (prevents it from being a ghost)
        await sql_manager.store_result(
            request_id=req_id,
            result_type="TestData",
            data_json='{"test": "data"}',
            is_valid=True,
        )
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_ghost_requests()

            assert result["total_count"] == 0
            assert len(result["by_continuation"]) == 0
            assert len(result["ghosts"]) == 0

    async def test_get_ghost_requests_detects_ghost(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test get_ghost_requests detects a ghost request."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a completed request with NO children and NO results (ghost)
        ghost_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/ghost",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_index",
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
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": ghost_id},
            )
            await session.commit()
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_ghost_requests()

            assert result["total_count"] == 1
            assert "parse_index" in result["by_continuation"]
            assert result["by_continuation"]["parse_index"] == 1
            assert len(result["ghosts"]) == 1
            assert result["ghosts"][0]["id"] == ghost_id
            assert result["ghosts"][0]["url"] == "https://example.com/ghost"
            assert result["ghosts"][0]["continuation"] == "parse_index"

    async def test_get_ghost_requests_not_ghost_with_children(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test that requests with children are not ghosts."""
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

        # Mark as completed
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": parent_id},
            )
            await session.commit()

        # Create a child request
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
            result = await debugger.get_ghost_requests()

            # Parent should not be a ghost (has children)
            assert result["total_count"] == 0

    async def test_get_ghost_requests_not_ghost_with_results(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test that requests with results are not ghosts."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a request
        req_id = await sql_manager.insert_request(
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

        # Mark as completed
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "completed", "id": req_id},
            )
            await session.commit()

        # Add a result (prevents it from being a ghost)
        await sql_manager.store_result(
            request_id=req_id,
            result_type="TestData",
            data_json='{"test": "data"}',
            is_valid=True,
        )
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_ghost_requests()

            # Should not be a ghost (has results)
            assert result["total_count"] == 0

    async def test_get_ghost_requests_multiple_continuations(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test get_ghost_requests groups by continuation."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create ghost requests in different continuations
        ghost_ids = []
        for i in range(3):
            continuation = "step1" if i < 2 else "step2"
            ghost_id = await sql_manager.insert_request(
                priority=1,
                request_type="navigating",
                method="GET",
                url=f"https://example.com/ghost{i}",
                headers_json="{}",
                cookies_json="{}",
                body=None,
                continuation=continuation,
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
                    sa.text(
                        "UPDATE requests SET status = :status WHERE id = :id"
                    ),
                    {"status": "completed", "id": ghost_id},
                )
                await session.commit()
            ghost_ids.append(ghost_id)
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_ghost_requests()

            assert result["total_count"] == 3
            assert result["by_continuation"]["step1"] == 2
            assert result["by_continuation"]["step2"] == 1
            assert len(result["ghosts"]) == 3

    async def test_get_ghost_requests_pending_not_ghost(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test that pending requests are not considered ghosts."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a pending request (should not be a ghost)
        _req_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/pending",
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

        # Leave as pending (default status)

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_ghost_requests()

            # Pending requests should not be ghosts
            assert result["total_count"] == 0

    async def test_get_ghost_requests_failed_not_ghost(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test that failed requests are not considered ghosts."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create a failed request
        req_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/failed",
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

        # Mark as failed
        async with session_factory() as session:
            await session.execute(
                sa.text("UPDATE requests SET status = :status WHERE id = :id"),
                {"status": "failed", "id": req_id},
            )
            await session.commit()
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_ghost_requests()

            # Failed requests should not be ghosts
            assert result["total_count"] == 0


class TestEstimateChecks:
    """Tests for check_estimates method."""

    async def test_check_estimates_no_estimates(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_estimates when no estimates exist."""
        engine, _session_factory = initialized_db
        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_estimates()

            assert result["summary"]["total"] == 0
            assert result["summary"]["passed"] == 0
            assert result["summary"]["failed"] == 0
            assert result["estimates"] == []

    async def test_check_estimates_passing(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_estimates with an estimate that passes."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # Create parent request (the one that emits the estimate)
        parent_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/search",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_search",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Store estimate: expect 3 CaseData results
        await sql_manager.store_estimate(
            request_id=parent_id,
            expected_types_json=json.dumps(["CaseData"]),
            min_count=3,
            max_count=3,
        )

        # Create child requests that produce results
        for i in range(3):
            child_id = await sql_manager.insert_request(
                priority=1,
                request_type="navigating",
                method="GET",
                url=f"https://example.com/case/{i}",
                headers_json="{}",
                cookies_json="{}",
                body=None,
                continuation="parse_case",
                current_location="",
                accumulated_data_json="{}",
                aux_data_json="{}",
                permanent_json="{}",
                expected_type=None,
                dedup_key=None,
                parent_id=parent_id,
            )
            await sql_manager.store_result(
                request_id=child_id,
                result_type="CaseData",
                data_json=json.dumps({"id": i}),
            )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_estimates()

            assert result["summary"]["total"] == 1
            assert result["summary"]["passed"] == 1
            assert result["summary"]["failed"] == 0
            est = result["estimates"][0]
            assert est["status"] == "pass"
            assert est["actual_count"] == 3

    async def test_check_estimates_failing_too_few(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_estimates fails when too few results produced."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        parent_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/search",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_search",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Expect 10 results but only produce 2
        await sql_manager.store_estimate(
            request_id=parent_id,
            expected_types_json=json.dumps(["CaseData"]),
            min_count=10,
            max_count=10,
        )

        for i in range(2):
            child_id = await sql_manager.insert_request(
                priority=1,
                request_type="navigating",
                method="GET",
                url=f"https://example.com/case/{i}",
                headers_json="{}",
                cookies_json="{}",
                body=None,
                continuation="parse_case",
                current_location="",
                accumulated_data_json="{}",
                aux_data_json="{}",
                permanent_json="{}",
                expected_type=None,
                dedup_key=None,
                parent_id=parent_id,
            )
            await sql_manager.store_result(
                request_id=child_id,
                result_type="CaseData",
                data_json=json.dumps({"id": i}),
            )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_estimates()

            assert result["summary"]["failed"] == 1
            est = result["estimates"][0]
            assert est["status"] == "fail"
            assert est["actual_count"] == 2
            assert est["min_count"] == 10

    async def test_check_estimates_failing_too_many(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_estimates fails when too many results produced."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        parent_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/search",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_search",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Expect at most 2 but produce 5
        await sql_manager.store_estimate(
            request_id=parent_id,
            expected_types_json=json.dumps(["CaseData"]),
            min_count=1,
            max_count=2,
        )

        for i in range(5):
            child_id = await sql_manager.insert_request(
                priority=1,
                request_type="navigating",
                method="GET",
                url=f"https://example.com/case/{i}",
                headers_json="{}",
                cookies_json="{}",
                body=None,
                continuation="parse_case",
                current_location="",
                accumulated_data_json="{}",
                aux_data_json="{}",
                permanent_json="{}",
                expected_type=None,
                dedup_key=None,
                parent_id=parent_id,
            )
            await sql_manager.store_result(
                request_id=child_id,
                result_type="CaseData",
                data_json=json.dumps({"id": i}),
            )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_estimates()

            assert result["summary"]["failed"] == 1
            est = result["estimates"][0]
            assert est["status"] == "fail"
            assert est["actual_count"] == 5
            assert est["max_count"] == 2

    async def test_check_estimates_unbounded_max(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_estimates passes with unbounded max when min is met."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        parent_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/search",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_search",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # min_count=5, max_count=None ("at least 5")
        await sql_manager.store_estimate(
            request_id=parent_id,
            expected_types_json=json.dumps(["CaseData"]),
            min_count=5,
            max_count=None,
        )

        for i in range(20):
            child_id = await sql_manager.insert_request(
                priority=1,
                request_type="navigating",
                method="GET",
                url=f"https://example.com/case/{i}",
                headers_json="{}",
                cookies_json="{}",
                body=None,
                continuation="parse_case",
                current_location="",
                accumulated_data_json="{}",
                aux_data_json="{}",
                permanent_json="{}",
                expected_type=None,
                dedup_key=None,
                parent_id=parent_id,
            )
            await sql_manager.store_result(
                request_id=child_id,
                result_type="CaseData",
                data_json=json.dumps({"id": i}),
            )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_estimates()

            assert result["summary"]["passed"] == 1
            est = result["estimates"][0]
            assert est["status"] == "pass"
            assert est["actual_count"] == 20

    async def test_check_estimates_deep_descendants(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_estimates counts results from deep descendants (grandchildren)."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        # search_page -> page_2 -> detail_page (results here)
        search_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/search",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_search",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        await sql_manager.store_estimate(
            request_id=search_id,
            expected_types_json=json.dumps(["CaseData"]),
            min_count=2,
            max_count=2,
        )

        # Child: page 2 of results
        page2_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/search?page=2",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_search",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=search_id,
        )

        # Grandchildren: detail pages producing results
        for i in range(2):
            detail_id = await sql_manager.insert_request(
                priority=1,
                request_type="navigating",
                method="GET",
                url=f"https://example.com/case/{i}",
                headers_json="{}",
                cookies_json="{}",
                body=None,
                continuation="parse_case",
                current_location="",
                accumulated_data_json="{}",
                aux_data_json="{}",
                permanent_json="{}",
                expected_type=None,
                dedup_key=None,
                parent_id=page2_id,
            )
            await sql_manager.store_result(
                request_id=detail_id,
                result_type="CaseData",
                data_json=json.dumps({"id": i}),
            )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_estimates()

            assert result["summary"]["passed"] == 1
            est = result["estimates"][0]
            assert est["status"] == "pass"
            assert est["actual_count"] == 2

    async def test_check_estimates_filters_by_type(
        self, db_path: Path, initialized_db
    ) -> None:
        """Test check_estimates only counts results of expected types."""
        engine, session_factory = initialized_db
        sql_manager = SQLManager(engine, session_factory)

        parent_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/search",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_search",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )

        # Expect 2 CaseData results only
        await sql_manager.store_estimate(
            request_id=parent_id,
            expected_types_json=json.dumps(["CaseData"]),
            min_count=2,
            max_count=2,
        )

        child_id = await sql_manager.insert_request(
            priority=1,
            request_type="navigating",
            method="GET",
            url="https://example.com/case/1",
            headers_json="{}",
            cookies_json="{}",
            body=None,
            continuation="parse_case",
            current_location="",
            accumulated_data_json="{}",
            aux_data_json="{}",
            permanent_json="{}",
            expected_type=None,
            dedup_key=None,
            parent_id=parent_id,
        )

        # 2 CaseData + 1 DocumentData (should not count)
        await sql_manager.store_result(
            request_id=child_id,
            result_type="CaseData",
            data_json=json.dumps({"id": 1}),
        )
        await sql_manager.store_result(
            request_id=child_id,
            result_type="CaseData",
            data_json=json.dumps({"id": 2}),
        )
        await sql_manager.store_result(
            request_id=child_id,
            result_type="DocumentData",
            data_json=json.dumps({"id": 3}),
        )

        await engine.dispose()

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_estimates()

            assert result["summary"]["passed"] == 1
            est = result["estimates"][0]
            assert est["actual_count"] == 2  # Only CaseData counted
