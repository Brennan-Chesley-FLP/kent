"""Tests for LocalDevDriverDebugger integrity check and ghost request detection.

Tests for check_integrity, get_orphan_details, and get_ghost_requests methods.
"""

from __future__ import annotations

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
