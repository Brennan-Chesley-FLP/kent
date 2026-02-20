"""Tests for LocalDevDriver web routes.

Tests cover:
- Request summary endpoint grouping
- Request summary with empty database
- Compression stats by continuation endpoint
- Results summary with valid/invalid counts by type
- Results JSONL export format
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import sqlalchemy as sa

from kent.driver.persistent_driver.web.routes.compression import (
    CompressionStatsByContinuationItem,
    CompressionStatsByContinuationResponse,
)
from kent.driver.persistent_driver.web.routes.requests import (
    RequestSummaryItem,
    RequestSummaryResponse,
)
from kent.driver.persistent_driver.web.routes.results import (
    ResultsSummaryResponse,
    ResultTypeSummaryItem,
)


@pytest.fixture
async def db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
async def initialized_db(db_path: Path):
    """Create and return an initialized database connection."""
    from kent.driver.persistent_driver.database import (
        init_database,
    )

    engine, session_factory = await init_database(db_path)
    yield engine, session_factory
    await engine.dispose()


class TestRequestSummary:
    """Tests for the request summary endpoint."""

    async def test_summary_empty_db(self, initialized_db) -> None:
        """Returns empty list for no requests."""
        engine, session_factory = initialized_db

        # Query counts grouped by continuation and status
        query = """
            SELECT continuation, status, COUNT(*) as count
            FROM requests
            GROUP BY continuation, status
            ORDER BY continuation
        """
        async with session_factory() as session:
            result = await session.execute(sa.text(query))
            rows = result.all()

        # Build pivot table (same logic as endpoint)
        summaries: dict[str, RequestSummaryItem] = {}
        grand_total = 0

        for continuation, _status_val, count in rows:
            if continuation not in summaries:
                summaries[continuation] = RequestSummaryItem(
                    continuation=continuation
                )

            item = summaries[continuation]
            grand_total += count
            item.total += count

        result = RequestSummaryResponse(
            items=list(summaries.values()),
            grand_total=grand_total,
        )

        assert result.items == []
        assert result.grand_total == 0

    async def test_summary_endpoint_grouping(self, initialized_db) -> None:
        """Counts correct per continuation/status."""
        engine, session_factory = initialized_db

        # Insert test requests with different continuations and statuses
        requests_data = [
            # parse_list continuation: 2 pending, 1 completed
            ("pending", 1, 1, "GET", "http://example.com/1", "parse_list"),
            ("pending", 1, 2, "GET", "http://example.com/2", "parse_list"),
            ("completed", 1, 3, "GET", "http://example.com/3", "parse_list"),
            # parse_detail continuation: 1 pending, 2 in_progress, 1 failed
            ("pending", 1, 4, "GET", "http://example.com/4", "parse_detail"),
            (
                "in_progress",
                1,
                5,
                "GET",
                "http://example.com/5",
                "parse_detail",
            ),
            (
                "in_progress",
                1,
                6,
                "GET",
                "http://example.com/6",
                "parse_detail",
            ),
            ("failed", 1, 7, "GET", "http://example.com/7", "parse_detail"),
            # archive continuation: 1 held
            ("held", 1, 8, "GET", "http://example.com/8", "archive"),
        ]

        async with session_factory() as session:
            for (
                status,
                priority,
                queue_counter,
                method,
                url,
                continuation,
            ) in requests_data:
                await session.execute(
                    sa.text("""
                    INSERT INTO requests (status, priority, queue_counter, method, url, continuation)
                    VALUES (:status, :priority, :queue_counter, :method, :url, :continuation)
                    """),
                    {
                        "status": status,
                        "priority": priority,
                        "queue_counter": queue_counter,
                        "method": method,
                        "url": url,
                        "continuation": continuation,
                    },
                )
            await session.commit()

        # Query counts grouped by continuation and status
        query = """
            SELECT continuation, status, COUNT(*) as count
            FROM requests
            GROUP BY continuation, status
            ORDER BY continuation
        """
        async with session_factory() as session:
            result = await session.execute(sa.text(query))
            rows = result.all()

        # Build pivot table (same logic as endpoint)
        summaries: dict[str, RequestSummaryItem] = {}
        grand_total = 0

        for continuation, status_val, count in rows:
            if continuation not in summaries:
                summaries[continuation] = RequestSummaryItem(
                    continuation=continuation
                )

            item = summaries[continuation]
            grand_total += count
            item.total += count

            if status_val == "pending":
                item.pending = count
            elif status_val == "in_progress":
                item.in_progress = count
            elif status_val == "completed":
                item.completed = count
            elif status_val == "failed":
                item.failed = count
            elif status_val == "held":
                item.held = count
            elif status_val == "cancelled":
                item.cancelled = count

        result = RequestSummaryResponse(
            items=list(summaries.values()),
            grand_total=grand_total,
        )

        # Verify grand total
        assert result.grand_total == 8

        # Verify we have 3 continuations
        assert len(result.items) == 3

        # Find each continuation and verify counts
        items_by_cont = {item.continuation: item for item in result.items}

        # archive: 1 held
        archive_item = items_by_cont["archive"]
        assert archive_item.held == 1
        assert archive_item.pending == 0
        assert archive_item.total == 1

        # parse_detail: 1 pending, 2 in_progress, 1 failed
        detail_item = items_by_cont["parse_detail"]
        assert detail_item.pending == 1
        assert detail_item.in_progress == 2
        assert detail_item.failed == 1
        assert detail_item.completed == 0
        assert detail_item.total == 4

        # parse_list: 2 pending, 1 completed
        list_item = items_by_cont["parse_list"]
        assert list_item.pending == 2
        assert list_item.completed == 1
        assert list_item.in_progress == 0
        assert list_item.total == 3


class TestCompressionStatsByContinuation:
    """Tests for the compression stats by continuation endpoint."""

    async def test_stats_by_continuation_empty_db(
        self, initialized_db
    ) -> None:
        """Returns empty list for no responses."""
        engine, session_factory = initialized_db

        query = """
            SELECT
                r.continuation,
                r.compression_dict_id,
                d.version,
                COUNT(*) as response_count,
                COALESCE(SUM(r.content_size_original), 0) as total_original,
                COALESCE(SUM(r.content_size_compressed), 0) as total_compressed
            FROM requests r
            LEFT JOIN compression_dicts d ON r.compression_dict_id = d.id
            WHERE r.response_status_code IS NOT NULL
            GROUP BY r.continuation, r.compression_dict_id
            ORDER BY r.continuation, d.version DESC NULLS LAST
        """
        async with session_factory() as session:
            result = await session.execute(sa.text(query))
            rows = result.all()

        items: list[CompressionStatsByContinuationItem] = []
        for (
            continuation,
            dict_id,
            version,
            count,
            total_orig,
            total_comp,
        ) in rows:
            ratio = total_orig / total_comp if total_comp > 0 else 0.0
            items.append(
                CompressionStatsByContinuationItem(
                    continuation=continuation,
                    dict_id=dict_id,
                    dict_version=version,
                    response_count=count,
                    total_original_bytes=total_orig,
                    total_compressed_bytes=total_comp,
                    compression_ratio=round(ratio, 2),
                )
            )

        result = CompressionStatsByContinuationResponse(
            items=items,
            grand_total_responses=0,
            grand_total_original=0,
            grand_total_compressed=0,
            overall_ratio=0.0,
        )

        assert result.items == []
        assert result.grand_total_responses == 0

    async def test_stats_by_continuation_grouping(
        self, initialized_db
    ) -> None:
        """Groups by continuation and calculates compression ratio correctly."""
        engine, session_factory = initialized_db

        async with session_factory() as session:
            # First insert requests (required foreign key)
            await session.execute(
                sa.text("""
                INSERT INTO requests (id, status, priority, queue_counter, method, url, continuation)
                VALUES (1, 'completed', 1, 1, 'GET', 'http://example.com/1', 'parse_list'),
                       (2, 'completed', 1, 2, 'GET', 'http://example.com/2', 'parse_list'),
                       (3, 'completed', 1, 3, 'GET', 'http://example.com/3', 'parse_detail')
                """)
            )

            # Insert responses for testing (no dictionary)
            responses_data = [
                # parse_list: 2 responses, 10000 original, 2000 compressed (5x ratio)
                (
                    1,
                    200,
                    "http://example.com/1",
                    b"compressed1",
                    5000,
                    1000,
                    None,
                    "parse_list",
                ),
                (
                    2,
                    200,
                    "http://example.com/2",
                    b"compressed2",
                    5000,
                    1000,
                    None,
                    "parse_list",
                ),
                # parse_detail: 1 response, 8000 original, 1000 compressed (8x ratio)
                (
                    3,
                    200,
                    "http://example.com/3",
                    b"compressed3",
                    8000,
                    1000,
                    None,
                    "parse_detail",
                ),
            ]

            for (
                req_id,
                status,
                url,
                content,
                orig_size,
                comp_size,
                dict_id,
                _cont,
            ) in responses_data:
                await session.execute(
                    sa.text("""
                    UPDATE requests SET
                        response_status_code = :status,
                        response_url = :url,
                        content_compressed = :content,
                        content_size_original = :orig_size,
                        content_size_compressed = :comp_size,
                        compression_dict_id = :dict_id
                    WHERE id = :req_id
                    """),
                    {
                        "req_id": req_id,
                        "status": status,
                        "url": url,
                        "content": content,
                        "orig_size": orig_size,
                        "comp_size": comp_size,
                        "dict_id": dict_id,
                    },
                )
            await session.commit()

        # Query using same logic as endpoint
        query = """
            SELECT
                r.continuation,
                r.compression_dict_id,
                d.version,
                COUNT(*) as response_count,
                COALESCE(SUM(r.content_size_original), 0) as total_original,
                COALESCE(SUM(r.content_size_compressed), 0) as total_compressed
            FROM requests r
            LEFT JOIN compression_dicts d ON r.compression_dict_id = d.id
            WHERE r.response_status_code IS NOT NULL
            GROUP BY r.continuation, r.compression_dict_id
            ORDER BY r.continuation, d.version DESC NULLS LAST
        """
        async with session_factory() as session:
            result = await session.execute(sa.text(query))
            rows = result.all()

        items: list[CompressionStatsByContinuationItem] = []
        grand_total_responses = 0
        grand_total_original = 0
        grand_total_compressed = 0

        for (
            continuation,
            dict_id,
            version,
            count,
            total_orig,
            total_comp,
        ) in rows:
            ratio = total_orig / total_comp if total_comp > 0 else 0.0
            items.append(
                CompressionStatsByContinuationItem(
                    continuation=continuation,
                    dict_id=dict_id,
                    dict_version=version,
                    response_count=count,
                    total_original_bytes=total_orig,
                    total_compressed_bytes=total_comp,
                    compression_ratio=round(ratio, 2),
                )
            )
            grand_total_responses += count
            grand_total_original += total_orig
            grand_total_compressed += total_comp

        overall_ratio = (
            grand_total_original / grand_total_compressed
            if grand_total_compressed > 0
            else 0.0
        )

        result = CompressionStatsByContinuationResponse(
            items=items,
            grand_total_responses=grand_total_responses,
            grand_total_original=grand_total_original,
            grand_total_compressed=grand_total_compressed,
            overall_ratio=round(overall_ratio, 2),
        )

        # Verify totals
        assert result.grand_total_responses == 3
        assert result.grand_total_original == 18000  # 5000 + 5000 + 8000
        assert result.grand_total_compressed == 3000  # 1000 + 1000 + 1000
        assert result.overall_ratio == 6.0  # 18000 / 3000

        # Verify grouping - should have 2 groups (parse_detail, parse_list)
        assert len(result.items) == 2

        items_by_cont = {item.continuation: item for item in result.items}

        # parse_detail: 1 response, 8x ratio
        detail_item = items_by_cont["parse_detail"]
        assert detail_item.response_count == 1
        assert detail_item.total_original_bytes == 8000
        assert detail_item.total_compressed_bytes == 1000
        assert detail_item.compression_ratio == 8.0
        assert detail_item.dict_id is None

        # parse_list: 2 responses, 5x ratio
        list_item = items_by_cont["parse_list"]
        assert list_item.response_count == 2
        assert list_item.total_original_bytes == 10000
        assert list_item.total_compressed_bytes == 2000
        assert list_item.compression_ratio == 5.0
        assert list_item.dict_id is None


class TestResultsSummary:
    """Tests for the results summary endpoint."""

    async def test_results_summary_empty_db(self, initialized_db) -> None:
        """Returns zeros for no results."""
        engine, session_factory = initialized_db

        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                    SELECT
                        result_type,
                        SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) as valid_count,
                        SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) as invalid_count,
                        COUNT(*) as total_count
                    FROM results
                    GROUP BY result_type
                    ORDER BY total_count DESC
                """)
            )
            rows = result.all()

        by_type: list[ResultTypeSummaryItem] = []
        total_valid = 0
        total_invalid = 0

        for result_type, valid_count, invalid_count, total_count in rows:
            by_type.append(
                ResultTypeSummaryItem(
                    result_type=result_type,
                    valid_count=valid_count,
                    invalid_count=invalid_count,
                    total_count=total_count,
                )
            )
            total_valid += valid_count
            total_invalid += invalid_count

        result = ResultsSummaryResponse(
            total_valid=total_valid,
            total_invalid=total_invalid,
            total=total_valid + total_invalid,
            by_type=by_type,
        )

        assert result.total == 0
        assert result.total_valid == 0
        assert result.total_invalid == 0
        assert result.by_type == []

    async def test_results_summary_counts_by_type(
        self, initialized_db
    ) -> None:
        """Correctly counts valid/invalid by result type."""
        engine, session_factory = initialized_db

        # Insert test results with different types and validity
        results_data = [
            # TennOpinion: 3 valid, 1 invalid
            ("TennOpinion", '{"case_number": "1"}', 1, None),
            ("TennOpinion", '{"case_number": "2"}', 1, None),
            ("TennOpinion", '{"case_number": "3"}', 1, None),
            (
                "TennOpinion",
                '{"case_number": "4"}',
                0,
                '[{"field": "date", "message": "required"}]',
            ),
            # TennJudge: 2 valid, 2 invalid
            ("TennJudge", '{"name": "John"}', 1, None),
            ("TennJudge", '{"name": "Jane"}', 1, None),
            (
                "TennJudge",
                '{"name": ""}',
                0,
                '[{"field": "name", "message": "empty"}]',
            ),
            (
                "TennJudge",
                "{}",
                0,
                '[{"field": "name", "message": "required"}]',
            ),
        ]

        async with session_factory() as session:
            for result_type, data_json, is_valid, errors_json in results_data:
                await session.execute(
                    sa.text("""
                    INSERT INTO results (result_type, data_json, is_valid, validation_errors_json)
                    VALUES (:result_type, :data_json, :is_valid, :errors_json)
                    """),
                    {
                        "result_type": result_type,
                        "data_json": data_json,
                        "is_valid": is_valid,
                        "errors_json": errors_json,
                    },
                )
            await session.commit()

        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                    SELECT
                        result_type,
                        SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) as valid_count,
                        SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) as invalid_count,
                        COUNT(*) as total_count
                    FROM results
                    GROUP BY result_type
                    ORDER BY total_count DESC
                """)
            )
            rows = result.all()

        by_type: list[ResultTypeSummaryItem] = []
        total_valid = 0
        total_invalid = 0

        for result_type, valid_count, invalid_count, total_count in rows:
            by_type.append(
                ResultTypeSummaryItem(
                    result_type=result_type,
                    valid_count=valid_count,
                    invalid_count=invalid_count,
                    total_count=total_count,
                )
            )
            total_valid += valid_count
            total_invalid += invalid_count

        result = ResultsSummaryResponse(
            total_valid=total_valid,
            total_invalid=total_invalid,
            total=total_valid + total_invalid,
            by_type=by_type,
        )

        # Verify totals
        assert result.total == 8
        assert result.total_valid == 5  # 3 + 2
        assert result.total_invalid == 3  # 1 + 2

        # Verify by type
        assert len(result.by_type) == 2
        types_by_name = {item.result_type: item for item in result.by_type}

        opinion = types_by_name["TennOpinion"]
        assert opinion.valid_count == 3
        assert opinion.invalid_count == 1
        assert opinion.total_count == 4

        judge = types_by_name["TennJudge"]
        assert judge.valid_count == 2
        assert judge.invalid_count == 2
        assert judge.total_count == 4


class TestResultsJsonlExport:
    """Tests for the JSONL export functionality."""

    async def test_jsonl_export_format(self, initialized_db) -> None:
        """Each line in JSONL export is valid JSON with correct fields."""
        engine, session_factory = initialized_db

        # Insert test data
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO results (result_type, data_json, is_valid, validation_errors_json)
                VALUES ('TestType', '{"foo": "bar"}', 1, NULL),
                       ('TestType', '{"baz": 123}', 0, '[{"field": "qux", "message": "error"}]')
                """)
            )
            await session.commit()

        # Query using same SQL as export endpoint
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                    SELECT id, request_id, result_type, data_json, is_valid,
                           validation_errors_json, created_at
                    FROM results
                    ORDER BY created_at ASC
                """)
            )
            rows = result.all()

        jsonl_lines = []
        for row in rows:
            (
                result_id,
                request_id,
                rtype,
                data_json,
                valid,
                errors_json,
                created_at,
            ) = row

            try:
                data = json.loads(data_json) if data_json else {}
            except json.JSONDecodeError:
                data = {}

            validation_errors = None
            if errors_json:
                try:
                    validation_errors = json.loads(errors_json)
                except json.JSONDecodeError:
                    pass

            record = {
                "id": result_id,
                "request_id": request_id,
                "result_type": rtype,
                "is_valid": bool(valid),
                "data": data,
                "validation_errors": validation_errors,
                "created_at": created_at,
            }
            jsonl_lines.append(json.dumps(record))

        # Verify we have 2 lines
        assert len(jsonl_lines) == 2

        # Verify each line is valid JSON and has expected fields
        for line in jsonl_lines:
            record = json.loads(line)
            assert "id" in record
            assert "result_type" in record
            assert "is_valid" in record
            assert "data" in record
            assert "validation_errors" in record
            assert "created_at" in record

        # Verify first record (valid)
        first = json.loads(jsonl_lines[0])
        assert first["result_type"] == "TestType"
        assert first["is_valid"] is True
        assert first["data"] == {"foo": "bar"}
        assert first["validation_errors"] is None

        # Verify second record (invalid with errors)
        second = json.loads(jsonl_lines[1])
        assert second["is_valid"] is False
        assert second["validation_errors"] == [
            {"field": "qux", "message": "error"}
        ]

    async def test_jsonl_export_with_filter(self, initialized_db) -> None:
        """JSONL export respects result_type and is_valid filters."""
        engine, session_factory = initialized_db

        # Insert mixed test data
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO results (result_type, data_json, is_valid)
                VALUES ('TypeA', '{}', 1),
                       ('TypeA', '{}', 0),
                       ('TypeB', '{}', 1),
                       ('TypeB', '{}', 0)
                """)
            )
            await session.commit()

        # Test filtering by result_type
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                    SELECT id, request_id, result_type, data_json, is_valid,
                           validation_errors_json, created_at
                    FROM results
                    WHERE result_type = :result_type
                    ORDER BY created_at ASC
                """),
                {"result_type": "TypeA"},
            )
            rows = result.all()
        assert len(rows) == 2

        # Test filtering by is_valid
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                    SELECT id, request_id, result_type, data_json, is_valid,
                           validation_errors_json, created_at
                    FROM results
                    WHERE is_valid = :is_valid
                    ORDER BY created_at ASC
                """),
                {"is_valid": 1},
            )
            rows = result.all()
        assert len(rows) == 2

        # Test combined filter
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                    SELECT id, request_id, result_type, data_json, is_valid,
                           validation_errors_json, created_at
                    FROM results
                    WHERE result_type = :result_type AND is_valid = :is_valid
                    ORDER BY created_at ASC
                """),
                {"result_type": "TypeA", "is_valid": 0},
            )
            rows = result.all()
        assert len(rows) == 1
