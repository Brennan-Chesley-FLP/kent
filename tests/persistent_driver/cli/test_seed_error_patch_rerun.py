"""Tests for the `pdd seed-error-patch-rerun` command."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from click.testing import CliRunner

from kent.driver.persistent_driver.cli import cli
from kent.driver.persistent_driver.database import init_database
from kent.driver.persistent_driver.sql_manager import SQLManager

# ---------------------------------------------------------------------------
# Fixture: a small request tree with configurable errors
# ---------------------------------------------------------------------------


async def _insert_root(
    sql: SQLManager,
    url: str,
    *,
    continuation: str = "entry",
    dedup_key: str | None = None,
) -> int:
    """Insert a root (no parent) request."""
    return await sql.insert_entry_request(
        priority=0,
        method="GET",
        url=url,
        headers_json='{"X-Test": "1"}',
        cookies_json="{}",
        body=None,
        continuation=continuation,
        current_location="",
        accumulated_data_json=None,
        permanent_json=None,
        dedup_key=dedup_key,
    )


async def _insert_child(
    sql: SQLManager, url: str, parent_id: int, *, continuation: str = "child"
) -> int:
    """Insert a child request with given parent."""
    return await sql.insert_request(
        priority=0,
        request_type="navigating",
        method="GET",
        url=url,
        headers_json="{}",
        cookies_json="{}",
        body=None,
        continuation=continuation,
        current_location="",
        accumulated_data_json=None,
        permanent_json=None,
        expected_type=None,
        dedup_key=None,
        parent_id=parent_id,
    )


async def _set_status(
    session_factory: Any, request_id: int, status: str
) -> None:
    async with session_factory() as session:
        await session.execute(
            sa.text("UPDATE requests SET status = :s WHERE id = :i"),
            {"s": status, "i": request_id},
        )
        await session.commit()


async def _insert_error(
    session_factory: Any,
    request_id: int,
    *,
    is_resolved: bool = False,
    error_type: str = "http",
) -> int:
    async with session_factory() as session:
        result = await session.execute(
            sa.text("""
            INSERT INTO errors (
                request_id, error_type, message, is_resolved,
                created_at, request_url, error_class, traceback
            ) VALUES (:request_id, :error_type, :message, :is_resolved,
                datetime('now'), :request_url, :error_class, :traceback)
            RETURNING id
            """),
            {
                "request_id": request_id,
                "error_type": error_type,
                "message": f"error on {request_id}",
                "is_resolved": is_resolved,
                "request_url": f"https://example.com/{request_id}",
                "error_class": "TestError",
                "traceback": "fake traceback",
            },
        )
        error_id = result.scalar()
        await session.commit()
        return error_id  # type: ignore[return-value]


async def _init_metadata(sql: SQLManager) -> None:
    await sql.init_run_metadata(
        scraper_name="test.scraper",
        scraper_version="1.0.0",
        num_workers=2,
        max_backoff_time=60.0,
        seed_params=[{"entry_func": {"arg": "value"}}],
    )


@pytest.fixture
async def tree_db(tmp_path: Path) -> Path:
    """Build a source DB with the following structure:

        R1 (root, completed)                        # unused / no error
          └── C1.a (completed, has error unresolved)   # errored leaf
          └── C1.b (completed, no error)                # sibling
        R2 (root, completed)
          └── C2.a (completed)
              └── G2 (completed, has error unresolved) # deep error
          └── C2.b (completed, has error unresolved)   # second error under R2
        R3 (root, completed, has error unresolved)     # root is itself errored

    Expected seed roots: R1, R2, R3. Three unique roots, 4 unresolved errors
    covered (one of which is on R3 itself).
    """
    db = tmp_path / "source.db"
    engine, session_factory = await init_database(db)
    sql = SQLManager(engine, session_factory)
    await _init_metadata(sql)

    r1 = await _insert_root(sql, "https://example.com/r1", dedup_key="r1-key")
    r2 = await _insert_root(sql, "https://example.com/r2")
    r3 = await _insert_root(sql, "https://example.com/r3")

    c1a = await _insert_child(sql, "https://example.com/r1/a", r1)
    c1b = await _insert_child(sql, "https://example.com/r1/b", r1)
    c2a = await _insert_child(sql, "https://example.com/r2/a", r2)
    g2 = await _insert_child(sql, "https://example.com/r2/a/g", c2a)
    c2b = await _insert_child(sql, "https://example.com/r2/b", r2)

    for rid in [r1, r2, r3, c1a, c1b, c2a, g2, c2b]:
        await _set_status(session_factory, rid, "completed")

    # Errors: unresolved on c1a, g2, c2b, r3
    await _insert_error(session_factory, c1a)
    await _insert_error(session_factory, g2)
    await _insert_error(session_factory, c2b)
    await _insert_error(session_factory, r3)

    await engine.dispose()
    return db


# ---------------------------------------------------------------------------
# Helpers for inspecting the output DB / source DB
# ---------------------------------------------------------------------------


async def _read_output(output_db: Path) -> dict[str, Any]:
    """Return the contents of the output DB as a simple dict."""
    engine, session_factory = await init_database(output_db)
    try:
        async with session_factory() as session:
            r = await session.execute(
                sa.text(
                    "SELECT id, url, status, parent_request_id, continuation, "
                    "deduplication_key, headers_json "
                    "FROM requests ORDER BY id"
                )
            )
            requests = [dict(row._mapping) for row in r.all()]
            m = await session.execute(
                sa.text(
                    "SELECT scraper_name, seed_params_json, num_workers "
                    "FROM run_metadata WHERE id = 1"
                )
            )
            meta_row = m.first()
            metadata = dict(meta_row._mapping) if meta_row else {}
    finally:
        await engine.dispose()

    return {"requests": requests, "metadata": metadata}


async def _read_source_errors(db: Path) -> list[dict[str, Any]]:
    engine, session_factory = await init_database(db)
    try:
        async with session_factory() as session:
            r = await session.execute(
                sa.text(
                    "SELECT id, is_resolved, resolution_type, resolution_notes "
                    "FROM errors ORDER BY id"
                )
            )
            return [dict(row._mapping) for row in r.all()]
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


class TestSeedErrorPatchRerun:
    def test_produces_one_request_per_unique_root(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
            ],
        )
        assert result.exit_code == 0, result.output
        assert out.exists()

        import asyncio

        data = asyncio.run(_read_output(out))
        urls = sorted(r["url"] for r in data["requests"])
        assert urls == [
            "https://example.com/r1",
            "https://example.com/r2",
            "https://example.com/r3",
        ]
        for req in data["requests"]:
            assert req["status"] == "pending"
            assert req["parent_request_id"] is None
            assert req["headers_json"] == '{"X-Test": "1"}'

    def test_output_metadata_has_no_seed_params(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
            ],
        )
        assert result.exit_code == 0, result.output

        import asyncio

        data = asyncio.run(_read_output(out))
        assert data["metadata"]["scraper_name"] == "test.scraper"
        assert data["metadata"]["num_workers"] == 2
        assert data["metadata"]["seed_params_json"] is None

    def test_preserves_dedup_key_on_copied_root(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
            ],
        )
        assert result.exit_code == 0, result.output

        import asyncio

        data = asyncio.run(_read_output(out))
        r1 = next(
            r for r in data["requests"] if r["url"] == "https://example.com/r1"
        )
        assert r1["deduplication_key"] == "r1-key"

    def test_default_template_reports_blast_radius(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
            ],
        )
        assert result.exit_code == 0, result.output
        output = result.output
        # 3 unique roots, 4 unresolved errors covered, 8 total requests.
        # Descendants of R1, R2, R3: R1+C1.a+C1.b + R2+C2.a+G2+C2.b + R3 = 8
        assert "Unique root ancestors: 3" in output
        assert "Errors covered by these roots: 4" in output
        assert "total descendants:       8" in output
        assert "errored descendants:     4" in output

    def test_json_format_emits_machine_readable_plan(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
                "--format",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["roots"]["count"] == 3
        assert data["roots"]["covered_error_count"] == 4
        assert data["blast_radius"]["total_descendants"] == 8
        assert data["blast_radius"]["errored_descendants"] == 4
        assert data["mode"]["wrote_output_db"] is True
        assert data["resolution"]["resolved_count"] == 4
        assert data["resolution"]["resolution_type"] == "rerun_generated"

    def test_default_marks_source_errors_resolved(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
            ],
        )
        assert result.exit_code == 0, result.output

        import asyncio

        rows = asyncio.run(_read_source_errors(tree_db))
        assert len(rows) == 4
        for row in rows:
            assert row["is_resolved"] == 1
            assert row["resolution_type"] == "rerun_generated"
            assert (
                row["resolution_notes"]
                == "Resolved via pdd seed-error-patch-rerun"
            )

    def test_no_resolve_leaves_source_untouched(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
                "--no-resolve",
            ],
        )
        assert result.exit_code == 0, result.output
        assert out.exists()

        import asyncio

        rows = asyncio.run(_read_source_errors(tree_db))
        assert len(rows) == 4
        for row in rows:
            assert row["is_resolved"] == 0
            assert row["resolution_type"] is None

    def test_report_only_makes_no_changes(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"  # path we will NOT want to be created
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--report",
            ],
        )
        assert result.exit_code == 0, result.output
        assert not out.exists(), "--report must not create an output DB"
        assert "report" in result.output.lower()
        assert "Unique root ancestors: 3" in result.output

        import asyncio

        rows = asyncio.run(_read_source_errors(tree_db))
        for row in rows:
            assert row["is_resolved"] == 0
            assert row["resolution_type"] is None

    def test_report_rejects_output_db(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
                "--report",
            ],
        )
        assert result.exit_code != 0

    def test_missing_output_db_without_report_errors(
        self, runner: CliRunner, tree_db: Path
    ) -> None:
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
            ],
        )
        assert result.exit_code != 0
        assert "--output-db is required" in result.output

    def test_refuses_to_overwrite_existing_output(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        out.write_text("")
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
            ],
        )
        assert result.exit_code != 0
        assert "Refusing to overwrite" in result.output

    def test_output_db_records_every_migration_version(
        self, runner: CliRunner, tree_db: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(tree_db),
                "--output-db",
                str(out),
            ],
        )
        assert result.exit_code == 0, result.output

        import asyncio

        from kent.driver.persistent_driver.database import SCHEMA_VERSION

        async def _read_max_version() -> int:
            engine, sf = await init_database(out)
            try:
                async with sf() as session:
                    r = await session.execute(
                        sa.text("SELECT MAX(version) FROM schema_info")
                    )
                    return r.scalar() or 0
            finally:
                await engine.dispose()

        max_version = asyncio.run(_read_max_version())
        assert max_version == SCHEMA_VERSION

    def test_no_errors_is_a_noop(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        import asyncio

        db = tmp_path / "empty_errors.db"

        async def _build() -> None:
            engine, sf = await init_database(db)
            sql = SQLManager(engine, sf)
            await _init_metadata(sql)
            await _insert_root(sql, "https://example.com/r1")
            await engine.dispose()

        asyncio.run(_build())

        out = tmp_path / "out.db"
        result = runner.invoke(
            cli,
            [
                "seed-error-patch-rerun",
                "--db",
                str(db),
                "--output-db",
                str(out),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Nothing to do" in result.output
        assert not out.exists()
