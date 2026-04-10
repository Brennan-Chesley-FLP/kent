"""Tests for the incidental_requests → incidental_request_storage migration."""

from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from kent.driver.persistent_driver.compression import compress
from kent.driver.persistent_driver.database import (
    create_engine_and_init,
)


async def _create_v15_db(db_path: Path) -> None:
    """Create a database at schema version 15 with old-style incidental_requests."""
    url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(
        url, connect_args={"check_same_thread": False}, poolclass=NullPool
    )
    async with engine.begin() as conn:
        await conn.execute(
            sa.text(
                "CREATE TABLE schema_info (id INTEGER PRIMARY KEY, version INTEGER, applied_at TEXT)"
            )
        )
        await conn.execute(
            sa.text("INSERT INTO schema_info (version) VALUES (15)")
        )
        await conn.execute(
            sa.text(
                "CREATE TABLE requests (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'pending', "
                "priority INTEGER DEFAULT 9, queue_counter INTEGER, method TEXT, url TEXT, "
                "continuation TEXT, current_location TEXT DEFAULT '', "
                "created_at TEXT DEFAULT CURRENT_TIMESTAMP, "
                "request_type TEXT DEFAULT 'navigating')"
            )
        )
        await conn.execute(
            sa.text(
                "INSERT INTO requests (queue_counter, method, url, continuation) "
                "VALUES (1, 'GET', 'https://example.com', 'parse')"
            )
        )
        # Old-style incidental_requests with all columns inline
        await conn.execute(
            sa.text(
                "CREATE TABLE incidental_requests ("
                "  id INTEGER PRIMARY KEY,"
                "  parent_request_id INTEGER REFERENCES requests(id),"
                "  resource_type TEXT NOT NULL,"
                "  method TEXT NOT NULL,"
                "  url TEXT NOT NULL,"
                "  headers_json TEXT,"
                "  body BLOB,"
                "  status_code INTEGER,"
                "  response_headers_json TEXT,"
                "  content_compressed BLOB,"
                "  content_size_original INTEGER,"
                "  content_size_compressed INTEGER,"
                "  compression_dict_id INTEGER,"
                "  started_at_ns INTEGER,"
                "  completed_at_ns INTEGER,"
                "  from_cache BOOLEAN,"
                "  failure_reason TEXT,"
                "  created_at TEXT DEFAULT CURRENT_TIMESTAMP"
                ")"
            )
        )
    await engine.dispose()
    return engine


async def _insert_old_incidental(
    db_path: Path,
    parent_id: int,
    resource_type: str,
    url: str,
    content: bytes | None = None,
    status_code: int | None = 200,
    failure_reason: str | None = None,
) -> int:
    url_str = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(
        url_str, connect_args={"check_same_thread": False}, poolclass=NullPool
    )
    compressed = compress(content) if content else None
    async with engine.begin() as conn:
        result = await conn.execute(
            sa.text(
                "INSERT INTO incidental_requests "
                "(parent_request_id, resource_type, method, url, status_code, "
                "content_compressed, content_size_original, content_size_compressed, "
                "failure_reason) "
                "VALUES (:pid, :rt, 'GET', :url, :sc, :cc, :cso, :csc, :fr)"
            ),
            {
                "pid": parent_id,
                "rt": resource_type,
                "url": url,
                "sc": status_code,
                "cc": compressed,
                "cso": len(content) if content else None,
                "csc": len(compressed) if compressed else None,
                "fr": failure_reason,
            },
        )
        ir_id = result.lastrowid
    await engine.dispose()
    return ir_id


class TestSchemaMigration:
    async def test_fresh_db_has_both_tables(self, tmp_path: Path) -> None:
        """A fresh database created with create_engine_and_init has both tables."""
        db_path = tmp_path / "fresh.db"
        engine = await create_engine_and_init(db_path)

        async with engine.begin() as conn:
            tables = await conn.run_sync(
                lambda c: [
                    row[0]
                    for row in c.execute(
                        sa.text(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        )
                    ).fetchall()
                ]
            )
        assert "incidental_requests" in tables
        assert "incidental_request_storage" in tables

        # Verify incidental_requests has storage_id and NOT old columns
        async with engine.begin() as conn:
            cols = await conn.run_sync(
                lambda c: [
                    row[1]
                    for row in c.execute(
                        sa.text("PRAGMA table_info(incidental_requests)")
                    ).fetchall()
                ]
            )
        assert "storage_id" in cols
        assert "url" in cols
        # Old columns should NOT exist on fresh DB
        assert "resource_type" not in cols
        assert "content_compressed" not in cols

        await engine.dispose()

    async def test_migration_creates_storage_table(
        self, tmp_path: Path
    ) -> None:
        """Migrating from v15 creates the storage table and adds storage_id."""
        db_path = tmp_path / "v15.db"
        await _create_v15_db(db_path)

        # Apply migration via create_engine_and_init (runs _apply_migrations)
        engine = await create_engine_and_init(db_path)

        async with engine.begin() as conn:
            tables = await conn.run_sync(
                lambda c: [
                    row[0]
                    for row in c.execute(
                        sa.text(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        )
                    ).fetchall()
                ]
            )
        assert "incidental_request_storage" in tables

        # Verify storage_id column was added
        async with engine.begin() as conn:
            cols = await conn.run_sync(
                lambda c: [
                    row[1]
                    for row in c.execute(
                        sa.text("PRAGMA table_info(incidental_requests)")
                    ).fetchall()
                ]
            )
        assert "storage_id" in cols

        # Version should be at least 16 (may be higher with new migrations)
        async with engine.begin() as conn:
            version = await conn.run_sync(
                lambda c: c.execute(
                    sa.text("SELECT MAX(version) FROM schema_info")
                ).scalar()
            )
        assert version >= 16

        await engine.dispose()


class TestDataMigration:
    async def test_backfill_and_dedup(self, tmp_path: Path) -> None:
        """The migration script backfills storage rows and deduplicates."""
        db_path = tmp_path / "migrate.db"
        await _create_v15_db(db_path)

        # Insert old-style rows with duplicate content
        content = b"shared-css-content"
        await _insert_old_incidental(
            db_path,
            1,
            "stylesheet",
            "https://cdn.example.com/style.css",
            content,
        )
        await _insert_old_incidental(
            db_path,
            1,
            "stylesheet",
            "https://cdn.example.com/style.css",
            content,
        )
        # A different resource
        await _insert_old_incidental(
            db_path,
            1,
            "script",
            "https://cdn.example.com/app.js",
            b"unique-js",
        )
        # A failed request with no content
        await _insert_old_incidental(
            db_path,
            1,
            "image",
            "https://cdn.example.com/img.png",
            content=None,
            status_code=None,
            failure_reason="blocked",
        )

        # Run the migration script
        # Run the migration via the new migration system
        migrate_engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
            connect_args={"check_same_thread": False},
            poolclass=NullPool,
        )
        from kent.driver.persistent_driver.migrations import migrate_to

        await migrate_to(migrate_engine, target=16)
        await migrate_engine.dispose()

        # Verify results
        url = f"sqlite+aiosqlite:///{db_path}"
        engine = create_async_engine(
            url, connect_args={"check_same_thread": False}, poolclass=NullPool
        )

        async with engine.begin() as conn:
            # All rows should have storage_id
            null_count = await conn.run_sync(
                lambda c: c.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM incidental_requests WHERE storage_id IS NULL"
                    )
                ).scalar()
            )
            assert null_count == 0

            # Deduplication: 2 identical CSS + 1 unique JS + 1 failed = 3 storage rows
            storage_count = await conn.run_sync(
                lambda c: c.execute(
                    sa.text("SELECT COUNT(*) FROM incidental_request_storage")
                ).scalar()
            )
            assert storage_count == 3

            # The two CSS rows share the same storage_id
            css_storage_ids = await conn.run_sync(
                lambda c: [
                    row[0]
                    for row in c.execute(
                        sa.text(
                            "SELECT DISTINCT storage_id FROM incidental_requests "
                            "WHERE url = 'https://cdn.example.com/style.css'"
                        )
                    ).fetchall()
                ]
            )
            assert len(css_storage_ids) == 1

            # Old columns should be gone
            cols = await conn.run_sync(
                lambda c: [
                    row[1]
                    for row in c.execute(
                        sa.text("PRAGMA table_info(incidental_requests)")
                    ).fetchall()
                ]
            )
            assert "content_compressed" not in cols
            assert "resource_type" not in cols
            assert "storage_id" in cols

        await engine.dispose()

    async def test_migration_idempotent(self, tmp_path: Path) -> None:
        """Running migration twice produces the same result."""
        db_path = tmp_path / "idempotent.db"
        await _create_v15_db(db_path)
        await _insert_old_incidental(
            db_path, 1, "script", "https://cdn.example.com/a.js", b"content"
        )

        # Run the migration via the new migration system
        migrate_engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
            connect_args={"check_same_thread": False},
            poolclass=NullPool,
        )
        from kent.driver.persistent_driver.migrations import migrate_to

        await migrate_to(migrate_engine, target=16)
        # Run again — should be a no-op (idempotent)
        await migrate_to(migrate_engine, target=16)
        await migrate_engine.dispose()

        url = f"sqlite+aiosqlite:///{db_path}"
        engine = create_async_engine(
            url, connect_args={"check_same_thread": False}, poolclass=NullPool
        )
        async with engine.begin() as conn:
            storage_count = await conn.run_sync(
                lambda c: c.execute(
                    sa.text("SELECT COUNT(*) FROM incidental_request_storage")
                ).scalar()
            )
            assert storage_count == 1
        await engine.dispose()
