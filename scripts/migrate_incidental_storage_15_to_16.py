#!/usr/bin/env python3
"""Migrate incidental_requests data into the split storage table.

Applies the schema migration (version 15 → 16) then backfills the new
incidental_request_storage table from existing incidental_requests rows,
deduplicating on content MD5.  Finally drops the old columns that have
been moved to the storage table.

Usage:
    uv run python scripts/migrate_incidental_storage_15_to_16.py <database_path>
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

BATCH_SIZE = 1000

# Columns to drop from incidental_requests after migration.
# SQLite doesn't support DROP COLUMN before 3.35, so we recreate the table.
OLD_COLUMNS_TO_DROP = {
    "resource_type",
    "method",
    "body",
    "status_code",
    "response_headers_json",
    "content_compressed",
    "content_size_original",
    "content_size_compressed",
    "compression_dict_id",
    "failure_reason",
}

KEPT_COLUMNS = [
    "id",
    "parent_request_id",
    "url",
    "headers_json",
    "started_at_ns",
    "completed_at_ns",
    "from_cache",
    "created_at",
    "storage_id",
]


async def migrate(db_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(
        url,
        connect_args={"check_same_thread": False},
        poolclass=NullPool,
    )

    async with engine.begin() as conn:
        # Check current version
        version = await conn.run_sync(
            lambda c: c.execute(
                sa.text("SELECT MAX(version) FROM schema_info")
            ).scalar()
            or 0
        )
        if version < 15:
            print(
                f"Database is at version {version}, expected >= 15. Aborting."
            )
            return
        if version >= 16:
            print(
                f"Database already at version {version}, schema migration not needed."
            )
        else:
            # Apply schema migration
            stmts = [
                """CREATE TABLE IF NOT EXISTS incidental_request_storage (
                    id INTEGER PRIMARY KEY,
                    resource_type TEXT NOT NULL,
                    url TEXT NOT NULL,
                    method TEXT NOT NULL,
                    body BLOB,
                    status_code INTEGER,
                    response_headers_json TEXT,
                    content_compressed BLOB,
                    content_size_original INTEGER,
                    content_size_compressed INTEGER,
                    compression_dict_id INTEGER REFERENCES compression_dicts(id),
                    failure_reason TEXT,
                    content_md5 TEXT
                )""",
                "CREATE INDEX IF NOT EXISTS idx_irs_content_md5 ON incidental_request_storage(content_md5)",
                "ALTER TABLE incidental_requests ADD COLUMN storage_id INTEGER REFERENCES incidental_request_storage(id)",
                "CREATE INDEX IF NOT EXISTS idx_incidental_requests_storage ON incidental_requests(storage_id)",
            ]
            for stmt in stmts:
                try:
                    await conn.execute(sa.text(stmt))
                except Exception:
                    pass  # column/table may already exist on fresh DB
            await conn.execute(
                sa.text("INSERT INTO schema_info (version) VALUES (:v)"),
                {"v": 16},
            )
            print("Schema migration to version 16 applied.")

    # --- Data migration: backfill storage rows ---
    md5_to_storage_id: dict[str, int] = {}
    total_rows = 0
    deduped = 0
    created = 0

    async with engine.begin() as conn:
        # Check if old columns still exist (they won't on a fresh v16 DB)
        cols = await conn.run_sync(
            lambda c: [
                row[1]
                for row in c.execute(
                    sa.text("PRAGMA table_info(incidental_requests)")
                ).fetchall()
            ]
        )
        if "content_compressed" not in cols:
            print("Old columns already removed — no data migration needed.")
            await engine.dispose()
            return

        # Count rows needing migration
        count = await conn.run_sync(
            lambda c: c.execute(
                sa.text(
                    "SELECT COUNT(*) FROM incidental_requests WHERE storage_id IS NULL"
                )
            ).scalar()
        )
        print(f"Rows to migrate: {count}")

        offset = 0
        while True:
            rows = await conn.run_sync(
                lambda c, off=offset: c.execute(
                    sa.text(
                        "SELECT id, resource_type, method, url, body, status_code, "
                        "response_headers_json, content_compressed, content_size_original, "
                        "content_size_compressed, compression_dict_id, failure_reason "
                        "FROM incidental_requests WHERE storage_id IS NULL "
                        "ORDER BY id LIMIT :limit OFFSET :offset"
                    ),
                    {"limit": BATCH_SIZE, "offset": off},
                ).fetchall()
            )
            if not rows:
                break

            for row in rows:
                total_rows += 1
                (
                    ir_id,
                    resource_type,
                    method,
                    url,
                    body,
                    status_code,
                    resp_headers,
                    content_compressed,
                    size_orig,
                    size_comp,
                    dict_id,
                    failure,
                ) = row

                content_md5 = None
                if content_compressed is not None:
                    content_md5 = hashlib.md5(content_compressed).hexdigest()

                storage_id = (
                    md5_to_storage_id.get(content_md5) if content_md5 else None
                )

                if storage_id is None:
                    result = await conn.run_sync(
                        lambda c, **kw: c.execute(
                            sa.text(
                                "INSERT INTO incidental_request_storage "
                                "(resource_type, url, method, body, status_code, "
                                "response_headers_json, content_compressed, "
                                "content_size_original, content_size_compressed, "
                                "compression_dict_id, failure_reason, content_md5) "
                                "VALUES (:resource_type, :url, :method, :body, "
                                ":status_code, :response_headers_json, "
                                ":content_compressed, :content_size_original, "
                                ":content_size_compressed, :compression_dict_id, "
                                ":failure_reason, :content_md5)"
                            ),
                            kw,
                        ).lastrowid,
                        resource_type=resource_type or "",
                        url=url or "",
                        method=method or "GET",
                        body=body,
                        status_code=status_code,
                        response_headers_json=resp_headers,
                        content_compressed=content_compressed,
                        content_size_original=size_orig,
                        content_size_compressed=size_comp,
                        compression_dict_id=dict_id,
                        failure_reason=failure,
                        content_md5=content_md5,
                    )
                    storage_id = result
                    if content_md5:
                        md5_to_storage_id[content_md5] = storage_id
                    created += 1
                else:
                    deduped += 1

                await conn.run_sync(
                    lambda c, sid=storage_id, iid=ir_id: c.execute(
                        sa.text(
                            "UPDATE incidental_requests SET storage_id = :sid WHERE id = :id"
                        ),
                        {"sid": sid, "id": iid},
                    )
                )

            offset += BATCH_SIZE
            print(f"  Processed {total_rows}/{count} rows...")

    print(
        f"Data migration complete: {total_rows} rows processed, "
        f"{created} storage rows created, {deduped} deduplicated."
    )

    # --- Drop old columns by recreating the table ---
    print("Dropping old columns from incidental_requests...")
    kept_cols = ", ".join(KEPT_COLUMNS)
    async with engine.begin() as conn:
        await conn.execute(
            sa.text(
                "CREATE TABLE incidental_requests_new ("
                "  id INTEGER PRIMARY KEY,"
                "  parent_request_id INTEGER NOT NULL REFERENCES requests(id),"
                "  url TEXT NOT NULL,"
                "  headers_json TEXT,"
                "  started_at_ns INTEGER,"
                "  completed_at_ns INTEGER,"
                "  from_cache BOOLEAN,"
                "  created_at TEXT DEFAULT CURRENT_TIMESTAMP,"
                "  storage_id INTEGER REFERENCES incidental_request_storage(id)"
                ")"
            )
        )
        await conn.execute(
            sa.text(
                f"INSERT INTO incidental_requests_new ({kept_cols}) "
                f"SELECT {kept_cols} FROM incidental_requests"
            )
        )
        await conn.execute(sa.text("DROP TABLE incidental_requests"))
        await conn.execute(
            sa.text(
                "ALTER TABLE incidental_requests_new RENAME TO incidental_requests"
            )
        )
        # Recreate indexes
        await conn.execute(
            sa.text(
                "CREATE INDEX idx_incidental_requests_parent "
                "ON incidental_requests(parent_request_id)"
            )
        )
        await conn.execute(
            sa.text(
                "CREATE INDEX idx_incidental_requests_storage "
                "ON incidental_requests(storage_id)"
            )
        )

    # --- Reclaim space from dropped columns/table ---
    print("Running VACUUM to reclaim space...")
    async with engine.begin() as conn:
        await conn.execute(sa.text("VACUUM"))

    print("Migration complete.")
    await engine.dispose()


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <database_path>")
        sys.exit(1)
    db_path = Path(sys.argv[1])
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)
    asyncio.run(migrate(db_path))


if __name__ == "__main__":
    main()
