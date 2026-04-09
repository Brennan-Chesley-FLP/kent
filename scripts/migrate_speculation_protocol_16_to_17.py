#!/usr/bin/env python3
"""Migrate speculation_tracking table for the Speculative protocol.

Applies the schema migration (version 16 → 17) which adds param_index
and template_json columns to the speculation_tracking table.

Usage:
    uv run python scripts/migrate_speculation_protocol_16_to_17.py <database_path>
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

MIGRATION_STMTS = [
    "ALTER TABLE speculation_tracking ADD COLUMN param_index INTEGER DEFAULT 0",
    "ALTER TABLE speculation_tracking ADD COLUMN template_json TEXT",
]


async def migrate(db_path: Path) -> None:
    """Run the v16→v17 migration on the given database."""
    if not db_path.exists():
        print(f"Error: database not found at {db_path}")
        sys.exit(1)

    url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(url, poolclass=NullPool)

    async with engine.begin() as conn:
        # Check current version
        current = await conn.run_sync(
            lambda c: c.execute(
                sa.text("SELECT MAX(version) FROM schema_info")
            ).scalar()
            or 0
        )

        if current >= 17:
            print(f"Database already at version {current}, nothing to do.")
            return

        if current < 16:
            print(
                f"Database is at version {current}, expected 16. "
                f"Apply earlier migrations first."
            )
            sys.exit(1)

        print(f"Migrating from version {current} to 17...")

        for stmt in MIGRATION_STMTS:
            try:
                await conn.execute(sa.text(stmt))
                print(f"  OK: {stmt[:60]}...")
            except Exception as e:
                # Column may already exist (fresh DB via create_all)
                print(f"  SKIP (already exists?): {e}")

        await conn.execute(
            sa.text("INSERT INTO schema_info (version) VALUES (17)")
        )
        print("Migration complete: version 16 → 17")

    await engine.dispose()


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <database_path>")
        sys.exit(1)

    db_path = Path(sys.argv[1])
    asyncio.run(migrate(db_path))


if __name__ == "__main__":
    main()
