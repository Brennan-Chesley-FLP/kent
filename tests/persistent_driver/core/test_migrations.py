"""Tests for the database migration system."""

from __future__ import annotations

from pathlib import Path

from kent.driver.persistent_driver.database import SCHEMA_VERSION
from kent.driver.persistent_driver.migrations import (
    _scan_migrations,
    get_latest_version,
)


class TestMigrationContinuity:
    """Verify every version from 0 to SCHEMA_VERSION has migration files."""

    def test_latest_version_matches_schema_version(self):
        """get_latest_version() matches SCHEMA_VERSION."""
        assert get_latest_version() == SCHEMA_VERSION

    def test_every_version_has_migration_files(self):
        """Every version from 0 to SCHEMA_VERSION has at least one migration file."""
        by_version = _scan_migrations()
        for version in range(SCHEMA_VERSION + 1):
            assert version in by_version, (
                f"No migration files found for version {version}. "
                f"Add a file like {version:04d}-01.sql or {version:04d}-01.py "
                f"to kent/driver/persistent_driver/migrations/"
            )

    def test_no_gaps_in_versions(self):
        """Version numbers are contiguous from 0 to max."""
        by_version = _scan_migrations()
        versions = sorted(by_version.keys())
        assert versions == list(range(versions[-1] + 1)), (
            f"Gap in migration versions: {versions}"
        )

    def test_steps_within_version_are_contiguous(self):
        """Step numbers within a version start at 1 and are contiguous."""
        by_version = _scan_migrations()
        for version, steps in by_version.items():
            step_nums = [s for s, _ext, _path in steps]
            assert step_nums == list(range(1, len(step_nums) + 1)), (
                f"Version {version} has non-contiguous steps: {step_nums}"
            )


class TestMigrationRunner:
    """Test the migration runner against a real database."""

    async def test_fresh_db_applies_all_migrations(
        self, tmp_path: Path
    ) -> None:
        """A fresh database gets all migrations applied at init."""
        from kent.driver.persistent_driver.database import (
            SCHEMA_VERSION,
            init_database,
        )
        from kent.driver.persistent_driver.migrations import (
            get_current_version,
        )

        db_path = tmp_path / "test.db"
        engine, _session_factory = await init_database(db_path)
        try:
            version = await get_current_version(engine)
            assert version == SCHEMA_VERSION
        finally:
            await engine.dispose()

    async def test_migrate_to_specific_version(self, tmp_path: Path) -> None:
        """migrate_to(target=N) stops at version N."""
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.pool import NullPool
        from sqlmodel import SQLModel

        from kent.driver.persistent_driver.migrations import (
            get_current_version,
            migrate_to,
        )

        db_path = tmp_path / "test.db"
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
            connect_args={"check_same_thread": False},
            poolclass=NullPool,
        )
        # Create tables but don't auto-migrate
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

        applied = await migrate_to(engine, target=15)
        version = await get_current_version(engine)
        assert version == 15
        assert 15 in applied
        assert 16 not in applied

        await engine.dispose()

    async def test_migrate_idempotent(self, tmp_path: Path) -> None:
        """Running migrate_to twice produces no errors."""
        from kent.driver.persistent_driver.database import init_database
        from kent.driver.persistent_driver.migrations import migrate_to

        db_path = tmp_path / "test.db"
        engine, _sf = await init_database(db_path)
        try:
            # Already at latest — should return empty list
            applied = await migrate_to(engine)
            assert applied == []
        finally:
            await engine.dispose()
