"""Database migration runner.

Migrations live in this directory as files named ``{version:04d}-{step:02d}.{sql,py}``.
The version is the **target** schema version; the step orders operations within
a version (allowing Python and SQL to interleave).

- ``.sql`` files are executed statement-by-statement (split on ``;``).
- ``.py`` files must define ``async def migrate(engine) -> bool``.
  Return ``True`` on success, ``False`` to abort the version.

Usage from code::

    from kent.driver.persistent_driver.migrations import migrate_to
    applied = await migrate_to(engine)           # migrate to latest
    applied = await migrate_to(engine, target=16)  # migrate to specific version
"""

from __future__ import annotations

import importlib.util
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import sqlalchemy as sa

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

_MIGRATIONS_DIR = Path(__file__).parent

# Matches filenames like 0016-01.sql or 0016-02.py
_FILE_PATTERN = re.compile(r"^(\d{4})-(\d{2})\.(sql|py)$")


def _scan_migrations() -> dict[int, list[tuple[int, str, Path]]]:
    """Scan the migrations directory and group files by version.

    Returns:
        Dict mapping version → sorted list of (step, extension, path).
    """
    by_version: dict[int, list[tuple[int, str, Path]]] = defaultdict(list)
    for f in _MIGRATIONS_DIR.iterdir():
        m = _FILE_PATTERN.match(f.name)
        if m:
            version = int(m.group(1))
            step = int(m.group(2))
            ext = m.group(3)
            by_version[version].append((step, ext, f))
    # Sort steps within each version
    for v in by_version:
        by_version[v].sort()
    return dict(by_version)


def get_latest_version() -> int:
    """Return the highest version number found in the migrations directory."""
    by_version = _scan_migrations()
    return max(by_version) if by_version else 0


async def get_current_version(engine: AsyncEngine) -> int:
    """Read the current schema version from the database.

    Returns:
        The MAX(version) from schema_info, or 0 if the table is empty.
    """
    async with engine.begin() as conn:
        result = await conn.run_sync(
            lambda c: c.execute(
                sa.text("SELECT MAX(version) FROM schema_info")
            ).scalar()
        )
        return result or 0


async def _run_sql_file(engine: AsyncEngine, path: Path) -> None:
    """Execute a .sql migration file statement-by-statement."""
    content = path.read_text()
    statements = [s.strip() for s in content.split(";") if s.strip()]
    async with engine.begin() as conn:
        for stmt in statements:
            try:
                await conn.execute(sa.text(stmt))
            except Exception:
                # Column/table may already exist (fresh DB via create_all)
                pass


async def _run_py_file(engine: AsyncEngine, path: Path) -> bool:
    """Import and execute a .py migration file.

    The module must define ``async def migrate(engine) -> bool``.

    Returns:
        The return value of migrate(), or True if it returns None.
    """
    # Build a module name from the file path relative to the package
    rel = path.relative_to(_MIGRATIONS_DIR)
    module_name = f"kent.driver.persistent_driver.migrations.{rel.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load migration module: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    migrate_fn = getattr(mod, "migrate", None)
    if migrate_fn is None:
        raise AttributeError(
            f"Migration {path.name} must define 'async def migrate(engine) -> bool'"
        )
    result = await migrate_fn(engine)
    return result if result is not None else True


async def _record_version(engine: AsyncEngine, version: int) -> None:
    """Insert a version row into schema_info."""
    async with engine.begin() as conn:
        await conn.execute(
            sa.text("INSERT INTO schema_info (version) VALUES (:v)"),
            {"v": version},
        )


async def migrate_to(
    engine: AsyncEngine, target: int | None = None
) -> list[int]:
    """Apply all pending migrations up to *target* (default: latest).

    Args:
        engine: An async SQLAlchemy engine connected to the database.
        target: Target schema version. Defaults to the highest version
            found in the migrations directory.

    Returns:
        List of version numbers that were applied.
    """
    by_version = _scan_migrations()
    if target is None:
        target = max(by_version) if by_version else 0

    current = await get_current_version(engine)
    applied: list[int] = []

    for version in sorted(by_version):
        if version <= current or version > target:
            continue

        steps = by_version[version]
        logger.info(f"Applying migration to version {version}...")
        aborted = False

        for _step_num, ext, path in steps:
            if ext == "sql":
                logger.debug(f"  Running {path.name}")
                await _run_sql_file(engine, path)
            elif ext == "py":
                logger.debug(f"  Running {path.name}")
                success = await _run_py_file(engine, path)
                if not success:
                    logger.warning(
                        f"  Migration {path.name} returned False, "
                        f"aborting version {version}"
                    )
                    aborted = True
                    break

        if aborted:
            break

        await _record_version(engine, version)
        applied.append(version)
        logger.info(f"  Version {version} applied.")

    return applied
