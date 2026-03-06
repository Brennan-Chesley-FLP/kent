"""Scraper discovery by filesystem scanning.

Provides the shared logic used by both ``kent list`` (CLI) and
``kent serve`` (web registry) to find BaseScraper subclasses.
"""

from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from kent.data_types import BaseScraper

logger = logging.getLogger(__name__)

_SKIP_DIRS = frozenset({
    "__pycache__",
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    ".env",
    "env",
    ".tox",
    ".nox",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "node_modules",
    ".eggs",
    "dist",
    "build",
})


def discover_scrapers(
    root: Path,
) -> Iterator[tuple[str, str, type[BaseScraper]]]:
    """Discover BaseScraper subclasses in ``.py`` files under *root*.

    Walks the directory tree, skipping virtual-env and cache
    directories.  Only files whose source text contains
    ``"BaseScraper"`` are imported, keeping the scan fast.

    *root* is added to ``sys.path`` (if absent) so that relative
    package imports resolve correctly.

    Yields:
        ``(module_path, class_name, scraper_class)`` tuples.
    """
    from kent.data_types import BaseScraper

    root = root.resolve()
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    for py_file in root.rglob("*.py"):
        if any(part in _SKIP_DIRS for part in py_file.parts):
            continue

        try:
            source = py_file.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if "BaseScraper" not in source:
            continue

        rel = py_file.relative_to(root).with_suffix("")
        parts = list(rel.parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if not parts:
            continue
        module_path = ".".join(parts)

        try:
            module = importlib.import_module(module_path)
        except Exception as exc:
            logger.debug(
                "Skipping %s: %s: %s",
                module_path,
                type(exc).__name__,
                exc,
            )
            continue

        for name in dir(module):
            obj = getattr(module, name, None)
            if (
                isinstance(obj, type)
                and issubclass(obj, BaseScraper)
                and obj is not BaseScraper
                and obj.__module__ == module.__name__
            ):
                yield module_path, name, obj
