"""Kent CLI — run scrapers and start the web UI.

Usage:
    kent list                               # List available scrapers
    kent inspect module.path:ScraperClass   # Show scraper metadata
    kent inspect ... --seed-params          # Output seed params JSON
    kent serve                              # Start the persistent driver web UI
    kent run module.path:ScraperClass       # Run a scraper (default: persistent driver)
    kent run module.path:ScraperClass --driver sync
"""

from __future__ import annotations

import asyncio
import importlib
import inspect as inspect_mod
import json
import logging
import sys
from pathlib import Path
from typing import Any

import click


def import_scraper(scraper_path: str) -> type:
    """Import a scraper class from a dotted path.

    Args:
        scraper_path: ``"module.path:ClassName"`` string.

    Returns:
        The scraper class.

    Raises:
        click.BadParameter: If the format is invalid or import fails.
    """
    if ":" not in scraper_path:
        raise click.BadParameter(
            f"Invalid scraper path '{scraper_path}'. "
            "Expected format: 'module.path:ClassName'"
        )

    module_path, class_name = scraper_path.rsplit(":", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise click.BadParameter(
            f"Could not import module '{module_path}': {e}"
        ) from e

    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise click.BadParameter(
            f"Module '{module_path}' has no class '{class_name}'"
        ) from e


def _example_value(param_type: type) -> Any:
    """Return a representative example value for a parameter type."""
    from datetime import date

    from pydantic import BaseModel as PydanticBaseModel

    if param_type is int:
        return 1
    if param_type is str:
        return "example"
    if param_type is date:
        return "2025-01-01"
    if isinstance(param_type, type) and issubclass(
        param_type, PydanticBaseModel
    ):
        # Build example from the model's field info
        example: dict[str, Any] = {}
        for field_name, field_info in param_type.model_fields.items():
            annotation = field_info.annotation
            if annotation is int:
                example[field_name] = 1
            elif annotation is str:
                example[field_name] = "example"
            elif annotation is date:
                example[field_name] = "2025-01-01"
            else:
                example[field_name] = None
        return example
    return None


@click.group()
@click.version_option(package_name="kent")
def cli() -> None:
    """Kent — scraper-driver framework CLI."""


_SKIP_DIRS = {
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
}


def _discover_scrapers(
    root: Path, verbose: bool = False
) -> list[tuple[str, type]]:
    """Discover BaseScraper subclasses in ``.py`` files under *root*.

    Walks the directory tree, skipping virtual-env and cache
    directories.  Only files whose source text contains
    ``"BaseScraper"`` are imported, keeping the scan fast.

    *root* is added to ``sys.path`` (if absent) so that relative
    package imports resolve correctly.

    Returns:
        Sorted list of ``("module.path:ClassName", class)`` tuples.
    """
    from kent.data_types import BaseScraper

    root = root.resolve()
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    found: list[tuple[str, type]] = []

    for py_file in root.rglob("*.py"):
        # Skip hidden / non-project directories
        if any(part in _SKIP_DIRS for part in py_file.parts):
            continue

        # Quick text pre-filter
        try:
            source = py_file.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if "BaseScraper" not in source:
            continue

        # Derive dotted module path relative to root
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
            if verbose:
                click.echo(
                    f"  skip {module_path}: "
                    f"{type(exc).__name__}: {exc}",
                    err=True,
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
                found.append((f"{module_path}:{name}", obj))

    found.sort(key=lambda t: t[0])
    return found


@cli.command("list")
@click.option("-v", "--verbose", is_flag=True, help="Show import errors.")
def list_scrapers(verbose: bool) -> None:
    """List available scrapers in the current directory tree.

    Scans ``.py`` files under the working directory for
    BaseScraper subclasses.
    """
    scrapers = _discover_scrapers(Path.cwd(), verbose=verbose)
    if not scrapers:
        click.echo("No scrapers found.")
        return

    for full_path, cls in scrapers:
        status = getattr(cls, "status", None)
        status_str = f" [{status.value}]" if status else ""
        entries = cls.list_entries()
        entry_names = ", ".join(e.name for e in entries)
        entries_str = f"  entries: {entry_names}" if entries else ""
        click.echo(f"{full_path}{status_str}{entries_str}")


@cli.command()
@click.argument("scraper")
@click.option(
    "--seed-params",
    is_flag=True,
    help=(
        "Output only a JSON seed-params list suitable for "
        "``kent run --params``."
    ),
)
def inspect(scraper: str, seed_params: bool) -> None:
    """Inspect a scraper's metadata and entry points.

    SCRAPER is a dotted import path in the form module.path:ClassName.

    \b
    Examples:
        kent inspect kent.demo.scraper:BugCourtDemoScraper
        kent inspect kent.demo.scraper:BugCourtDemoScraper --seed-params
    """
    scraper_class = import_scraper(scraper)

    entries = scraper_class.list_entries()

    # --seed-params: emit JSON and exit
    if seed_params:
        params_list: list[dict[str, dict[str, Any]]] = []
        for entry_info in entries:
            kwargs: dict[str, Any] = {}
            for pname, ptype in entry_info.param_types.items():
                kwargs[pname] = _example_value(ptype)
            params_list.append({entry_info.name: kwargs})
        click.echo(json.dumps(params_list, indent=2))
        return

    # -- Human-readable output ----------------------------------------

    click.echo(f"Class:     {scraper_class.__name__}")
    click.echo(
        f"Module:    {scraper_class.__module__}"
    )

    status = getattr(scraper_class, "status", None)
    if status is not None:
        click.echo(f"Status:    {status.value}")

    version = getattr(scraper_class, "version", "")
    if version:
        click.echo(f"Version:   {version}")

    court_url = getattr(scraper_class, "court_url", "")
    if court_url:
        click.echo(f"Court URL: {court_url}")

    court_ids = getattr(scraper_class, "court_ids", set())
    if court_ids:
        click.echo(f"Court IDs: {', '.join(sorted(court_ids))}")

    data_types = getattr(scraper_class, "data_types", set())
    if data_types:
        click.echo(f"Data types: {', '.join(sorted(data_types))}")

    oldest = getattr(scraper_class, "oldest_record", None)
    if oldest is not None:
        click.echo(f"Oldest record: {oldest}")

    last_verified = getattr(scraper_class, "last_verified", "")
    if last_verified:
        click.echo(f"Last verified: {last_verified}")

    if getattr(scraper_class, "requires_auth", False):
        click.echo("Auth:      required")

    rate_limits = getattr(scraper_class, "rate_limits", None)
    if rate_limits:
        parts = []
        for r in rate_limits:
            parts.append(f"{r.limit}/{r.interval}ms")
        click.echo(f"Rate limits: {', '.join(parts)}")

    # Entry points
    if entries:
        click.echo(f"\nEntry points ({len(entries)}):")
        for entry_info in entries:
            spec_tag = " [speculative]" if entry_info.speculative else ""
            returns = entry_info.return_type.__name__
            click.echo(
                f"  {entry_info.name}{spec_tag} -> {returns}"
            )
            if entry_info.param_types:
                for pname, ptype in entry_info.param_types.items():
                    click.echo(f"    {pname}: {ptype.__name__}")

    # Steps
    steps = scraper_class.list_steps()
    if steps:
        click.echo(f"\nSteps ({len(steps)}):")
        for step_info in steps:
            # Determine response type tags from signature
            method = getattr(scraper_class, step_info.name, None)
            tag = ""
            if method is not None:
                sig = inspect_mod.signature(method)
                param_names = set(sig.parameters) - {"self"}
                if "page" in param_names or "lxml_tree" in param_names:
                    tag = "html"
                elif "json_content" in param_names:
                    tag = "json"
                elif "response" in param_names:
                    tag = "file"

            tag_str = f" [{tag}]" if tag else ""
            click.echo(f"  {step_info.name}{tag_str}")


@cli.command()
@click.option(
    "--runs-dir",
    default="runs",
    show_default=True,
    help="Directory containing run databases.",
)
@click.option(
    "--host",
    default="127.0.0.1",
    show_default=True,
    help="Host to bind the server to.",
)
@click.option(
    "--port",
    default=8000,
    show_default=True,
    type=int,
    help="Port to bind the server to.",
)
@click.option("-v", "--verbose", is_flag=True, help="Verbose logging.")
def serve(runs_dir: str, host: str, port: int, verbose: bool) -> None:
    """Start the persistent driver web UI."""
    try:
        import uvicorn  # noqa: F811

        from kent.driver.persistent_driver.web.app import create_app
    except ImportError as e:
        raise click.ClickException(
            f"Missing dependency: {e}. "
            "Install the 'web' and 'persistent-driver' extras: "
            "pip install kent[web,persistent-driver]"
        ) from e

    runs_path = Path(runs_dir)
    runs_path.mkdir(parents=True, exist_ok=True)

    app = create_app(runs_dir=runs_path)

    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("kent.driver.persistent_driver").setLevel(log_level)

    click.echo(f"Starting web server at http://{host}:{port}")
    click.echo(f"Runs directory: {runs_path.absolute()}")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info" if verbose else "warning",
    )


@cli.command()
@click.argument("scraper")
@click.option(
    "--driver",
    "driver_name",
    type=click.Choice(["sync", "async", "persistent", "playwright"]),
    default="persistent",
    show_default=True,
    help="Driver to use.",
)
@click.option(
    "--db",
    "db_path",
    type=click.Path(),
    default=None,
    help="SQLite database path (persistent/playwright).",
)
@click.option(
    "--workers",
    type=int,
    default=1,
    show_default=True,
    help="Number of concurrent workers (async/persistent/playwright).",
)
@click.option(
    "--storage",
    type=click.Path(),
    default=None,
    help="Directory for downloaded files.",
)
@click.option(
    "--no-resume",
    is_flag=True,
    help="Start fresh instead of resuming (persistent/playwright).",
)
@click.option(
    "--params",
    "params_json",
    default=None,
    help=(
        "JSON list of seed parameters for initial_seed(). "
        'Example: \'[{"get_oral_arguments": {}}]\'. '
        "Use ``kent inspect --seed-params`` to generate a template."
    ),
)
@click.option("-v", "--verbose", is_flag=True, help="Verbose logging.")
def run(
    scraper: str,
    driver_name: str,
    db_path: str | None,
    workers: int,
    storage: str | None,
    no_resume: bool,
    params_json: str | None,
    verbose: bool,
) -> None:
    """Run a scraper with the chosen driver.

    SCRAPER is a dotted import path in the form module.path:ClassName.

    \b
    Examples:
        kent run kent.demo.scraper:BugCourtDemoScraper
        kent run kent.demo.scraper:BugCourtDemoScraper --driver sync
        kent run my.scraper:MyScraper --driver persistent --db run.db
        kent run my.scraper:MyScraper --params '[{"get_entry": {}}]'
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    scraper_class = import_scraper(scraper)
    scraper_instance = scraper_class()
    scraper_name = scraper_class.__name__

    storage_dir = Path(storage) if storage else None
    if storage_dir:
        storage_dir.mkdir(parents=True, exist_ok=True)

    seed_params: list[dict[str, dict[str, Any]]] | None = None
    if params_json is not None:
        try:
            seed_params = json.loads(params_json)
        except json.JSONDecodeError as e:
            raise click.BadParameter(
                f"Invalid JSON for --params: {e}"
            ) from e
        if not isinstance(seed_params, list):
            raise click.BadParameter(
                "--params must be a JSON list"
            )

    click.echo(f"Scraper: {scraper_name}")
    click.echo(f"Driver:  {driver_name}")

    if driver_name == "sync":
        _run_sync(scraper_instance, storage_dir, seed_params)
    elif driver_name == "async":
        _run_async(scraper_instance, storage_dir, workers, seed_params)
    elif driver_name == "persistent":
        _run_persistent(
            scraper_instance,
            scraper_name,
            db_path,
            storage_dir,
            workers,
            no_resume,
            seed_params,
        )
    elif driver_name == "playwright":
        _run_playwright(
            scraper_instance,
            scraper_name,
            db_path,
            storage_dir,
            workers,
            no_resume,
            seed_params,
        )


# ------------------------------------------------------------------
# Driver runners
# ------------------------------------------------------------------


def _run_sync(
    scraper: Any,
    storage_dir: Path | None,
    seed_params: list[dict[str, dict[str, Any]]] | None,
) -> None:
    from kent.driver.sync_driver import SyncDriver

    driver = SyncDriver(scraper=scraper, storage_dir=storage_dir)
    driver.seed_params = seed_params
    driver.run()
    click.echo("Done.")


def _run_async(
    scraper: Any,
    storage_dir: Path | None,
    workers: int,
    seed_params: list[dict[str, dict[str, Any]]] | None,
) -> None:
    from kent.driver.async_driver import AsyncDriver

    async def _go() -> None:
        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=storage_dir,
            num_workers=workers,
        )
        driver.seed_params = seed_params
        await driver.run()

    asyncio.run(_go())
    click.echo("Done.")


def _run_persistent(
    scraper: Any,
    scraper_name: str,
    db_path: str | None,
    storage_dir: Path | None,
    workers: int,
    no_resume: bool,
    seed_params: list[dict[str, dict[str, Any]]] | None,
) -> None:
    try:
        from kent.driver.persistent_driver import PersistentDriver
    except ImportError as e:
        raise click.ClickException(
            f"Missing dependency: {e}. "
            "Install the 'persistent-driver' extra: "
            "pip install kent[persistent-driver]"
        ) from e

    resolved_db = Path(db_path) if db_path else Path(f"{scraper_name}.db")
    click.echo(f"Database: {resolved_db}")

    async def _go() -> None:
        async with PersistentDriver.open(
            scraper=scraper,
            db_path=resolved_db,
            storage_dir=storage_dir,
            num_workers=workers,
            resume=not no_resume,
            seed_params=seed_params,
        ) as driver:
            await driver.run()

    asyncio.run(_go())
    click.echo("Done.")


def _run_playwright(
    scraper: Any,
    scraper_name: str,
    db_path: str | None,
    storage_dir: Path | None,
    workers: int,
    no_resume: bool,
    seed_params: list[dict[str, dict[str, Any]]] | None,
) -> None:
    try:
        from kent.driver.playwright_driver import PlaywrightDriver
    except ImportError as e:
        raise click.ClickException(
            f"Missing dependency: {e}. "
            "Install the 'playwright' and 'persistent-driver' extras: "
            "pip install kent[playwright,persistent-driver]"
        ) from e

    resolved_db = Path(db_path) if db_path else Path(f"{scraper_name}.db")
    click.echo(f"Database: {resolved_db}")

    async def _go() -> None:
        async with PlaywrightDriver.open(
            scraper=scraper,
            db_path=resolved_db,
            storage_dir=storage_dir,
            num_workers=workers,
            resume=not no_resume,
            seed_params=seed_params,
        ) as driver:
            await driver.run()

    asyncio.run(_go())
    click.echo("Done.")


def main() -> None:
    """Entry point for the ``kent`` console script."""
    cli()
