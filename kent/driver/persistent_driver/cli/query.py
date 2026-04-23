"""``pdd query`` — run parameterized SQL queries against a scraper run DB.

Queries are bundled as JSON files with shape::

    {
        "schema_version": 19,
        "description": "Human-readable description.",
        "query": "SELECT … FROM requests WHERE continuation = :step;",
        "params": ["step"]
    }

Resolution order for a named query:

1. ``~/.config/kent/queries/<name>.json`` (user override)
2. ``<this_dir>/queries/<name>.json`` (built-in)

The DB is always opened read-only.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from typing import Any

import click
import sqlalchemy as sa
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)
from typing_extensions import Self

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.cli._options import (
    db_option,
    format_options,
)
from kent.driver.persistent_driver.cli.templating import render_output
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

BUILTIN_QUERIES_DIR = Path(__file__).parent / "queries"
USER_QUERIES_DIR = Path.home() / ".config" / "kent" / "queries"

RESERVED_NAMES = frozenset({"list", "run"})

# Matches :name placeholders. Not perfect for SQL string literals, but
# good enough to give a clear error before we hit the DB.
_PLACEHOLDER_RE = re.compile(r":([A-Za-z_]\w*)")


async def _read_schema_version(conn: Any) -> int:
    """Read the highest version from schema_info. Returns 0 on error."""
    try:
        result = await conn.execute(
            sa.text(
                "SELECT version FROM schema_info ORDER BY version DESC LIMIT 1"
            )
        )
        row = result.first()
        return int(row[0]) if row else 0
    except Exception:
        return 0


# =========================================================================
# Query definition
# =========================================================================


class QueryDef(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: int
    description: str
    query: str
    params: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _params_match_placeholders(self) -> Self:
        declared = set(self.params)
        referenced = set(_PLACEHOLDER_RE.findall(self.query))
        if declared != referenced:
            raise ValueError(
                f"declared params {sorted(declared)} do not match SQL "
                f"placeholders {sorted(referenced)}"
            )
        return self


# =========================================================================
# Resolution & collection
# =========================================================================


def _candidate_paths(name: str) -> list[Path]:
    return [
        USER_QUERIES_DIR / f"{name}.json",
        BUILTIN_QUERIES_DIR / f"{name}.json",
    ]


def _load_query_file(path: Path) -> QueryDef:
    return QueryDef.model_validate_json(path.read_text())


def _resolve_named(name: str) -> tuple[QueryDef, Path]:
    if name in RESERVED_NAMES:
        raise click.ClickException(
            f"{name!r} is reserved and cannot be used as a query name"
        )
    tried = _candidate_paths(name)
    for path in tried:
        if path.is_file():
            return _load_query_file(path), path
    searched = "\n  ".join(str(p) for p in tried)
    raise click.ClickException(
        f"No query named {name!r}. Searched:\n  {searched}"
    )


def _source_label(path: Path) -> str:
    try:
        if path.is_relative_to(USER_QUERIES_DIR):
            return "user"
    except AttributeError:
        # Path.is_relative_to is 3.9+, but kent requires 3.10+.
        pass
    if path.is_relative_to(BUILTIN_QUERIES_DIR):
        return "builtin"
    return "external"


def _collect_queries() -> tuple[list[dict[str, Any]], list[str]]:
    """Walk both dirs and build a listing.

    Later entries (user) shadow earlier ones (builtin) by name.
    Returns (entries, warnings).
    """
    found: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []

    def _scan(directory: Path, source: str) -> None:
        if not directory.is_dir():
            return
        for path in sorted(directory.glob("*.json")):
            name = path.stem
            if name in RESERVED_NAMES:
                warnings.append(f"ignoring reserved name {name!r} at {path}")
                continue
            try:
                qdef = _load_query_file(path)
                entry = {
                    "name": name,
                    "description": qdef.description,
                    "schema_version": qdef.schema_version,
                    "params": qdef.params,
                    "source": source,
                    "path": str(path),
                }
            except (ValidationError, ValueError, json.JSONDecodeError) as e:
                entry = {
                    "name": name,
                    "description": f"<error: {e}>",
                    "schema_version": None,
                    "params": [],
                    "source": source,
                    "path": str(path),
                }
            found[name] = entry

    _scan(BUILTIN_QUERIES_DIR, "builtin")
    _scan(USER_QUERIES_DIR, "user")

    return sorted(found.values(), key=lambda e: e["name"]), warnings


# =========================================================================
# Help callback (dynamic, shows query-specific metadata when available)
# =========================================================================


def _try_resolve_for_help(
    name: str | None, path: str | None
) -> tuple[QueryDef | None, str | None, str | None]:
    """Load a QueryDef for --help output. Never raises.

    Returns (qdef, display_name, error_note).
    """
    if path:
        p = Path(path)
        if not p.is_file():
            return None, None, f"query file not found: {path}"
        try:
            return _load_query_file(p), p.stem, None
        except (ValidationError, ValueError, json.JSONDecodeError) as e:
            return None, p.stem, f"failed to parse {path}: {e}"
    if name:
        if name in RESERVED_NAMES:
            return None, None, None
        for candidate in _candidate_paths(name):
            if candidate.is_file():
                try:
                    return _load_query_file(candidate), name, None
                except (
                    ValidationError,
                    ValueError,
                    json.JSONDecodeError,
                ) as e:
                    return (
                        None,
                        name,
                        f"failed to parse {candidate}: {e}",
                    )
        return None, name, f"no query named {name!r}"
    return None, None, None


def _print_query_help(
    ctx: click.Context,
    name: str | None,
    query_path: str | None,
) -> None:
    """Render ``--help`` for the ``run`` subcommand, with per-query
    metadata when a name/path is supplied."""
    click.echo(ctx.get_help())

    qdef, display, note = _try_resolve_for_help(name, query_path)

    if qdef is not None and display is not None:
        click.echo("")
        click.echo(f"Query: {display}")
        click.echo(f"Description: {qdef.description}")
        click.echo(f"Schema version: {qdef.schema_version}")
        if qdef.params:
            click.echo(f"Required params: {', '.join(qdef.params)}")
        else:
            click.echo("Required params: (none)")
    elif note:
        click.echo(f"\n(query help: {note})", err=True)


# =========================================================================
# Param handling
# =========================================================================


def _parse_query_params_json(raw: str | None) -> dict[str, Any]:
    if raw is None:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise click.UsageError(f"--query-params is not valid JSON: {e}")
    if not isinstance(parsed, dict):
        raise click.UsageError(
            "--query-params must be a JSON object (got "
            f"{type(parsed).__name__})"
        )
    return parsed


def _check_bind_params(
    qdef: QueryDef, supplied: dict[str, Any]
) -> dict[str, Any]:
    declared = set(qdef.params)
    given = set(supplied)
    missing = sorted(declared - given)
    extra = sorted(given - declared)
    if missing or extra:
        parts = []
        if missing:
            parts.append(f"missing: {', '.join(missing)}")
        if extra:
            parts.append(f"unexpected: {', '.join(extra)}")
        raise click.UsageError(
            "--query-params mismatch (" + "; ".join(parts) + ")"
        )
    return {k: supplied[k] for k in qdef.params}


# =========================================================================
# Click group + subcommands
# =========================================================================


class QueryGroup(click.Group):
    """Group where unrecognized tokens route to the ``run`` subcommand."""

    def resolve_command(
        self, ctx: click.Context, args: list[str]
    ) -> tuple[str | None, click.Command | None, list[str]]:
        if args and args[0] in self.commands:
            return super().resolve_command(ctx, args)
        # Implicit run dispatch: "pdd query foo" -> "pdd query run foo"
        return super().resolve_command(ctx, ["run", *args])


@cli.group(
    cls=QueryGroup,
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
    },
)
def query() -> None:
    """Run parameterized SQL queries against a scraper run DB.

    \b
    Examples:
        pdd query list
        pdd query <name> --db run.db --query-params '{"step": "parse"}'
        pdd query --query ./adhoc.json --db run.db
    """


@query.command("list")
@format_options
def query_list(format_type: str, template_name: str | None) -> None:
    """List available queries."""
    entries, warnings = _collect_queries()
    for w in warnings:
        click.echo(f"warning: {w}", err=True)

    output = {
        "items": entries,
        "count": len(entries),
    }
    render_output(
        output,
        format_type=format_type,
        template_path="query/_list",
        template_name=template_name or "default",
    )


@query.command(
    "run",
    add_help_option=False,
    context_settings={"help_option_names": []},
)
@click.argument("name", required=False)
@click.option(
    "--query",
    "query_path",
    type=click.Path(dir_okay=False),
    default=None,
    help="Run an ad-hoc query JSON file.",
)
@click.option(
    "--query-params",
    "query_params_json",
    default=None,
    help="JSON object mapping param names to values.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Bypass schema_version match check (with warning).",
)
@format_options
@db_option
@click.option(
    "--help",
    "show_help",
    is_flag=True,
    default=False,
    help="Show this message and, if a query is specified, its description.",
)
@click.pass_context
def query_run(
    ctx: click.Context,
    name: str | None,
    query_path: str | None,
    query_params_json: str | None,
    force: bool,
    format_type: str,
    template_name: str | None,
    db_path: str | None,
    show_help: bool,
) -> None:
    """Run a named or ad-hoc query."""
    if show_help:
        _print_query_help(ctx, name, query_path)
        ctx.exit()
    if query_path and not Path(query_path).is_file():
        raise click.UsageError(f"--query path does not exist: {query_path}")
    if name and query_path:
        raise click.UsageError(
            "Pass either a query name or --query <path>, not both."
        )
    if not name and not query_path:
        raise click.UsageError(
            "Missing query. Pass a name (see 'pdd query list') "
            "or --query <path>."
        )

    db_path = _resolve_db_path(ctx, db_path)

    # Load the query definition.
    if query_path:
        qdef_path = Path(query_path)
        try:
            qdef = _load_query_file(qdef_path)
        except (ValidationError, ValueError, json.JSONDecodeError) as e:
            raise click.ClickException(f"invalid query file {query_path}: {e}")
        display_name = qdef_path.stem
        is_builtin_template = False
    else:
        assert name is not None
        try:
            qdef, qdef_path = _resolve_named(name)
        except ValidationError as e:
            raise click.ClickException(f"invalid query {name!r}: {e}")
        display_name = name
        # Named queries can ship a bespoke template at query/<name>/<tpl>.
        is_builtin_template = True

    supplied = _parse_query_params_json(query_params_json)
    bind_params = _check_bind_params(qdef, supplied)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            engine = debugger.sql.engine
            async with engine.connect() as conn:
                db_ver = await _read_schema_version(conn)
                if db_ver != qdef.schema_version:
                    msg = (
                        f"Query {display_name!r} expects schema "
                        f"version {qdef.schema_version}, but DB is at "
                        f"{db_ver}."
                    )
                    if not force:
                        raise click.ClickException(
                            msg + " Use --force to override."
                        )
                    click.echo(f"warning: {msg} (--force)", err=True)

                result = await conn.execute(sa.text(qdef.query), bind_params)
                # For SELECTs this works; for non-SELECT it may not have
                # keys(), so guard.
                try:
                    columns = list(result.keys())
                except sa.exc.ResourceClosedError:
                    columns = []
                try:
                    rows = [dict(r._mapping) for r in result]
                except sa.exc.ResourceClosedError:
                    rows = []

        output = {
            "name": display_name,
            "description": qdef.description,
            "schema_version": qdef.schema_version,
            "params": bind_params,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        }

        if is_builtin_template and name is not None:
            # Try per-query template first, fall back to generic.
            render_output(
                output,
                format_type=format_type,
                template_path=f"query/{name}",
                template_name=template_name or "default",
                fallback_template_path="query/_generic",
            )
        else:
            render_output(
                output,
                format_type=format_type,
                template_path="query/_generic",
                template_name=template_name or "default",
            )

    asyncio.run(run())
