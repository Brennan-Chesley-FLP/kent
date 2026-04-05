"""Jinja2-based output templating for the PDD CLI.

Every command produces a JSON-serializable data dict, then delegates
to ``render_output()`` which dispatches to json, jsonl, or a Jinja2
template for human-readable output.

Templates are resolved via a two-level search path:

1. User-local: ``~/.config/kent/templates/<command>/<name>.jinja2``
2. Built-in:   ``cli/templates/<command>/<name>.jinja2``

The user-local path is checked first, so users can override any
built-in template by placing a file at the same relative path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
import jinja2

# Built-in templates live alongside this module
BUILTIN_TEMPLATES_DIR = Path(__file__).parent / "templates"

# User-local overrides
USER_TEMPLATES_DIR = Path.home() / ".config" / "kent" / "templates"

# Aliases for backwards compatibility with old --format values
_FORMAT_ALIASES = {"summary": "default", "table": "default"}


def _create_jinja_env() -> jinja2.Environment:
    loaders: list[jinja2.BaseLoader] = []

    # User-local templates take priority (directory may not exist)
    if USER_TEMPLATES_DIR.is_dir():
        loaders.append(jinja2.FileSystemLoader(str(USER_TEMPLATES_DIR)))

    # Built-in templates
    loaders.append(jinja2.FileSystemLoader(str(BUILTIN_TEMPLATES_DIR)))

    env = jinja2.Environment(
        loader=jinja2.ChoiceLoader(loaders),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=False,
        undefined=jinja2.StrictUndefined,
    )

    # Custom filters
    env.filters["checkmark"] = _filter_checkmark
    env.filters["ljust"] = _filter_ljust
    env.filters["truncate_str"] = _filter_truncate_str
    env.filters["format_bytes"] = _filter_format_bytes

    # Globals
    env.globals["json_dumps"] = json.dumps

    return env


# Lazy singleton
_env: jinja2.Environment | None = None


def _get_env() -> jinja2.Environment:
    global _env
    if _env is None:
        _env = _create_jinja_env()
    return _env


# ---------------------------------------------------------------------------
# Custom Jinja2 filters
# ---------------------------------------------------------------------------


def _filter_checkmark(value: Any) -> str:
    """Render a boolean as ✓ or ✗."""
    return "\u2713" if value else "\u2717"


def _filter_ljust(value: Any, width: int = 15) -> str:
    """Left-justify a value within *width* characters."""
    return str(value).ljust(width)


def _filter_truncate_str(value: Any, max_len: int = 50) -> str:
    """Truncate a string to *max_len* characters."""
    s = str(value) if value is not None else ""
    return s[:max_len] if len(s) > max_len else s


def _filter_format_bytes(value: Any) -> str:
    """Format a byte count with thousands separators."""
    if value is None:
        return "N/A"
    return f"{int(value):,} bytes"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def render_output(
    data: Any,
    *,
    format_type: str = "default",
    template_path: str,
    template_name: str = "default",
) -> None:
    """Render command output.

    Args:
        data: JSON-serializable data (dict or list).
        format_type: ``"json"``, ``"jsonl"``, or any other value
            (treated as a template format — ``"default"`` uses
            ``default.jinja2``).
        template_path: Relative path under the templates directory,
            e.g. ``"requests/list"``.
        template_name: Template file stem.  Defaults to ``"default"``.
            Overridden by ``--template`` on the CLI.
    """
    # Resolve old format names
    effective = _FORMAT_ALIASES.get(format_type, format_type)

    if effective == "json":
        click.echo(json.dumps(data, indent=2, default=str))
        return

    if effective == "jsonl":
        items = data
        if isinstance(data, dict) and "items" in data:
            items = data["items"]
        if isinstance(items, list):
            for item in items:
                click.echo(json.dumps(item, default=str))
        else:
            click.echo(json.dumps(items, default=str))
        return

    # Template rendering
    env = _get_env()
    tpl_file = f"{template_path}/{template_name}.jinja2"
    try:
        template = env.get_template(tpl_file)
    except jinja2.TemplateNotFound:
        click.echo(f"[template not found: {tpl_file}]", err=True)
        click.echo(json.dumps(data, indent=2, default=str))
        return

    rendered = template.render(data=data)
    click.echo(rendered)
