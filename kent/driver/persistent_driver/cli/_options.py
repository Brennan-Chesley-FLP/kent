"""Shared Click option decorators for the pdd CLI.

Convention: stack shared decorators *after* command-specific ``@click.option``
calls so shared options appear first in ``--help`` (Click shows options in
reverse decorator-application order).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

import click

F = TypeVar("F", bound=Callable[..., Any])

FORMAT_CHOICES = ("default", "json", "jsonl")


db_option = click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)


_format_option = click.option(
    "--format",
    "format_type",
    type=click.Choice(FORMAT_CHOICES),
    default="default",
    help="Output format",
)


_template_option = click.option(
    "--template",
    "template_name",
    default=None,
    help="Template name",
)


_limit_option = click.option(
    "--limit",
    type=int,
    default=100,
    help="Maximum number of results",
)


_offset_option = click.option(
    "--offset",
    type=int,
    default=0,
    help="Number of results to skip",
)


def format_options(f: F) -> F:
    """Adds ``--format`` and ``--template``."""
    return _format_option(_template_option(f))


def pagination_options(f: F) -> F:
    """Adds ``--limit`` and ``--offset``."""
    return _limit_option(_offset_option(f))


def search_options(f: F) -> F:
    """Adds ``--text``, ``--regex``, ``--xpath``."""
    f = click.option(
        "--xpath", "xpath_expr", help="XPath expression to evaluate"
    )(f)
    f = click.option(
        "--regex", "regex_pattern", help="Regular expression pattern"
    )(f)
    f = click.option(
        "--text", "text_pattern", help="Plain text to search for"
    )(f)
    return f
