"""Opt-in leniency patches for h11's strict header validation.

The patch is gated on a :class:`ContextVar` so it only loosens behavior for
the scraper that asked for it. Other scrapers running concurrently in the
same process see vanilla h11.

Pinned to a specific h11 version in ``pyproject.toml`` because the patch
depends on h11 internals (``h11._headers.normalize_and_validate``).
"""

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

import h11._events as _h11_events
import h11._headers as _h11_headers

if TYPE_CHECKING:
    from kent.data_types import BaseScraper

_lenient_te: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "kent_h11_lenient_te", default=False
)

_orig_normalize_and_validate = _h11_headers.normalize_and_validate


def _dedupe_transfer_encoding(
    headers: Iterable[tuple[Any, Any]],
) -> list[tuple[Any, Any]]:
    seen = False
    out: list[tuple[Any, Any]] = []
    for name, value in headers:
        key = (
            name.lower()
            if isinstance(name, bytes)
            else name.lower().encode("ascii")
        )
        if key == b"transfer-encoding":
            if seen:
                continue
            seen = True
        out.append((name, value))
    return out


def _patched_normalize_and_validate(
    headers: Any, _parsed: bool = False
) -> Any:
    # Only loosen for parsed (response) headers. Outbound requests stay
    # strict so we don't mask request-smuggling shapes we generate ourselves.
    if _parsed and _lenient_te.get():
        headers = _dedupe_transfer_encoding(headers)
    return _orig_normalize_and_validate(headers, _parsed=_parsed)


def install() -> None:
    # h11._events imports normalize_and_validate by name at module load time
    # (`from ._headers import normalize_and_validate`), so the response-parsing
    # path resolves the symbol via _events' module globals and never touches
    # _headers.normalize_and_validate. Patch both bindings.
    if _h11_headers.normalize_and_validate is _patched_normalize_and_validate:
        return
    _h11_headers.normalize_and_validate = _patched_normalize_and_validate
    _h11_events.normalize_and_validate = _patched_normalize_and_validate  # type: ignore[attr-defined]


@contextlib.contextmanager
def lenient_te() -> Iterator[None]:
    token = _lenient_te.set(True)
    try:
        yield
    finally:
        _lenient_te.reset(token)


def lenient_te_for(
    scraper: type[BaseScraper[Any]] | BaseScraper[Any],
) -> contextlib.AbstractContextManager[None]:
    """Context manager that enables lenient TE iff the scraper opts in.

    Hoist this around the driver's ``run()`` body so child tasks (workers,
    monitors) inherit the contextvar via :pep:`asyncio.Task` snapshotting.
    """
    from kent.data_types import DriverRequirement

    enabled = DriverRequirement.H11_HEADER_FIXES in getattr(
        scraper, "driver_requirements", []
    )
    return lenient_te() if enabled else contextlib.nullcontext()


install()
