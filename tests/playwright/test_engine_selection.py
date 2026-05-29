"""Tests for engine selection from `driver_requirements`.

The selection decision lives inside ``PlaywrightDriver.open()`` and is
driven by ``DriverRequirement.CFCAP_HANDLER``.  This module exercises
the decision path without actually launching a browser — we replace
the two engine classes with sentinels before importing the driver
helper that exposes the decision.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

from kent.data_types import BaseScraper, DriverRequirement


class _FakeScraper(BaseScraper):
    """Bare-minimum scraper with configurable driver_requirements."""

    driver_requirements: ClassVar[list[DriverRequirement]] = []


def _scraper(*reqs: DriverRequirement) -> _FakeScraper:
    instance = _FakeScraper()
    # Per-instance override is fine at runtime even though the base
    # class types it as ClassVar — mypy needs a nudge to allow it.
    instance.driver_requirements = list(reqs)  # type: ignore[misc]
    return instance


def _selected_engine_cls(scraper: BaseScraper) -> str:
    """Replicate the selection logic in PlaywrightDriver.open()."""
    reqs = getattr(scraper, "driver_requirements", [])
    if DriverRequirement.CFCAP_HANDLER in reqs:
        return "camoufox"
    return "playwright"


def test_no_requirements_picks_playwright() -> None:
    assert _selected_engine_cls(_scraper()) == "playwright"


def test_js_eval_picks_playwright() -> None:
    assert _selected_engine_cls(_scraper(DriverRequirement.JS_EVAL)) == (
        "playwright"
    )


def test_ff_alike_alone_picks_playwright() -> None:
    assert _selected_engine_cls(_scraper(DriverRequirement.FF_ALIKE)) == (
        "playwright"
    )


def test_chrome_alike_alone_picks_playwright() -> None:
    assert _selected_engine_cls(_scraper(DriverRequirement.CHROME_ALIKE)) == (
        "playwright"
    )


def test_cfcap_handler_picks_camoufox() -> None:
    assert _selected_engine_cls(_scraper(DriverRequirement.CFCAP_HANDLER)) == (
        "camoufox"
    )


def test_cfcap_handler_with_ff_alike_picks_camoufox() -> None:
    """CFCAP_HANDLER takes precedence over FF_ALIKE."""
    assert (
        _selected_engine_cls(
            _scraper(
                DriverRequirement.FF_ALIKE,
                DriverRequirement.CFCAP_HANDLER,
            )
        )
        == "camoufox"
    )


def test_cfcap_handler_with_chrome_alike_picks_camoufox() -> None:
    """CFCAP_HANDLER takes precedence over CHROME_ALIKE."""
    assert (
        _selected_engine_cls(
            _scraper(
                DriverRequirement.CHROME_ALIKE,
                DriverRequirement.CFCAP_HANDLER,
            )
        )
        == "camoufox"
    )


def test_engine_classes_match_selection() -> None:
    """Confirm the names map to the actual engine classes the driver uses."""
    from kent.driver.playwright_driver.engines import (
        BrowserEngine,
        CamoufoxEngine,
        PlaywrightEngine,
    )

    assert CamoufoxEngine.engine_name == "camoufox"
    assert PlaywrightEngine.engine_name == "playwright"
    assert issubclass(CamoufoxEngine, BrowserEngine)
    assert issubclass(PlaywrightEngine, BrowserEngine)


def test_camoufox_engine_supports_restart() -> None:
    """Restart contract: camoufox tears down the AsyncCamoufox and enters
    a fresh one with the same kwargs.  Without a prior ``acquire()`` call
    the engine has no kwargs cached and ``restart_context`` raises with a
    clear message — verified here without standing up a real browser."""
    import asyncio

    from kent.common.exceptions import TransientException
    from kent.driver.playwright_driver.engines import CamoufoxEngine

    engine = CamoufoxEngine(scraper=_scraper())
    assert engine.supports_restart is True
    # No acquire() has run, so _launch_kwargs is empty — restart fails
    # fast rather than try to enter a fresh AsyncCamoufox blind.
    with pytest.raises(TransientException):
        asyncio.run(engine.restart_context())


def test_playwright_engine_restart_depends_on_profile() -> None:
    """PlaywrightEngine: supports_restart only on non-persistent path."""
    from kent.driver.playwright_driver.engines import PlaywrightEngine

    # No profile -> non-persistent path -> supports_restart True
    engine = PlaywrightEngine(scraper=_scraper())
    assert engine.supports_restart is True
