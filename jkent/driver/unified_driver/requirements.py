"""Requirement-classification policy: which ``DriverRequirement`` sets demand
a browser transport or the camoufox engine.

Pure data derived from :class:`~jkent.data_types.DriverRequirement` — a leaf
module with no driver dependencies, so both the run bootstrapper (transport
and browser-profile selection) and the Playwright transport (engine
selection) can import it without a cycle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from jkent.data_types import DriverRequirement

if TYPE_CHECKING:
    from jkent.data_types import BaseScraper

#: Requirements that demand a live browser (mirrors the CLI's
#: ``needs_playwright`` set).
BROWSER_REQUIREMENTS = frozenset(
    {
        DriverRequirement.JS_EVAL,
        DriverRequirement.FF_ALIKE,
        DriverRequirement.CHROME_ALIKE,
        DriverRequirement.HCAP_HANDLER,
        DriverRequirement.RCAP_HANDLER,
        DriverRequirement.CFCAP_HANDLER,
        DriverRequirement.STRICTLY_SERIAL,
    }
)

#: Requirements that demand the camoufox engine. Camoufox is the stealthy
#: Firefox build that reliably passes Cloudflare, hCaptcha, *and* reCAPTCHA
#: challenges; ``CFCAP_HANDLER``, ``HCAP_HANDLER``, and ``RCAP_HANDLER``
#: scrapers all run on it (transport selection, engine build, and
#: browser-profile resolution all key off this).
CAMOUFOX_REQUIREMENTS = frozenset(
    {
        DriverRequirement.CFCAP_HANDLER,
        DriverRequirement.HCAP_HANDLER,
        DriverRequirement.RCAP_HANDLER,
    }
)


def needs_browser(scraper: BaseScraper[Any]) -> bool:
    """Whether the scraper's requirements demand a browser transport."""
    reqs = getattr(scraper, "driver_requirements", [])
    return any(r in BROWSER_REQUIREMENTS for r in reqs)
