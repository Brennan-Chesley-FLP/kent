"""Interstitial page handlers for the Playwright driver.

Interstitial handlers run on the live Playwright page after navigation
but before the DOM snapshot is taken.  The driver races each handler's
waitlist against the scraper step's own await_list; if a handler's
conditions match first, it gets to interact with the page (e.g. solve a
captcha) before the scraper ever sees the HTML.
"""

from __future__ import annotations

import abc
import logging
from typing import TYPE_CHECKING

from kent.data_types import (
    DriverRequirement,
    WaitForLoadState,
    WaitForSelector,
    WaitForTimeout,
    WaitForURL,
)

if TYPE_CHECKING:
    from playwright.async_api import Page

logger = logging.getLogger(__name__)

WaitCondition = WaitForSelector | WaitForLoadState | WaitForURL | WaitForTimeout


class InterstitialHandler(abc.ABC):
    """Handles interstitial pages (captchas, disclaimers, etc.) on the live
    Playwright page, after navigation but before DOM snapshot."""

    @abc.abstractmethod
    def waitlist(self) -> list[WaitCondition]:
        """Conditions that indicate this interstitial is present.

        All conditions must match (conjunction) for the handler to fire.
        """

    @abc.abstractmethod
    async def navigate_through(self, page: Page) -> None:
        """Interact with the live page to get past the interstitial.

        When this returns, the page should be showing the real content
        (or another interstitial that a subsequent handler can deal with).
        """


class HCaptchaHandler(InterstitialHandler):
    """Handles hCaptcha interstitial pages.

    Clicks the ``div.h-captcha`` element, which triggers the hCaptcha
    widget.  In headless Firefox with ``navigator.webdriver`` overridden,
    this auto-solves; the JS callback then submits the form, navigating
    to the real content page.
    """

    def waitlist(self) -> list[WaitCondition]:
        return [WaitForSelector("div.h-captcha")]

    async def navigate_through(self, page: Page) -> None:
        logger.info("hCaptcha interstitial detected — clicking to solve")
        captcha = page.locator("div.h-captcha")
        await captcha.click()
        await page.wait_for_load_state("networkidle")


INTERSTITIAL_HANDLERS: dict[DriverRequirement, InterstitialHandler] = {
    DriverRequirement.HCAP_HANDLER: HCaptchaHandler(),
}
