"""Playwright-based driver for JavaScript-heavy court websites.

This module provides a Playwright driver that extends LocalDevDriver to handle
JavaScript-heavy websites using browser automation while maintaining step function
purity through DOM snapshotting.
"""

from kent.driver.playwright_driver.browser_profile import (
    BrowserProfile,
    load_browser_profile,
)
from kent.driver.playwright_driver.playwright_driver import (
    PlaywrightDriver,
)

__all__ = ["BrowserProfile", "PlaywrightDriver", "load_browser_profile"]
