"""Playwright-based driver for JavaScript-heavy court websites.

This module provides a Playwright driver that extends LocalDevDriver to handle
JavaScript-heavy websites using browser automation while maintaining step function
purity through DOM snapshotting.
"""

from kent.driver.playwright_driver.playwright_driver import (
    PlaywrightDriver,
)

__all__ = ["PlaywrightDriver"]
