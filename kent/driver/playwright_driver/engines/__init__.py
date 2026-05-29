"""Browser engines for the Playwright driver.

Each engine encapsulates the engine-specific launch + lifecycle
behaviour behind a ``BrowserEngine`` interface.  The driver receives
a ``BrowserContext`` from the engine's ``acquire()`` and is otherwise
engine-agnostic.
"""

from kent.driver.playwright_driver.engines.base import BrowserEngine
from kent.driver.playwright_driver.engines.camoufox import CamoufoxEngine
from kent.driver.playwright_driver.engines.playwright import PlaywrightEngine

__all__ = ["BrowserEngine", "CamoufoxEngine", "PlaywrightEngine"]
