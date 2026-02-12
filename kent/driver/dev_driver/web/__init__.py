"""Web interface for LocalDevDriver.

This package provides a FastAPI-based web interface for managing
scraper runs, viewing progress, and interacting with the driver.
"""

from kent.driver.dev_driver.web.app import (
    RunInfo,
    RunManager,
    app,
    create_app,
    get_run_manager,
    lifespan,
)

__all__ = [
    "app",
    "create_app",
    "get_run_manager",
    "lifespan",
    "RunInfo",
    "RunManager",
]
