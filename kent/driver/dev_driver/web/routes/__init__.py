"""Route modules for LocalDevDriver web interface.

This package contains all API route modules:
- scrapers: Scraper discovery and parameter schema endpoints
- runs: Run management endpoints
- requests: Request listing and cancellation endpoints
- responses: Response viewing and content retrieval
- results: Result listing and data access
- errors: Error tracking and requeue endpoints
- compression: Dictionary training and recompression
- export: WARC export endpoints
- debug: Response diagnosis with XPath observation
- archived_files: Archived file metadata and content
- rate_limiter: Rate limiter state monitoring
- websocket: Real-time progress events via WebSocket
"""

from kent.driver.dev_driver.web.routes.archived_files import (
    router as archived_files_router,
)
from kent.driver.dev_driver.web.routes.compression import (
    router as compression_router,
)
from kent.driver.dev_driver.web.routes.debug import (
    router as debug_router,
)
from kent.driver.dev_driver.web.routes.errors import (
    router as errors_router,
)
from kent.driver.dev_driver.web.routes.export import (
    router as export_router,
)
from kent.driver.dev_driver.web.routes.rate_limiter import (
    router as rate_limiter_router,
)
from kent.driver.dev_driver.web.routes.requests import (
    router as requests_router,
)
from kent.driver.dev_driver.web.routes.responses import (
    router as responses_router,
)
from kent.driver.dev_driver.web.routes.results import (
    router as results_router,
)
from kent.driver.dev_driver.web.routes.runs import (
    router as runs_router,
)
from kent.driver.dev_driver.web.routes.scrapers import (
    router as scrapers_router,
)
from kent.driver.dev_driver.web.routes.views import (
    router as views_router,
)
from kent.driver.dev_driver.web.websocket import (
    router as websocket_router,
)

__all__ = [
    "archived_files_router",
    "compression_router",
    "debug_router",
    "errors_router",
    "export_router",
    "rate_limiter_router",
    "requests_router",
    "responses_router",
    "results_router",
    "runs_router",
    "scrapers_router",
    "views_router",
    "websocket_router",
]
