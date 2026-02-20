"""SQLManager - Database operations for LocalDevDriver.

This package provides a standalone class for all SQLite database operations,
enabling independent testing and programmatic inspection of the database
without requiring a full driver instance.

The SQLManager handles:
- Request queue operations (enqueue, dequeue, status updates)
- Response storage with compression
- Result storage with validation tracking
- Error requeue operations
- Run metadata management
- Speculative progress tracking
- Statistics and listing operations
"""

from kent.driver.persistent_driver.sql_manager._base import SQLManagerBase
from kent.driver.persistent_driver.sql_manager._errors import ErrorRequeueMixin
from kent.driver.persistent_driver.sql_manager._estimates import (
    EstimateStorageMixin,
)
from kent.driver.persistent_driver.sql_manager._listing import ListingMixin
from kent.driver.persistent_driver.sql_manager._rate_limiter import (
    RateLimiterMixin,
)
from kent.driver.persistent_driver.sql_manager._requests import (
    RequestQueueMixin,
)
from kent.driver.persistent_driver.sql_manager._requeue import RequeueMixin
from kent.driver.persistent_driver.sql_manager._responses import (
    ResponseStorageMixin,
)
from kent.driver.persistent_driver.sql_manager._results import (
    ResultStorageMixin,
)
from kent.driver.persistent_driver.sql_manager._run_metadata import (
    RunMetadataMixin,
)
from kent.driver.persistent_driver.sql_manager._speculation import (
    SpeculationMixin,
)
from kent.driver.persistent_driver.sql_manager._types import (
    Page,
    RequestRecord,
    RequeueResult,
    ResponseRecord,
    ResultRecord,
    compute_cache_key,
)
from kent.driver.persistent_driver.sql_manager._validation import (
    ValidationMixin,
)


class SQLManager(
    RunMetadataMixin,
    RequestQueueMixin,
    ResponseStorageMixin,
    ResultStorageMixin,
    EstimateStorageMixin,
    ErrorRequeueMixin,
    SpeculationMixin,
    RateLimiterMixin,
    ValidationMixin,
    ListingMixin,
    RequeueMixin,
    SQLManagerBase,
):
    """Database manager for LocalDevDriver operations.

    Provides all database operations needed by the LocalDevDriver in a
    standalone class that can be used independently for testing, inspection,
    and programmatic access to the SQLite database.

    Example::

        # Standalone usage for inspection
        async with SQLManager.open(db_path) as manager:
            stats = await manager.get_stats()
            requests = await manager.list_requests(status="pending")

        # With existing engine/session factory (for driver integration)
        manager = SQLManager(engine, session_factory)
        await manager.store_response(request_id, response, continuation)
    """

    pass


__all__ = [
    "Page",
    "RequeueResult",
    "RequestRecord",
    "ResponseRecord",
    "ResultRecord",
    "SQLManager",
    "compute_cache_key",
]
