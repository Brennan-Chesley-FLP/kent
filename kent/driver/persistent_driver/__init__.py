"""LocalDevDriver package for SQLite-backed local scraper development.

This package provides a development driver that extends AsyncDriver with:
- Persistent request queue in SQLite
- Response archival with zstd compression
- Resumability from graceful shutdown
- WARC export capability
- Web interface integration via callbacks
"""

from kent.driver.persistent_driver.comparison import (
    ComparisonResult,
    ComparisonSummary,
    DataDiff,
    ErrorDiff,
    RequestChange,
    RequestDiff,
    compare_continuation_output,
)
from kent.driver.persistent_driver.compression import (
    DEFAULT_COMPRESSION_LEVEL,
    DEFAULT_DICT_SIZE,
    compress,
    compress_response,
    decompress,
    decompress_response,
    get_compression_dict,
    get_dict_by_id,
    recompress_responses,
    train_compression_dict,
)
from kent.driver.persistent_driver.database import (
    SCHEMA_VERSION,
    get_next_queue_counter,
    get_schema_version,
    init_database,
)
from kent.driver.persistent_driver.dry_run_driver import (
    CapturedData,
    CapturedError,
    CapturedRequest,
    DryRunDriver,
    DryRunResult,
)
from kent.driver.persistent_driver.errors import (
    ErrorRecord,
    classify_error,
    count_errors,
    get_error,
    list_errors,
    resolve_error,
    store_error,
)
from kent.driver.persistent_driver.persistent_driver import (
    Page,
    PersistentDriver,
    ProgressEvent,
    RequestRecord,
    ResponseRecord,
    ResultRecord,
)
from kent.driver.persistent_driver.rate_limiter import (
    AioSQLiteBucket,
)
from kent.driver.persistent_driver.stats import (
    CompressionStats,
    DevDriverStats,
    ErrorStats,
    QueueStats,
    ResultStats,
    ThroughputStats,
    get_compression_stats,
    get_error_stats,
    get_queue_stats,
    get_result_stats,
    get_stats,
    get_throughput_stats,
)
from kent.driver.persistent_driver.warc_export import (
    export_warc,
    export_warc_for_continuation,
)

__all__ = [
    # Main driver
    "PersistentDriver",
    "Page",
    "ProgressEvent",
    "RequestRecord",
    "ResponseRecord",
    "ResultRecord",
    # Compression
    "DEFAULT_COMPRESSION_LEVEL",
    "DEFAULT_DICT_SIZE",
    "compress",
    "compress_response",
    "decompress",
    "decompress_response",
    "get_compression_dict",
    "get_dict_by_id",
    "recompress_responses",
    "train_compression_dict",
    # Errors
    "ErrorRecord",
    "classify_error",
    "count_errors",
    "get_error",
    "list_errors",
    "resolve_error",
    "store_error",
    # Rate limiting
    "AioSQLiteBucket",
    # Schema
    "SCHEMA_VERSION",
    "get_next_queue_counter",
    "get_schema_version",
    "init_database",
    # Stats
    "CompressionStats",
    "DevDriverStats",
    "ErrorStats",
    "QueueStats",
    "ResultStats",
    "ThroughputStats",
    "get_compression_stats",
    "get_error_stats",
    "get_queue_stats",
    "get_result_stats",
    "get_stats",
    "get_throughput_stats",
    # WARC export
    "export_warc",
    "export_warc_for_continuation",
    # Dry run driver
    "CapturedData",
    "CapturedError",
    "CapturedRequest",
    "DryRunDriver",
    "DryRunResult",
    # Comparison
    "ComparisonResult",
    "ComparisonSummary",
    "DataDiff",
    "ErrorDiff",
    "RequestChange",
    "RequestDiff",
    "compare_continuation_output",
]
