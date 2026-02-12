Step 16: Request Deduplication
================================

In the previous steps, the driver processed all yielded requests without checking
for duplicates. This can lead to inefficiency when scraping sites with cyclic links
or when the same URL is discovered through multiple paths.

In Step 16, we introduce **request deduplication** to prevent fetching the same
resource multiple times.


Overview
--------

In this step, we introduce:

1. **deduplication_key field** - Added to BaseRequest with automatic default generation
2. **Automatic key generation** - SHA256 hash of URL, params, and data
3. **Custom deduplication keys** - Scrapers can override the default
4. **duplicate_check callback** - Optional callback to control deduplication logic


Why Deduplication?
------------------

**Avoiding Redundant Requests**

Many websites have cyclic link structures:
- List pages link to detail pages
- Detail pages link back to list pages
- Search results paginate through overlapping content
- Category pages cross-reference each other

Without deduplication, the scraper could:
- Fetch the same URL hundreds of times
- Waste bandwidth and time
- Get rate-limited or blocked for excessive requests
- Process duplicate data

**Memory Efficiency**

Combined with Step 15 (Priority Queue), deduplication:
- Reduces queue size (no duplicate requests)
- Prevents redundant HTTP fetches
- Lowers peak memory usage


Deduplication Key
-----------------

The ``deduplication_key`` field is added to ``BaseRequest``:

.. code-block:: python

    @dataclass(frozen=True)
    class BaseRequest:
        request: HTTPRequestParams
        continuation: str
        current_location: str = ""
        previous_requests: list[BaseRequest] = field(default_factory=list)
        accumulated_data: dict[str, Any] = field(default_factory=dict)
        aux_data: dict[str, Any] = field(default_factory=dict)
        priority: int = 9
        deduplication_key: str | None = None  # Step 16

**Default Generation:**

If ``deduplication_key`` is ``None``, the ``__post_init__`` method generates
a SHA256 hash of:

1. **URL** - The full URL
2. **Query parameters** - Sorted by key for consistency
3. **POST data** - Sorted dict items or list tuples
4. **JSON data** - JSON-serialized with sorted keys

This ensures:
- Same URL + same data = same key
- Same data in different order = same key
- Different URL or data = different key

In the event that we need to skip deduplication checks, the key may be set to ``SkipDeduplicationCheck()``.
This can be important if the request is changing every time, or we're retrying something with aux_data.


Duplicate Check Callback
-------------------------

The ``duplicate_check`` callback allows custom deduplication logic:

.. code-block:: python

    driver = SyncDriver(
        scraper=scraper,
        storage_dir=tmp_path,
        duplicate_check=my_duplicate_checker,
    )

**Callback Signature:**

.. code-block:: python

    def duplicate_check(deduplication_key: str) -> bool:
        """Check if a request should be enqueued.

        Args:
            deduplication_key: The deduplication key for the request.

        Returns:
            True to enqueue the request, False to skip it.
        """
        ...


Next Steps
----------

In :doc:`15_permanent_data`, we introduce permanent request data - headers
and cookies that persist across the entire request chain. This simplifies
authentication workflows where session cookies or auth tokens must flow
through all requests.
