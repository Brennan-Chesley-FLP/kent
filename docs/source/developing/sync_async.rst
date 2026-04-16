========================
Sync and Async Drivers
========================

The sync and async drivers are the simplest driver implementations. They
exist primarily as **templates and for testing purposes** -- they implement the
full driver contract in a minimal way, making them useful as reference
implementations and for running scrapers in test suites where SQLite
persistence is unnecessary.

The async driver is also the base class that the persistent driver extends.


SyncDriver
==========

Location: ``kent/driver/sync_driver.py``

The synchronous driver processes the request queue in a single thread. Its
``run()`` method:

1. Calls ``on_run_start``
2. Seeds non-speculative entries via ``initial_seed()``
3. Discovers speculative entries and seeds the speculative queue
4. Enters main loop:

   a. Pops lowest-priority request from the heap
   b. Checks deduplication (skip if key already seen)
   c. Makes the HTTP request via ``SyncRequestManager``
   d. For speculative requests, calls ``fails_successfully()`` and updates
      speculation state
   e. Routes the response to the continuation step
   f. Pattern-matches yields and enqueues/collects/records
   g. For ``ParsedData``, validates via ``DeferredValidation.confirm()``

5. Calls ``on_run_complete``

The queue is a ``heapq`` min-heap with entries of ``(priority, counter, request)``
where ``counter`` ensures FIFO ordering within the same priority level.

Callbacks
---------

The sync and async drivers expose callbacks for all driver events, giving
callers fine-grained control:

- ``on_data(data)`` -- called with validated, unwrapped data
- ``on_invalid_data(deferred)`` -- called when validation fails
- ``on_structural_error(exception)`` -- called on HTML structure mismatch; return True to continue
- ``on_transient_exception(exception)`` -- called on retryable errors; return True to continue
- ``on_run_start(scraper_name)`` -- lifecycle hook
- ``on_run_complete(scraper_name, status, error)`` -- lifecycle hook
- ``duplicate_check(key)`` -- return True to enqueue, False to skip

SpeculationState
----------------

The ``SpeculationState`` dataclass (defined in ``sync_driver.py``) tracks
per-template speculation state. Both the sync and async drivers use this
directly. The persistent driver imports and extends it.

.. code-block:: python

    @dataclass
    class SpeculationState:
        func_name: str           # state key: "{entry_name}:{param_index}"
        template: Speculative    # protocol instance for from_int() calls
        param_index: int         # position in params list
        base_func_name: str      # actual method name on scraper
        highest_successful_id: int = 0
        consecutive_failures: int = 0
        current_ceiling: int = 0
        stopped: bool = False


AsyncDriver
===========

Location: ``kent/driver/async_driver.py``

Mirrors the SyncDriver API but uses ``async``/``await`` and
``AsyncRequestManager``. The main loop is an async coroutine. The persistent
driver extends this class, inheriting its request processing logic and
adding SQLite persistence on top.


Usage in Tests
==============

Both drivers are commonly used in tests with the ``collect_results`` helper:

.. code-block:: python

    from tests.utils import collect_results

    callback, results = collect_results()
    driver = SyncDriver(scraper, on_data=callback)
    driver.run()
    assert len(results) == expected_count
