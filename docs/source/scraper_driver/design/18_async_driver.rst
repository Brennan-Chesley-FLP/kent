Step 21: AsyncDriver
====================

The separation of effectful parts into the driver means that we can make an
AsyncDriver without making any changes to our parsers. We can use Sync when we
want determinism or ease of debugging, and async when we want to run efficiently.

Let's make the AsyncDriver

Overview
--------

In this step, we introduce:

1. **AsyncDriver class** - Mirrors SyncDriver with async/await semantics
2. **Concurrent workers** - Multiple coroutines processing requests in parallel
3. **asyncio.PriorityQueue** - Async-compatible priority queue
4. **asyncio.Event stop_event** - Graceful shutdown for async contexts
5. **Shared httpx.AsyncClient** - Connection pooling across workers


Design Principles
-----------------

The AsyncDriver closely mirrors the SyncDriver with three key differences:

1. **Worker method** - The main loop is factored into ``_worker()`` coroutines
2. **Async priority queue** - Uses ``asyncio.PriorityQueue`` instead of ``heapq``
3. **num_workers parameter** - Controls concurrency level

This design ensures:

- **API compatibility** - Same callbacks and scraper interface (modulo async)
- **Behavioral equivalence** - Same priority ordering and deduplication


Implementation
--------------

**Initialization**

.. code-block:: python

    class AsyncDriver(Generic[ScraperReturnDatatype]):
        def __init__(
            self,
            scraper: BaseScraper[ScraperReturnDatatype],
            storage_dir: Path | None = None,
            on_data: Callable[[ScraperReturnDatatype], None] | None = None,
            on_structural_error: Callable[[HTMLStructuralAssumptionException], bool] | None = None,
            on_invalid_data: Callable[[DeferredValidation], None] | None = None,
            on_transient_exception: Callable[[TransientException], bool] | None = None,
            on_archive: Callable[[bytes, str, str | None, Path], str] | None = None,
            on_run_start: Callable[[str], None] | None = None,
            on_run_complete: Callable[[str, str, int, int, Exception | None], None] | None = None,
            duplicate_check: Callable[[str], bool] | None = None,
            stop_event: asyncio.Event | None = None,  # asyncio.Event, not threading.Event
            num_workers: int = 1,  # Number of concurrent workers
        ) -> None:
            # ... same setup as SyncDriver ...

            # Async-specific: PriorityQueue and locks
            self.request_queue: asyncio.PriorityQueue[tuple[int, int, Request]]
            self._queue_lock = asyncio.Lock()

            # Shared async client for connection pooling
            self._client = httpx.AsyncClient()

**Run Method**

The ``run()`` method spawns worker tasks and waits for completion:

.. code-block:: python

    async def run(self) -> None:

        # Fire on_run_start callback
        if self.on_run_start:
            self.on_run_start(self.scraper.__class__.__name__)

        try:
            # Check for early stop
            if self.stop_event and self.stop_event.is_set():
                return

            # Initialize queue with entry request
            entry_request = self.scraper.get_entry()
            self.request_queue = asyncio.PriorityQueue()
            await self.request_queue.put(
                (entry_request.priority, self._queue_counter, entry_request)
            )

            # Start workers
            workers = [
                asyncio.create_task(self._worker(i))
                for i in range(self.num_workers)
            ]

            # Wait for queue to drain (with stop_event checks)
            await self.request_queue.join()

            # Cancel workers
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        finally:
            if self.on_run_complete:
                self.on_run_complete(...)

**Worker Method**

Each worker processes requests from the shared queue:

.. code-block:: python

    async def _worker(self, worker_id: int) -> None:
        while True:
            # Check for graceful shutdown
            if self.stop_event and self.stop_event.is_set():
                break

            try:
                _priority, _counter, request = await self.request_queue.get()
            except asyncio.CancelledError:
                break

            try:
                # Process request (same logic as SyncDriver)
                response = await self.resolve_request(request)
                continuation = self.scraper.get_continuation(request.continuation)

                for item in continuation(response):
                    match item:
                        case ParsedData():
                            await self.handle_data(item.unwrap())
                        case Request():
                            await self.enqueue_request(item, response)
                        # ... other cases ...
            finally:
                self.request_queue.task_done()


Thread Safety
-------------

The AsyncDriver uses locks to protect shared state for the queue counter:

**Counter Protection**

.. code-block:: python

    async def enqueue_request(self, new_request, context):
        resolved = new_request.resolve_from(context)

        # Lock protects _queue_counter
        async with self._queue_lock:
            await self.request_queue.put(
                (resolved.priority, self._queue_counter, resolved)
            )
            self._queue_counter += 1

If we're okay dropping FIFO in an async context, we can remove that lock.
For now, I've left it as is to make the implementations and behavior as close
as possible.


Graceful Shutdown
-----------------

The ``stop_event`` parameter enables graceful shutdown:

.. code-block:: python

    import asyncio

    stop_event = asyncio.Event()

    driver = AsyncDriver(
        scraper=scraper,
        stop_event=stop_event,
        num_workers=4,
    )

    # In another coroutine or signal handler:
    stop_event.set()  # Workers will stop after current request

**Stop Event Behavior:**

1. If set before ``run()``, no requests are processed
2. If set during processing, workers complete their current request then stop
3. Queue is drained to prevent ``join()`` from blocking


Choosing Worker Count
---------------------

One worker should cause the AsyncDriver to behave basically the same as the SyncDriver.
More workers might overwhelm servers, be mindful.


Next Steps
----------

In :doc:`19_speculative_request`, we introduce speculative requests for handling
infinite pagination and optional resources. Scrapers can yield requests that may
or may not exist, receiving a boolean response to decide whether to continue.
