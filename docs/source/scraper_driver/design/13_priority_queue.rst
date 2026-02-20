Step 15: Priority Queue
========================

In the previous steps, the SyncDriver used a simple list to manage the request queue,
processing requests in FIFO (first-in-first-out) order. This works correctly but can
lead to unnecessary memory consumption when the scraper yields many requests that
navigate deeper into the site before processing terminal requests (like file downloads).

In Step 15, we introduce a **priority queue** to optimize memory usage by processing
high-priority terminal requests first.


Overview
--------

In this step, we introduce:

1. **priority field** - Added to Request with a default of 9
2. **heapq-based queue** - Replaced list with min-heap for priority ordering
3. **Archive request priority** - Default priority of 1 (higher than regular requests)
4. **FIFO tie-breaking** - Maintain insertion order for same-priority requests


Why Priority Queue?
-------------------

**Memory Optimization**

Consider a scraper that:
1. Fetches a list page with 100 case links (priority 9)
2. Navigates to each case detail page (priority 9)
3. Downloads a PDF for each case (priority 1 via archive request)

Without priority queue:
- All 100 case detail requests are queued
- Then all 100 PDF downloads are queued
- Peak queue size: ~200 requests in memory

With priority queue:
- Case detail request is processed
- PDF download (priority 1) is queued and processed next
- Case data is emitted and cleared
- Peak queue size: ~100 requests in memory

**Terminal Request Priority**

Archive requests are typically "terminal" - they don't generally yield more requests, just data.
Processing them early:
- Emits data faster (can be written to disk/database and freed)
- Reduces peak memory usage
- Clears completed work from the queue


Priority Field
--------------

The ``priority`` field is on ``Request`` with a default value of 9:

.. code-block:: python

    @dataclass(frozen=True)
    class Request:
        request: HTTPRequestParams
        continuation: str
        current_location: str = ""
        previous_requests: list[Request] = field(default_factory=list)
        accumulated_data: dict[str, Any] = field(default_factory=dict)
        aux_data: dict[str, Any] = field(default_factory=dict)
        nonnavigating: bool = False
        archive: bool = False
        expected_type: str | None = None
        priority: int = 9  # Lower number = higher priority

**Semantics:**

- **Lower number = higher priority** (min-heap ordering)
- **Default priority 9** - Regular navigating and non-navigating requests
- **Priority 1** - Archive requests (terminal, should be processed early)
- **Custom priorities** - Scrapers can set any integer priority, but it
  should generally be the number of steps left until you're done.

Archive Request Priority
-------------------------

Archive requests typically use priority 1:

.. code-block:: python

    yield Request(
        request=HTTPRequestParams(url="/opinions/case.pdf"),
        continuation="archive_opinion",
        archive=True,
        expected_type="pdf",
        priority=1,  # Higher priority than regular requests
    )

This ensures archive requests (file downloads) are processed before most navigating
requests, reducing queue size.


Implementation
--------------

The SyncDriver uses Python's ``heapq`` module to implement a min-heap priority queue:

.. code-block:: python

    import heapq

    class SyncDriver:
        def __init__(self, ...):
            # Step 15: Use heapq for priority queue (min heap)
            # Each entry is (priority, counter, request) for stable FIFO ordering
            self.request_queue: list[tuple[int, int, Request]] = []
            self._queue_counter = 0  # For FIFO tie-breaking within same priority

**Queue Operations:**

**Push (enqueue):**

.. code-block:: python

    def enqueue_request(self, new_request: Request, context: Response | Request):
        resolved_request = new_request.resolve_from(context)
        # Step 15: Push onto heap with priority and counter for stable ordering
        heapq.heappush(
            self.request_queue,
            (resolved_request.priority, self._queue_counter, resolved_request),
        )
        self._queue_counter += 1

**Pop (dequeue):**

.. code-block:: python

    def run(self):
        # ...
        while self.request_queue:
            # Step 15: Pop from heap (lowest priority first)
            _priority, _counter, request = heapq.heappop(self.request_queue)
            # Process request...

**FIFO Tie-Breaking:**

The counter ensures that requests with the same priority are processed in FIFO order:
- Tuple comparison in Python is lexicographic: ``(priority, counter, request)``
- If ``priority`` is equal, ``counter`` determines order
- ``counter`` always increases, ensuring FIFO

Next Steps
----------

In :doc:`14_deduplication`, we'll explore request deduplication to avoid
fetching the same URL multiple times when navigating complex site structures
with cyclic links.
