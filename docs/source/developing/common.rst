=============
Common Design
=============

This page covers the shared architecture, type system, and subsystems that
all drivers build on.


Architecture Overview
=====================

Kent's core insight is the **scraper-driver split**: scrapers are pure
generator functions that yield data and requests; drivers handle all side
effects. This separation is inspired by Scrapy but avoids its Twisted
dependency and addresses narrower concerns.

.. md-mermaid::
    :class: align-center

    flowchart LR
        subgraph Scraper["Scraper (Pure)"]
            Entry["@entry methods"]
            Steps["@step methods"]
        end

        subgraph Driver["Driver (Side Effects)"]
            Dedup["Deduplication"]
            Queue["Priority Queue (heapq)"]
            RateLimit["Rate Limiter"]
            HTTP["HTTP Client (httpx)"]
            Interstitial["Interstitial Handlers"]
            Archive["Archive Handler"]
            Callbacks["Callback System"]
        end

        Entry -->|Request| Dedup
        Dedup -->|Deduplicated Request| Queue
        Queue --> RateLimit
        RateLimit --> HTTP
        HTTP --> Interstitial
        Interstitial -->|Response| Steps
        Steps -->|ParsedData| Callbacks
        Steps -->|Request| Dedup
        Steps -->|ArchiveRequest| Archive

The flow:

1. ``@entry`` methods produce initial ``Request`` objects
2. The driver checks deduplication and enqueues novel requests in a priority heap
3. The driver dequeues, applies rate limiting, and makes the HTTP request
4. Interstitial handlers process any CAPTCHA or challenge pages before the response is passed on
5. The ``Response`` is routed to the ``@step`` continuation method
6. The step yields ``ParsedData`` (collected), ``Request`` (deduplicated and enqueued), or ``EstimateData`` (recorded)
7. Repeat until the queue is empty


Types
=====

``BaseRequest`` and ``Request``
-------------------------------

``BaseRequest`` is a frozen dataclass holding all request metadata:

- ``request: HTTPRequestParams`` -- the actual HTTP parameters
- ``continuation: str | Callable`` -- the step to call with the response
- ``accumulated_data``, ``permanent`` -- deep-copied context dicts
- ``priority`` -- for heap ordering
- ``deduplication_key`` -- SHA256 of URL + data (auto-generated but overridable)
- ``is_speculative``, ``speculation_id`` -- speculation tracking
- ``via`` -- replay hint for Playwright

``Request`` extends ``BaseRequest`` with:

- ``nonnavigating: bool`` -- doesn't update ``current_location``
- ``archive: bool`` -- triggers file download (auto-sets priority to 1)
- ``expected_type: str | None`` -- file type hint for archives

The ``__post_init__`` method deep-copies all mutable context dicts to prevent
cross-branch mutation bugs.


The Decorator System
====================

``@step`` -- Parameter Injection
--------------------------------

The ``@step`` decorator (in ``kent/common/decorators.py``) wraps step methods
to auto-inject parameters based on the function's signature. The decorator
inspects parameter names at decoration time and builds an injection plan:

.. code-block:: python

    @step
    def parse_detail(self, page: LxmlPageElement, response: Response, accumulated_data: dict):
        ...

At call time, the driver calls the wrapped function with the ``Response``
object. The decorator:

1. Checks which parameter names the function declared
2. Parses the response into the requested forms (HTML tree, JSON, etc.)
3. Passes only the requested parameters

This means the HTML parser is never invoked if the step only asks for
``json_content``, and JSON parsing is skipped if the step only asks for
``page``.

The decorator also:

- Resolves ``Callable`` continuations to string names (so ``self.parse_detail``
  becomes ``"parse_detail"`` in the serialized request)
- Attaches ``StepMetadata`` to the method for introspection
- Handles encoding and XSD/JSON schema validation options

``@entry`` -- Typed Entry Points
---------------------------------

The ``@entry`` decorator marks methods as scraper entry points and captures
their type signature:

.. code-block:: python

    @entry(CaseData)
    def fetch_case(self, case_id: YearlySpeculativeRange) -> Request:
        ...

The decorator:

1. Records the return data type (``CaseData``)
2. Inspects parameter types via ``get_type_hints``
3. Detects if any parameter implements the ``Speculative`` protocol
4. Attaches ``EntryMetadata`` with ``func_name``, ``return_type``,
   ``param_types``, and ``speculative_param``

The ``BaseScraper.initial_seed()`` method dispatches runtime params to the
correct entry method, validates kwargs against their Pydantic types, and
stores speculative templates for the driver to consume.


The Speculation System
======================

Speculation enables content discovery by probing sequential IDs. The system
has three components:

1. **The Speculative Protocol** (``kent/common/speculative.py``): A
   runtime-checkable Protocol that parameter models implement. Five methods:
   ``should_speculate()``, ``to_int()``, ``from_int(n)``,
   ``check_success()``, ``max_gap()``.

2. **Detection**: When ``@entry`` decorates a method, it checks if any
   parameter's type implements ``Speculative``. If so, it records the
   ``speculative_param`` name in ``EntryMetadata``.

3. **Driver Execution**: The driver's speculation loop has two phases:

   **Phase 1 (Non-speculative)**: Starting from ``to_int()``, seed upward
   while ``check_success()`` returns ``False``. These requests are
   unconditional.

   **Phase 2 (Speculative)**: Once ``check_success()`` returns ``True``,
   seed ``max_gap()`` speculative requests. Track success/failure:

   - On success: update ``highest_successful_id``, reset ``consecutive_failures``,
     extend the window if needed
   - On failure: increment ``consecutive_failures``
   - When ``consecutive_failures >= max_gap()``: stop speculation for this template

   ``fails_successfully()`` on the scraper detects soft-404 responses
   (HTTP 200 with error content).

**SpeculationState** tracks per-template state:

- ``highest_successful_id``: Watermark for successes
- ``consecutive_failures``: Current failure streak
- ``current_ceiling``: Highest ID seeded
- ``stopped``: Whether max_gap was reached


The PageElement Protocol
========================

``PageElement`` (``kent/common/page_element.py``) defines the interface for
parsed HTML interaction. ``LxmlPageElement`` (``kent/common/lxml_page_element.py``)
is the concrete implementation wrapping lxml's ``HtmlElement``.

We do three things that regular lxml does not.
- Require a description of the elements we're trying to select.
- Get min and max counts for elements so we can raise an exception on mismatch.
- Participate in observation for easy visual debugging.

Form support (``find_form``, ``Form``, ``FormField``) parses ``<form>``
elements and produces ``Request`` objects with the correct action URL, method,
and form data -- allowing scrapers to submit forms declaratively rather than
manually constructing POST requests.

Selector Observability
----------------------

``SelectorObserver`` (``kent/common/selector_observer.py``) records every
selector query made against a ``PageElement`` during a step's execution,
building a tree that mirrors the scraper's parsing structure. It is
automatically created by the ``@step`` decorator whenever HTML is parsed --
scraper authors do not need to set it up.

The observer records:

- The selector string and type (XPath or CSS)
- The human-readable description passed to the query method
- The match count and expected min/max bounds
- Sample text content from matched elements (up to 3 samples, truncated)
- Parent-child relationships between queries (e.g., querying cells within rows)

When the same selector is used repeatedly with the same parent (e.g.,
extracting a column from each row in a loop), the observer **deduplicates**
these into a single entry with aggregated match counts and samples.

**Output formats:**

``simple_tree()`` produces a human-readable tree::

    - //div[@id='mainContent']/table "Main Table" ✓ (1 match)
      - .//tr "Table Rows" ✓ (5 matches)
        - .//td[1] "Docket Column" ✓ (5 matches)
          → "BCC-2024-001"
        - .//td[2] "Name Column" ✗ (0 matches, expected 1+)

``json()`` returns a list of dictionaries suitable for JavaScript processing,
used by the persistent driver's web UI to provide visual selector highlighting.

**Autowait integration:** The Playwright driver uses the observer's
``compose_absolute_selector()`` method to construct absolute selectors from
relative ones. When a step fails with an ``HTMLStructuralAssumptionException``
during autowait, the driver walks the observer's parent chain to build the
full selector path, then waits for that selector to appear in the DOM before
retrying the snapshot. In practice, waitlists may make autowait fairly redundant.
The feature may be removed in a future release.

After step execution, the observer is stored on ``StepMetadata.observer``,
making it available to drivers and debugging tools.


Exception Design
================

The exception hierarchy reflects the two kinds of failures scrapers encounter:

**Assumption Exceptions** (``ScraperAssumptionException`` and subclasses):
The website changed or the scraper's expectations are wrong. These are
permanent failures that require code changes:

- ``HTMLStructuralAssumptionException``: Selector count mismatch
- ``DataFormatAssumptionException``: Pydantic validation failure

**Transient Exceptions** (``TransientException`` and subclasses):
Temporary failures that may resolve on retry:

- ``HTMLResponseAssumptionException``: Unexpected HTTP status code
- ``RequestTimeoutException``: Request timeout

The driver routes these to different callbacks, allowing different handling
strategies (log and continue vs. retry vs. halt).


Deferred Validation
===================

``DeferredValidation`` (``kent/common/deferred_validation.py``) wraps
unvalidated data and postpones Pydantic validation until ``.confirm()`` is
called. This design choice:

1. Lets scrapers yield data immediately without try/except around construction
2. Gives the driver a single point to handle validation errors via callbacks
3. Preserves the raw data for error reporting (you can see what was extracted
   even when validation fails)

The ``ScrapedData.raw()`` classmethod creates ``DeferredValidation`` instances.
The driver calls ``.confirm()`` on each piece of data, routing failures to
``on_invalid_data``.


Deep Copy Semantics
===================

``BaseRequest.__post_init__`` deep-copies ``accumulated_data`` and
``permanent``. This is a critical correctness property.

Without deep copy, when a step yields multiple requests sharing the same dict:

.. code-block:: python

    shared = {"count": 0}
    yield Request(..., accumulated_data=shared)
    yield Request(..., accumulated_data=shared)

Mutations in the first branch's step would silently affect the second branch.
Deep copy ensures each request gets an independent copy of its context.


Request Queue
=============

The queue is a min-heap with tuple entries
``(priority, counter, request)``:

- **priority**: Lower values are dequeued first. Default is 9 for navigating
  requests, 1 for archive requests (ensuring files are downloaded promptly).
- **counter**: Monotonically increasing integer for FIFO ordering within
  the same priority level.

Properly structuring a scrape to do depth-first traversal minimises the number
of requests in flight. It can also be used to prioritize requests to expiring links.


Deduplication
=============

Each request has a ``deduplication_key`` -- by default a SHA256 hash of the
URL and request data (query params, POST body, JSON body). The driver
maintains a ``_seen_keys`` set and skips requests with keys it has already
processed.

Custom deduplication is supported via the ``duplicate_check`` callback,
which receives the key and returns ``True`` to enqueue or ``False`` to skip.

Requests can opt out via ``SkipDeduplicationCheck``.


Rate Limiting
=============

Scrapers declare rate limits as ``pyrate_limiter.Rate`` objects on the class:

.. code-block:: python

    rate_limits = [Rate(2, Duration.SECOND)]

The driver passes these to the ``RequestManager``, which applies them before
each HTTP request. The ``bypass_rate_limit`` flag on individual requests can
skip rate limiting for time-sensitive operations (e.g., file downloads where
server-side state expires quickly).


Discovery and CLI
=================

``kent/discovery.py`` handles scraper discovery -- finding all
``BaseScraper`` subclasses in a given module path. The ``kent`` CLI
(``kent/cli.py``) uses this for:

- ``kent list``: Enumerate all discovered scrapers with metadata
- ``kent inspect MODULE:CLASS``: Show entry points, step methods, and JSON schema
- ``kent run MODULE:CLASS``: Execute a scraper with a chosen driver
- ``kent serve``: Launch the persistent driver's web UI

The CLI uses Click for command parsing and supports ``--driver``,
``--params``, ``--headed``, and other options.


Project Layout
==============

::

    kent/
    |-- __init__.py
    |-- cli.py                       # Main CLI (kent list/inspect/run/serve)
    |-- data_types.py                # Core types: BaseScraper, Request, Response, ParsedData
    |-- discovery.py                 # Scraper discovery
    |-- common/
    |   |-- decorators.py            # @step, @entry decorators
    |   |-- data_models.py           # ScrapedData base class
    |   |-- exceptions.py            # Exception hierarchy
    |   |-- request_manager.py       # HTTP client (sync/async)
    |   |-- lxml_page_element.py     # PageElement implementation
    |   |-- page_element.py          # PageElement protocol
    |   |-- checked_html.py          # Count-validated HTML parsing
    |   |-- speculative.py           # Speculative protocol
    |   |-- param_models.py          # DateRange, SpeculativeRange, etc.
    |   |-- deferred_validation.py   # Lazy validation wrapper
    |   |-- selector_observer.py     # XPath/CSS recording for debugging
    |-- driver/
    |   |-- sync_driver.py           # Synchronous driver
    |   |-- async_driver.py          # Async driver
    |   |-- callbacks.py             # Callback system
    |   |-- archive_handler.py       # File archival
    |   |-- interstitials.py         # CAPTCHA handling
    |   |-- persistent_driver/       # SQLite persistence
    |   |   |-- persistent_driver.py
    |   |   |-- models.py            # DB schema (SQLModel)
    |   |   |-- _queue.py            # Priority queue management
    |   |   |-- _speculation.py      # Speculation state tracking
    |   |   |-- _workers.py          # Async worker threads
    |   |   |-- cli/                 # pdd CLI
    |   |   |-- web/                 # Web UI
    |   |   |-- sql_manager/         # DB operations
    |   |   |-- migrations/          # Schema migrations
    |   |-- playwright_driver/       # Browser automation
    |-- demo/                        # BugCivilCourt demo
        |-- app.py                   # FastAPI mock server
        |-- scraper.py               # Reference scraper
        |-- models.py                # Demo data models
