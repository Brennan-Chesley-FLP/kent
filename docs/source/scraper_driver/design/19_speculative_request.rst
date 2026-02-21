===================================
Step 19: Speculative Request
===================================

The Problem
-----------

Some scrapers take advantage of expected sequential IDs to gather data.
Generally, we don't know if an ID exists before we request the page for it, so we need a way of potentially handling
an unbounded number of potential pages, and deciding when to stop checking IDs.


The Solution
------------

The **@speculate decorator** enables scrapers to define functions that generate requests for sequential IDs.
The driver calls these functions with incrementing IDs, tracking successes and failures to determine when to stop:

.. code-block:: python

    from kent.common.decorators import speculate

    class MyScraper(BaseScraper[CaseData]):
        @speculate(highest_observed=500, largest_observed_gap=20)
        def fetch_case(self, case_id: int) -> Request:
            """Probe for a case by ID.

            The driver calls this with sequential IDs, tracking successes
            and failures to determine when to stop probing.
            """
            return Request(
                request=HTTPRequestParams(url=f"/case/{case_id}"),
                continuation=self.parse_case,
            )

        @step
        def parse_case(self, lxml_tree) -> Generator[ScraperYield, None, None]:
            # Parse the case page and yield data
            yield ParsedData(data=CaseData(...))

The scraper defines a function that takes an integer ID and returns a request.
The driver handles the rest—calling the function with sequential IDs, making requests,
and deciding when to stop based on response patterns.


How It Works
------------

The flow is:

1. **Discovery**: Driver introspects the scraper class using ``list_speculators()`` to find all ``@speculate`` decorated functions
2. **Configuration**: Driver reads decorator metadata (``highest_observed``, ``largest_observed_gap``) and optional consumer params (``definite_range``, ``plus``)
3. **Request Generation**: Driver calls the speculate function with sequential IDs to generate requests
4. **Request Execution**: Requests flow through normal pipeline (queue, deduplication)
5. **Success Tracking**: Driver tracks which IDs succeed (2xx responses) vs fail (non-2xx or deduplication)
6. **Stopping Criteria**: Driver stops when consecutive failures exceed the configured gap threshold


Key Decorator
-------------

@speculate
^^^^^^^^^^

Marks a function as generating speculative requests from sequential IDs:

.. code-block:: python

    @speculate(
        highest_observed=500,        # Highest ID known to exist
        largest_observed_gap=20,     # Max consecutive failures before stopping
        observation_date=date(2025, 1, 15)  # When values were last verified (optional)
    )
    def fetch_case(self, case_id: int) -> Request:
        return Request(
            request=HTTPRequestParams(url=f"/case/{case_id}"),
            continuation=self.parse_case,
        )

**Parameters:**

- ``highest_observed``: The highest ID observed to exist (defaults to 1)
- ``largest_observed_gap``: Max consecutive failures to tolerate (defaults to 10)
- ``observation_date``: Date when metadata was last updated (optional, for documentation)

**Function signature:**

- Must accept exactly one parameter (the ID) in addition to ``self``
- Must return a ``Request``
- The decorator automatically sets ``is_speculative=True`` on returned requests


Consumer Configuration
----------------------

Consumers can configure speculative functions via the params interface:

definite_range
^^^^^^^^^^^^^^

Specify an exact range of IDs to fetch (start, end inclusive):

.. code-block:: python

    params = MyScraper.params()
    params.speculative.fetch_case.definite_range = (100, 200)

    # Fetch a single ID
    params.speculative.fetch_case.definite_range = (12345, 12345)

When ``definite_range`` is set, the driver fetches all IDs in the range regardless of failures.
This is useful for:

- Fetching a specific case by ID
- Resuming from a known checkpoint
- Filling gaps in previously scraped data

plus
^^^^

Control how many consecutive failures to tolerate beyond the highest successful ID:

.. code-block:: python

    params = MyScraper.params()
    params.speculative.fetch_case.plus = 50  # Override decorator's largest_observed_gap

When ``plus`` is set, it overrides the ``largest_observed_gap`` from the decorator.
Set to 0 to stop immediately after the first failure beyond the definite range.


Driver Integration
------------------

Discovering Speculate Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Drivers use ``list_speculators()`` to discover all ``@speculate`` decorated functions:

.. code-block:: python

    speculators = MyScraper.list_speculators()
    # Returns: [('fetch_case', 500, None, 20)]
    #           (name, highest_observed, observation_date, largest_observed_gap)

This enables drivers to automatically seed their queues with speculative requests
without requiring explicit entry points.

Seeding the Queue
^^^^^^^^^^^^^^^^^

The driver calls each speculate function with sequential IDs based on configuration:

.. code-block:: python

    # Get configuration from params (or use decorator defaults)
    definite_range = params.speculative.fetch_case.definite_range or (1, highest_observed)
    plus = params.speculative.fetch_case.plus or largest_observed_gap

    # Generate requests for definite range
    for id in range(definite_range[0], definite_range[1] + 1):
        request = scraper.fetch_case(id)
        queue.add(request)

    # Continue probing beyond definite range until 'plus' consecutive failures
    current_id = definite_range[1] + 1
    consecutive_failures = 0
    while consecutive_failures < plus:
        request = scraper.fetch_case(current_id)
        queue.add(request)

        # Track success/failure after execution
        if request_succeeded:
            consecutive_failures = 0
        else:
            consecutive_failures += 1

        current_id += 1

Behavior
^^^^^^^^

The driver determines success/failure purely based on HTTP response status codes:

- **2xx response**: Success, calls continuation with response, will check a fails_successfully handler if present.
- **Non-2xx response**: Failure, skips continuation, increments failure counter
- **Deduplicated**: Treated as failure (prevents infinite loops)


Implementation Details
----------------------

Request Marking
^^^^^^^^^^^^^^^

The ``@speculate`` decorator automatically marks requests as speculative:

.. code-block:: python

    @wraps(fn)
    def wrapper(scraper_self: Any, id_value: int) -> Request:
        request = fn(scraper_self, id_value)
        # Set is_speculative=True on the request
        object.__setattr__(request, "is_speculative", True)
        return request

This enables drivers to identify and track speculative requests separately from
regular navigation requests.

Deduplication Handling
^^^^^^^^^^^^^^^^^^^^^^

When a speculative request is deduplicated (URL already seen):

- Driver treats it as a failure
- Increments consecutive failure counter
- Does not call the continuation
- This prevents infinite loops on duplicate URLs

Queue Priority
^^^^^^^^^^^^^^

Speculative requests inherit priority from their continuation step's ``@step`` decorator.
This ensures proper queue ordering in A*/depth-first traversal.


Usage Examples
--------------

Basic Sequential ID Probing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    class CaseScraper(BaseScraper[CaseData]):
        @speculate(highest_observed=50000, largest_observed_gap=100)
        def fetch_case(self, case_id: int) -> Request:
            """Probe for a case by ID."""
            return Request(
                request=HTTPRequestParams(url=f"/case/{case_id}"),
                continuation=self.parse_case,
            )

        @step
        def parse_case(self, lxml_tree) -> Generator[ScraperYield, None, None]:
            case_name = lxml_tree.checked_xpath("//h1/text()", "case name")[0]
            yield ParsedData(data=CaseData(case_id=..., case_name=case_name))

Multiple Speculate Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A scraper can have multiple ``@speculate`` functions for different ID sequences:

.. code-block:: python

    class MultiCourtScraper(BaseScraper[CaseData]):
        @speculate(highest_observed=275000, largest_observed_gap=20)
        def fetch_supreme_court_case(self, case_id: int) -> Request:
            return Request(
                request=HTTPRequestParams(url=f"/supreme/{case_id}"),
                continuation=self.parse_case,
            )

        @speculate(highest_observed=170000, largest_observed_gap=20)
        def fetch_appeals_case(self, case_id: int) -> Request:
            return Request(
                request=HTTPRequestParams(url=f"/appeals/{case_id}"),
                continuation=self.parse_case,
            )

        @step
        def parse_case(self, lxml_tree) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data=CaseData(...))

Consumers can configure each function independently:

.. code-block:: python

    params = MultiCourtScraper.params()
    params.speculative.fetch_supreme_court_case.definite_range = (275000, 275100)
    params.speculative.fetch_appeals_case.plus = 50

Fetching a Specific Case
^^^^^^^^^^^^^^^^^^^^^^^^^

Use ``definite_range`` to fetch a single case by ID:

.. code-block:: python

    params = CaseScraper.params()
    params.speculative.fetch_case.definite_range = (12345, 12345)
    params.speculative.fetch_case.plus = 0  # Don't probe beyond this ID

    scraper = CaseScraper(params=params)
    driver = SyncDriver(scraper)
    results = driver.scrape()

Resuming from a Checkpoint
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use ``definite_range`` to resume from a known checkpoint:

.. code-block:: python

    # Resume from ID 10000, probe 100 IDs beyond highest successful
    params = CaseScraper.params()
    params.speculative.fetch_case.definite_range = (10000, 50000)
    params.speculative.fetch_case.plus = 100

    scraper = CaseScraper(params=params)


Design Decisions
----------------

**Function-based approach**: Using simple functions (``int → Request``) instead of
generators makes the pattern more explicit and easier to understand.

**Automatic discovery**: Drivers can introspect scrapers using ``list_speculators()``
to find all speculative entry points without requiring explicit registration.

**Declarative configuration**: Consumers control behavior via params (``definite_range``, ``plus``)
instead of implementing custom callbacks. This makes common use cases simpler while
remaining flexible.

**Status-based success**: Determining success/failure purely from HTTP status codes
simplifies the driver implementation and makes behavior more predictable.

