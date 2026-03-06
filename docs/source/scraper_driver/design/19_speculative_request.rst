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

Speculation is configured directly on ``@entry`` decorators using
``SimpleSpeculation`` or ``YearlySpeculation``.  The driver reads the
configuration, generates requests for sequential IDs, tracks successes
and failures, and decides when to stop:

.. code-block:: python

    from kent.common.decorators import entry, step
    from kent.common.speculation_types import SimpleSpeculation

    class MyScraper(BaseScraper[CaseData]):
        @entry(
            CaseData,
            speculative=SimpleSpeculation(
                highest_observed=500,
                largest_observed_gap=20,
            ),
        )
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

The scraper defines an ``@entry`` that takes an integer ID and returns a request.
The driver handles the rest — calling the function with sequential IDs, making requests,
and deciding when to stop based on response patterns.


How It Works
------------

The flow is:

1. **Discovery**: Driver introspects the scraper class using ``list_entries()`` to find entries with speculation config
2. **Configuration**: Driver reads speculation metadata (``highest_observed``, ``largest_observed_gap``) and builds a ``SpeculateFunctionConfig`` with ``definite_range`` and ``plus``
3. **Request Generation**: Driver calls the entry function with sequential IDs to generate requests
4. **Request Execution**: Requests flow through normal pipeline (queue, deduplication)
5. **Success Tracking**: Driver tracks which IDs succeed (2xx responses) vs fail (non-2xx or deduplication)
6. **Stopping Criteria**: Driver stops when consecutive failures exceed the configured gap threshold


Speculation Types
-----------------

SimpleSpeculation
^^^^^^^^^^^^^^^^^

For scrapers that probe a single sequential integer ID:

.. code-block:: python

    from kent.common.speculation_types import SimpleSpeculation

    @entry(
        CaseData,
        speculative=SimpleSpeculation(
            highest_observed=500,        # Highest ID known to exist
            largest_observed_gap=20,     # Max consecutive failures before stopping
            observation_date=date(2025, 1, 15),  # When values were last verified (optional)
        ),
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

- Must accept exactly one ``int`` parameter (the ID) in addition to ``self``
- Must return a ``Request``

YearlySpeculation
^^^^^^^^^^^^^^^^^

For scrapers that partition IDs by year (e.g., docket numbers like ``2025-00123``):

.. code-block:: python

    from datetime import timedelta
    from kent.common.speculation_types import YearlySpeculation, YearPartition

    @entry(
        CaseData,
        speculative=YearlySpeculation(
            backfill=(
                YearPartition(year=2024, number=(1, 10), frozen=True),
                YearPartition(year=2025, number=(1, 10), frozen=True),
                YearPartition(year=2026, number=(1, 10), frozen=False),
            ),
            trailing_period=timedelta(days=60),
            largest_observed_gap=3,
        ),
    )
    def fetch_case(self, year: int, number: int) -> Request:
        return Request(
            request=HTTPRequestParams(url=f"/cases/{year}/{number}"),
            continuation=self.parse_case,
        )

**YearlySpeculation parameters:**

- ``backfill``: Tuple of ``YearPartition`` objects defining year-specific ranges
- ``trailing_period``: After January 1, keep probing the previous year for this duration (defaults to 60 days)
- ``largest_observed_gap``: Max consecutive failures before stopping a non-frozen partition (defaults to 10)

**YearPartition parameters:**

- ``year``: The calendar year
- ``number``: Tuple ``(start, end)`` defining the range of the speculative axis
- ``frozen``: If ``True``, this is a backfill-only range with no adaptive extension. If ``False``, the driver extends past the upper bound when it finds successes.

**Function signature:**

- Must accept exactly two ``int`` parameters: one named ``year`` and one other (the speculative axis)
- Must return a ``Request``


Driver Integration
------------------

Discovering Speculative Entries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Drivers use ``list_entries()`` to discover entry points with speculation config:

.. code-block:: python

    entries = MyScraper.list_entries()
    # Each EntryInfo has a .speculation field:
    #   None for normal entries
    #   SimpleSpeculation or YearlySpeculation for speculative entries

Seeding the Queue
^^^^^^^^^^^^^^^^^

The driver builds a ``SpeculationState`` for each speculative entry (or each
year-partition for ``YearlySpeculation``), with a ``SpeculateFunctionConfig``
controlling the ``definite_range`` and ``plus``:

- ``definite_range``: Tuple ``(start, end)`` of IDs to fetch with certainty.
  Defaults to ``(1, highest_observed)`` from the speculation metadata.
- ``plus``: Number of consecutive failures to tolerate beyond the definite range.
  Defaults to ``largest_observed_gap`` from the speculation metadata.

For ``YearlySpeculation``, the driver also handles:

- **Rollover**: Automatically creates the current year's partition if not listed in ``backfill``
- **Trailing period**: Continues probing the previous year for the configured duration after January 1

Behavior
^^^^^^^^

The driver determines success/failure based on HTTP response status codes and
the scraper's ``fails_successfully()`` method:

- **2xx response**: Calls continuation with response. If a ``fails_successfully()`` handler is present, it is checked to detect soft-404 pages.
- **Non-2xx response**: Failure, skips continuation, increments failure counter
- **Deduplicated**: Treated as failure (prevents infinite loops)


Implementation Details
----------------------

Request Marking
^^^^^^^^^^^^^^^

Requests generated by speculative entries are automatically marked with
``is_speculative=True`` and a ``speculation_id`` tuple of
``(function_name, integer_id)``.  This enables drivers to identify and
track speculative requests separately from regular navigation requests.

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
        @entry(
            CaseData,
            speculative=SimpleSpeculation(
                highest_observed=50000,
                largest_observed_gap=100,
            ),
        )
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

Year-Partitioned Probing
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    class CourtScraper(BaseScraper[CaseData]):
        @entry(
            CaseData,
            speculative=YearlySpeculation(
                backfill=(
                    YearPartition(year=2024, number=(1, 4000), frozen=True),
                    YearPartition(year=2025, number=(1, 2000), frozen=False),
                ),
                trailing_period=timedelta(days=60),
                largest_observed_gap=15,
            ),
        )
        def fetch_case(self, year: int, number: int) -> Request:
            return Request(
                request=HTTPRequestParams(url=f"/cases/{year}/{number}"),
                continuation=self.parse_case,
            )

        @step
        def parse_case(self, lxml_tree) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data=CaseData(...))

Frozen vs non-frozen partitions:

- **frozen=True**: The driver fetches exactly the IDs in the ``number`` range and stops. Useful for historical years where the full range is known.
- **frozen=False**: The driver fetches the ``number`` range, then continues probing beyond it until ``largest_observed_gap`` consecutive failures. Useful for the current year where new cases are still being filed.

Soft-404 Detection
^^^^^^^^^^^^^^^^^^^

Override ``fails_successfully()`` on the scraper to detect pages that return
200 but aren't real content:

.. code-block:: python

    class CaseScraper(BaseScraper[CaseData]):
        def fails_successfully(self, response: Response) -> bool:
            """Return True if the response is a real page, False if soft-404."""
            return "Case Not Found" not in response.text


Design Decisions
----------------

**Integrated with @entry**: Speculation is configured on ``@entry`` decorators rather
than a separate ``@speculate`` decorator.  This keeps entry point discovery unified
through ``list_entries()`` and avoids a parallel discovery mechanism.

**Year-aware partitioning**: ``YearlySpeculation`` is a first-class concept because
many courts use year-prefixed docket numbers.  The driver handles rollover and
trailing periods automatically.

**Frozen vs adaptive**: The ``frozen`` flag on ``YearPartition`` lets scrapers
distinguish between historical backfill (exact range) and live probing (adaptive extension).

**Status-based success**: Determining success/failure from HTTP status codes
(plus optional ``fails_successfully()``) simplifies the driver implementation
and makes behavior more predictable.
