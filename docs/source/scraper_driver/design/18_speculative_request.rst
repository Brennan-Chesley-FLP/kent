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

Speculation is driven by the ``Speculative`` protocol
(``kent.common.speculative``).  Scraper authors define a Pydantic
``BaseModel`` that implements the protocol, then use it as a parameter on
an ``@entry``-decorated function.  The driver detects it automatically and
handles seeding, tracking, extension, and stopping:

.. code-block:: python

    from pydantic import BaseModel
    from kent.common.decorators import entry, step

    class CaseId(BaseModel):
        case_id: int
        speculate: bool = True
        threshold: int = 0
        gap: int = 20

        def should_speculate(self) -> bool:
            return self.speculate
        def to_int(self) -> int:
            return self.case_id
        def from_int(self, n: int) -> "CaseId":
            return CaseId(case_id=n, speculate=self.speculate,
                          threshold=self.threshold, gap=self.gap)
        def check_success(self) -> bool:
            return self.case_id >= self.threshold
        def max_gap(self) -> int:
            return self.gap

    class MyScraper(BaseScraper[CaseData]):
        @entry(CaseData)
        def fetch_case(self, cid: CaseId) -> Request:
            return Request(
                request=HTTPRequestParams(url=f"/case/{cid.case_id}"),
                continuation=self.parse_case,
            )

        @step
        def parse_case(self, lxml_tree) -> Generator[ScraperYield, None, None]:
            yield ParsedData(data=CaseData(...))

The scraper defines an ``@entry`` whose parameter implements ``Speculative``.
The driver handles the rest — calling the function with sequential IDs, making requests,
and deciding when to stop based on response patterns.


The Speculative Protocol
------------------------

.. code-block:: python

    @runtime_checkable
    class Speculative(Protocol[T]):
        def should_speculate(self) -> bool: ...
        def to_int(self) -> int: ...
        def from_int(self, n: int) -> T: ...
        def check_success(self) -> bool: ...
        def max_gap(self) -> int: ...

**Methods:**

- ``should_speculate()`` — Whether the driver should run the adaptive
  speculation loop (extension + gap-based stopping).  When ``False`` the
  driver still seeds ``1..to_int()`` but does not track or extend.
  Controllable externally via params.

- ``to_int()`` — The integer representation of this instance.  On the
  template, this is the initial upper bound for seeding.

- ``from_int(n)`` — Creates a new instance for integer ID *n*, preserving
  all other fields (year, config, etc.) from the template.

- ``check_success()`` — Whether this ID should be evaluated for
  success/failure.  When ``False``, the request is seeded as non-speculative
  (unconditional — we know we want this data).  When ``True``, the driver
  checks status codes and ``fails_successfully()`` to determine outcome.

- ``max_gap()`` — Maximum consecutive failures to tolerate before stopping.
  Return ``0`` for frozen ranges (seed only, no extension).


How It Works
------------

The flow is:

1. **Detection**: The ``@entry`` decorator inspects parameter types.  If a parameter's
   type implements ``Speculative`` (checked via ``issubclass``), the entry is automatically
   marked speculative.  No extra decorator kwargs needed.

2. **Template Storage**: When ``initial_seed()`` is called with params, the validated
   ``Speculative`` model instance is stored as a *template* on the scraper
   (``_speculation_templates``).

3. **Discovery**: The driver calls ``_discover_speculate_functions()`` which finds
   templates from step 2 and creates a ``SpeculationState`` per template.

4. **Seeding**: For each template, the driver iterates ``1..to_int()`` and for each *n*:

   - Calls ``template.from_int(n)`` to get a concrete instance
   - If ``check_success()`` is **False**: seeds as a non-speculative request
   - If ``check_success()`` is **True**: seeds as a speculative request

5. **Tracking**: After each speculative response, the driver checks ``status_code`` and
   ``fails_successfully()`` to determine success or failure, updating
   ``highest_successful_id`` and ``consecutive_failures``.

6. **Extension**: When ``highest_successful_id`` approaches ``current_ceiling``, the
   driver seeds additional IDs (``current_ceiling + 1 .. current_ceiling + max_gap()``).

7. **Stopping**: When ``consecutive_failures >= max_gap()``, speculation stops.
   For ``max_gap() == 0`` (frozen), the driver stops immediately after seeding.


Year-Partitioned Probing
-------------------------

For scrapers that partition IDs by year (e.g., docket numbers like ``2025-00123``),
the Pydantic model encapsulates the year:

.. code-block:: python

    class DocketId(BaseModel):
        year: int
        number: int
        speculate: bool = True
        threshold: int = 0
        gap: int = 15

        def should_speculate(self) -> bool:
            return self.speculate
        def to_int(self) -> int:
            return self.number
        def from_int(self, n: int) -> "DocketId":
            return DocketId(year=self.year, number=n,
                            speculate=self.speculate,
                            threshold=self.threshold, gap=self.gap)
        def check_success(self) -> bool:
            return self.number >= self.threshold
        def max_gap(self) -> int:
            return self.gap

    class CourtScraper(BaseScraper[CaseData]):
        @entry(CaseData)
        def fetch_case(self, case_id: DocketId) -> Request:
            return Request(
                request=HTTPRequestParams(
                    url=f"/cases/{case_id.year}/{case_id.number}"
                ),
                continuation=self.parse_case,
            )

Multiple year partitions are supplied via ``seed_params``:

.. code-block:: python

    seed_params = [
        {"fetch_case": {"case_id": {"year": 2024, "number": 4000, "gap": 0}}},
        {"fetch_case": {"case_id": {"year": 2025, "number": 4000, "gap": 0}}},
        {"fetch_case": {"case_id": {"year": 2026, "number": 2000, "gap": 15}}},
    ]

Each invocation creates a separate template, tracked independently via
``param_index``.

**Frozen vs live partitions:**

- ``gap=0``: The driver seeds the range and stops.  No extension.  Use for
  historical years where the full range is known.
- ``gap=15``: The driver seeds the range, then continues probing beyond it
  until 15 consecutive failures.  Use for the current year where new cases
  are still being filed.


Driver Integration
------------------

Discovering Speculative Entries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Drivers use ``list_entries()`` to discover entry points.  Speculative entries
have ``speculative_param`` set to the name of the ``Speculative`` parameter:

.. code-block:: python

    entries = MyScraper.list_entries()
    for entry_info in entries:
        if entry_info.speculative:
            print(f"{entry_info.name}: speculative param = {entry_info.speculative_param}")

Seeding the Queue
^^^^^^^^^^^^^^^^^

The driver builds a ``SpeculationState`` for each template.  Each state
tracks:

- ``template``: The ``Speculative`` instance (template for ``from_int`` calls)
- ``param_index``: Position in the params list (for multi-template entries)
- ``highest_successful_id``: Watermark of successful IDs
- ``consecutive_failures``: Failure counter beyond the watermark
- ``current_ceiling``: Highest ID seeded so far
- ``stopped``: Whether speculation has stopped

The ``check_success()`` Split
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When seeding, each ID is classified:

- ``check_success() == False``: Seeded as a **non-speculative** request.
  The driver will fetch it unconditionally without tracking.  This is useful
  for IDs below a threshold that are known to exist.

- ``check_success() == True``: Seeded as a **speculative** request.
  The driver tracks success/failure and uses it for gap-based stopping.

This split happens regardless of ``should_speculate()``.

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
``(state_key, param_index, integer_id)``.  This enables drivers to identify and
track speculative requests separately from regular navigation requests.

Template Serialization
^^^^^^^^^^^^^^^^^^^^^^

The persistent driver serializes templates to the ``speculation_tracking``
table as ``template_json`` (via Pydantic's ``model_dump_json()``).  On resume,
templates are deserialized back using the param type's ``model_validate_json()``.
The ``param_index`` column ensures multi-template entries are matched correctly.


Usage Examples
--------------

Basic Sequential ID Probing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    class RecordId(BaseModel):
        record_id: int
        gap: int = 100
        def should_speculate(self) -> bool: return True
        def to_int(self) -> int: return self.record_id
        def from_int(self, n: int) -> "RecordId":
            return RecordId(record_id=n, gap=self.gap)
        def check_success(self) -> bool: return True
        def max_gap(self) -> int: return self.gap

    class CaseScraper(BaseScraper[CaseData]):
        @entry(CaseData)
        def fetch_case(self, rid: RecordId) -> Request:
            return Request(
                request=HTTPRequestParams(url=f"/case/{rid.record_id}"),
                continuation=self.parse_case,
            )

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

**Protocol-based**: Speculation semantics live in scraper-owned Pydantic models
that implement the ``Speculative`` protocol.  This makes the system open to
extension without framework changes — scraper authors define their own models
with whatever fields they need (year, court, threshold, etc.).

**Auto-detection**: The ``@entry`` decorator checks ``issubclass(T, Speculative)``
at import time.  No explicit ``speculative=`` kwarg needed.

**check_success split**: IDs below a threshold can be seeded as unconditional
(non-speculative) requests.  This handles the common pattern where a scraper
knows the first N records exist and only needs to speculate past that point.

**max_gap for frozen**: Returning ``max_gap() == 0`` gives frozen-range behavior
(seed once, no extension).  This replaces the old ``frozen=True`` flag on
``YearPartition``.

**Status-based success**: Determining success/failure from HTTP status codes
(plus optional ``fails_successfully()``) simplifies the driver implementation
and makes behavior more predictable.

**Template persistence**: The persistent driver stores templates as JSON in the
``speculation_tracking`` table so that runs can resume with the correct state
after server restarts.
