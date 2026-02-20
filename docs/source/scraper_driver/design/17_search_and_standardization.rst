Step 17: Search and Standardization
====================================

The Problem
-----------

As scrapers grow in number and complexity, several challenges emerge:

1. **Discoverability** - How do users find scrapers for specific courts?
2. **Consistency** - How do we ensure all scrapers follow the same patterns?
3. **Documentation** - How do we auto-generate accurate documentation?
4. **Parameterization** - How do users configure what a scraper fetches?

In Step 17, we introduce **standardized metadata** on scrapers and
**@entry decorators** with typed parameters for configuring scraper
entry points.


Overview
--------

This step introduces:

1. **BaseScraper ClassVars** - Standardized metadata fields for autodoc
2. **@entry decorator** - Typed entry points replacing the old params system
3. **initial_seed() interface** - JSON-serializable dispatch for entry invocation
4. **schema() generation** - JSON Schema from entry point metadata
5. **Registry builder** - Auto-generates documentation from metadata


BaseScraper Metadata
--------------------

Every scraper should define these class variables for documentation:

.. code-block:: python

    from datetime import date
    from typing import ClassVar

    from kent.data_types import BaseScraper, ScraperStatus


    class MyScraper(BaseScraper[MyDataModel]):
        """Scraper for Example Court dockets."""

        # === REQUIRED METADATA ===

        # Court IDs from courts-db that this scraper covers
        court_ids: ClassVar[set[str]] = {"examplect", "examplectapp"}

        # Primary URL for the court system
        court_url: ClassVar[str] = "https://courts.example.gov/"

        # Data types produced (opinions, dockets, oral_arguments, etc.)
        data_types: ClassVar[set[str]] = {"dockets"}

        # Scraper lifecycle status
        status: ClassVar[ScraperStatus] = ScraperStatus.ACTIVE

        # === RECOMMENDED METADATA ===

        # Version tracking (date-based recommended)
        version: ClassVar[str] = "2025-01-03"
        last_verified: ClassVar[str] = "2025-01-03"

        # Earliest available records
        oldest_record: ClassVar[date] = date(1990, 1, 1)

        # === OPTIONAL METADATA ===

        # Authentication requirements
        requires_auth: ClassVar[bool] = False

        # Rate limiting (pyrate_limiter Rate objects)
        rate_limits: ClassVar[list[Rate]] = [Rate(2, Duration.SECOND)]

**Metadata Purpose:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Purpose
   * - ``court_ids``
     - Links to courts-db; enables coverage reports and filtering
   * - ``court_url``
     - Displayed in docs; used for manual verification
   * - ``data_types``
     - Coverage calculations; documentation categorization
   * - ``status``
     - Filter retired scrapers from production
   * - ``version``
     - Track scraper changes; cache invalidation
   * - ``oldest_record``
     - Set user expectations for data availability
   * - ``rate_limits``
     - Rate limiter configuration (list of pyrate_limiter Rate objects)


The @entry Decorator
--------------------

The ``@entry`` decorator marks scraper methods as entry points with typed
parameters. It replaces the old searchable annotation and ``params()`` proxy
system with a simpler, more explicit approach.

Each ``@entry`` method declares:

1. **What data type it produces** (the return type argument)
2. **What parameters it accepts** (via function signature type annotations)

**Basic Usage:**

.. code-block:: python

    from collections.abc import Generator
    from datetime import date

    from pydantic import BaseModel

    from kent.common.decorators import entry
    from kent.common.param_models import DateRange
    from kent.data_types import (
        BaseScraper,
        HttpMethod,
        HTTPRequestParams,
        Request,
    )


    class MyScraper(BaseScraper[CaseDocket]):

        @entry(CaseDocket)
        def search_by_name(
            self, name: str
        ) -> Generator[Request, None, None]:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"/search?name={name}",
                ),
                continuation="parse_results",
            )

        @entry(CaseDocket)
        def search_by_date(
            self, date_range: DateRange
        ) -> Generator[Request, None, None]:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"/search?start={date_range.start}&end={date_range.end}",
                ),
                continuation="parse_results",
            )

        @entry(CaseDocket)
        def fetch_by_number(
            self, docket_number: str
        ) -> Generator[Request, None, None]:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"/docket/{docket_number}",
                ),
                continuation="parse_detail",
            )

**What the decorator does:**

The ``@entry`` decorator inspects the function signature, extracts parameter
names and types, and attaches an ``EntryMetadata`` dataclass to the function.
It does **not** modify runtime behavior -- entry methods run exactly as written.

.. code-block:: python

    @dataclass(frozen=True)
    class EntryMetadata:
        return_type: type           # e.g. CaseDocket
        param_types: dict[str, type]  # e.g. {"name": str}
        func_name: str              # e.g. "search_by_name"
        speculative: bool = False
        observation_date: date | None = None
        highest_observed: int = 1
        largest_observed_gap: int = 10


Parameter Types
---------------

Entry function parameters must be one of:

1. **Pydantic BaseModel subclasses** - For structured, validated parameters
2. **Primitives** - ``str``, ``int``, or ``date``

Tuples and other types are explicitly rejected.

**Pydantic BaseModel Parameters:**

Use these when a parameter has multiple fields or needs validation:

.. code-block:: python

    from pydantic import BaseModel
    from kent.common.param_models import DateRange

    class OpinionFilters(BaseModel):
        court_id: str
        year: int

    @entry(Opinion)
    def search_opinions(
        self, filters: OpinionFilters
    ) -> Generator[Request, None, None]:
        ...

    @entry(CaseDocket)
    def search_by_date(
        self, date_range: DateRange
    ) -> Generator[Request, None, None]:
        ...

The ``kent.common.param_models`` module provides shared parameter models.
Currently available:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Model
     - Description
   * - ``DateRange``
     - Inclusive date range with ``start`` and ``end`` fields

**Primitive Parameters:**

Use these for simple, single-value parameters:

.. code-block:: python

    @entry(CaseDocket)
    def search_by_name(self, name: str) -> Generator[...]:
        ...

    @entry(CaseDocket)
    def fetch_by_id(self, record_id: int) -> Generator[...]:
        ...

    @entry(CaseDocket)
    def search_by_filing_date(self, filing_date: date) -> Generator[...]:
        ...


The initial_seed() Interface
----------------------------

The ``BaseScraper.initial_seed()`` method dispatches a JSON-serializable
list of parameter invocations to the appropriate ``@entry`` functions:

**Basic Usage:**

.. code-block:: python

    scraper = MyScraper()

    # Single invocation
    requests = scraper.initial_seed([
        {"search_by_name": {"name": "alice"}}
    ])

    # Multiple invocations (even to different entry points)
    requests = scraper.initial_seed([
        {"search_by_name": {"name": "alice"}},
        {"search_by_name": {"name": "bob"}},
        {"fetch_by_id": {"record_id": 99}},
    ])

    # BaseModel parameters passed as dicts (Pydantic validates)
    requests = scraper.initial_seed([
        {"search_by_date": {
            "date_range": {"start": "2024-01-01", "end": "2024-12-31"}
        }}
    ])

**Format:**

Each invocation is a single-key dict mapping the entry function name to
its keyword arguments:

.. code-block:: python

    [
        {"entry_function_name": {"param1": value1, "param2": value2}},
        ...
    ]

**Validation:**

``initial_seed()`` validates parameters through ``EntryMetadata.validate_params()``:

- BaseModel parameters are validated via Pydantic's ``model_validate()``
- ``date`` parameters accept both ``date`` objects and ISO format strings
- ``str`` and ``int`` parameters are coerced via their type constructors
- Unknown entry names raise ``ValueError``
- Missing or unexpected parameters raise ``ValueError``


The schema() Method
-------------------

The ``BaseScraper.schema()`` class method generates a JSON Schema describing
all entry points, their parameter types, and return types:

.. code-block:: python

    schema = MyScraper.schema()

**Example Output:**

.. code-block:: json

    {
        "scraper": "MyScraper",
        "entries": {
            "search_by_name": {
                "returns": "CaseDocket",
                "speculative": false,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"}
                    },
                    "required": ["name"]
                }
            },
            "search_by_date": {
                "returns": "CaseDocket",
                "speculative": false,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "date_range": {"$ref": "#/$defs/DateRange"}
                    },
                    "required": ["date_range"]
                }
            },
            "fetch_by_id": {
                "returns": "CaseDocket",
                "speculative": true,
                "highest_observed": 500,
                "largest_observed_gap": 20,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "record_id": {"type": "integer"}
                    },
                    "required": ["record_id"]
                }
            }
        },
        "$defs": {
            "DateRange": {
                "type": "object",
                "properties": {
                    "start": {"type": "string", "format": "date"},
                    "end": {"type": "string", "format": "date"}
                },
                "required": ["start", "end"],
                "title": "DateRange"
            }
        }
    }

**Type Mapping:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Python Type
     - JSON Schema
   * - ``str``
     - ``{"type": "string"}``
   * - ``int``
     - ``{"type": "integer"}``
   * - ``date``
     - ``{"type": "string", "format": "date"}``
   * - ``BaseModel`` subclass
     - ``{"$ref": "#/$defs/ModelName"}`` with full Pydantic schema in ``$defs``


Speculative Entries
-------------------

For scrapers that probe sequential IDs, ``@entry`` supports speculative mode:

.. code-block:: python

    @entry(
        CaseDocket,
        speculative=True,
        highest_observed=105336,
        largest_observed_gap=20,
    )
    def fetch_docket(
        self, crn: int
    ) -> Generator[Request, None, None]:
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"/docket/{crn}",
            ),
            continuation="parse_docket",
        )

The speculative metadata tells drivers:

- ``highest_observed`` - The highest ID known to exist
- ``largest_observed_gap`` - The largest gap seen in the sequence
- ``observation_date`` - When the metadata was last updated

Drivers use ``list_speculators()`` to discover speculative entries and
``list_entries()`` to get full entry metadata including speculative flags.


Entry Point Discovery
---------------------

``BaseScraper`` provides introspection methods for discovering entry points:

.. code-block:: python

    # List all entry points
    for info in MyScraper.list_entries():
        print(f"{info.name}: {info.return_type.__name__}")
        print(f"  params: {info.param_types}")
        print(f"  speculative: {info.speculative}")

    # List only speculative entries
    for name, highest, obs_date, gap in MyScraper.list_speculators():
        print(f"{name}: highest={highest}, gap={gap}")

    # Generate JSON Schema for all entries
    schema = MyScraper.schema()


Documentation Generation
------------------------

The registry builder script (``scripts/build_court_registry.py``) extracts
metadata from all scrapers and generates:

1. **court_registry.toml** - Machine-readable registry
2. **Sphinx documentation** - Auto-generated court coverage pages

**Running the Builder:**

.. code-block:: bash

    uv run python -m scripts.build_court_registry

**Generated Registry Fields:**

.. code-block:: toml

    [scrapers.Site]
    scraper_id = "Site"
    court_ids = ["bugct", "bugctapp"]
    court_url = "https://courts.bugcivil.gov/"
    data_types = ["dockets"]

    # Return type information
    return_type = "BugCourtDocket | BugCourtDocketEntry"

    # Lifecycle metadata
    status = "active"
    version = "2025-01-03"
    oldest_record = "1900-01-01"
    requires_auth = false
    msec_per_request = 500

    # Entry points
    [scrapers.Site.entries]
    search_by_date = {returns = "BugCourtDocket", speculative = false}
    fetch_docket = {returns = "BugCourtDocket", speculative = true, highest_observed = 105336}

**Sphinx Extension:**

The ``court_coverage`` Sphinx extension reads the registry and generates:

- Per-jurisdiction coverage pages
- Per-scraper documentation with metadata
- Coverage statistics and charts


Best Practices
--------------

**Metadata:**

1. Always set ``court_ids`` - Links your scraper to courts-db
2. Use accurate ``status`` - Don't leave scrapers as IN_DEVELOPMENT
3. Update ``version`` and ``last_verified`` regularly
4. Set ``oldest_record`` if known - Helps users set date filters

**@entry Decorators:**

1. Use Pydantic BaseModel parameters for structured input (date ranges, filters)
2. Use primitives (``str``, ``int``, ``date``) for simple single-value parameters
3. Set ``speculative=True`` with accurate metadata for ID-based scrapers
4. Every scraper needs at least one ``@entry`` method

**initial_seed():**

1. Pass parameters as JSON-serializable dicts
2. BaseModel parameters are passed as dicts and validated by Pydantic
3. ``date`` values can be ISO strings (``"2024-01-01"``) or ``date`` objects


Testing
-------

**Entry Point Discovery:**

.. code-block:: python

    def test_entry_points_discovered():
        """list_entries() shall find all @entry-decorated methods."""
        entries = MyScraper.list_entries()
        names = {e.name for e in entries}
        assert "search_by_name" in names
        assert "search_by_date" in names

**Entry Metadata:**

.. code-block:: python

    def test_entry_metadata():
        """@entry shall attach correct metadata."""
        meta = get_entry_metadata(MyScraper.search_by_name)
        assert meta.return_type is CaseDocket
        assert meta.param_types == {"name": str}
        assert meta.speculative is False

**Parameter Validation:**

.. code-block:: python

    def test_validate_basemodel_params():
        """validate_params() shall validate BaseModel parameters."""
        meta = get_entry_metadata(MyScraper.search_by_date)
        result = meta.validate_params(
            {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}}
        )
        assert isinstance(result["date_range"], DateRange)
        assert result["date_range"].start == date(2024, 1, 1)

**initial_seed() Dispatch:**

.. code-block:: python

    def test_initial_seed_dispatches():
        """initial_seed() shall dispatch to correct entry functions."""
        scraper = MyScraper()
        requests = list(
            scraper.initial_seed([{"search_by_name": {"name": "test"}}])
        )
        assert len(requests) == 1
        assert "name=test" in requests[0].request.url

**Schema Generation:**

.. code-block:: python

    def test_schema_is_json_serializable():
        """schema() shall produce valid JSON."""
        import json
        schema = MyScraper.schema()
        json.dumps(schema)  # Should not raise


Next Steps
----------

In :doc:`18_async_driver`, we introduce the AsyncDriver - an asynchronous
implementation that processes multiple requests concurrently using worker
coroutines for improved performance on I/O-bound workloads.
