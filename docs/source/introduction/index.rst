========================
An Introduction to Kent
========================

Kent is a scraper-driver framework that separates **parsing logic** (scrapers)
from **I/O orchestration** (drivers). Scrapers are pure functions: they receive
HTML or JSON, parse it, and yield data or requests for more pages. Drivers
handle HTTP requests, file storage, rate limiting, and persistence.

This guide covers everything you need to know to write scrapers in Kent.


Core Concepts
=============

The Scraper-Driver Split
------------------------

.. md-mermaid::
    :class: align-center

    flowchart TB
        subgraph Driver["Driver (Side Effects)"]
            HTTP[HTTP Client]
            Queue[Request Queue]
            Storage[File Storage]
            Hooks[Event Hooks]
        end

        subgraph Scraper["Scraper (Pure)"]
            Parse[Parse HTML/JSON]
            Yield[Yield Data or Requests]
        end

        HTTP -->|Response| Parse
        Parse --> Yield
        Yield -->|Data| Hooks
        Yield -->|Request| Queue
        Yield -->|Download File| Storage
        Queue --> HTTP

**Scrapers** are generator functions that:

1. Receive a parsed page (HTML tree, JSON, or raw text)
2. Extract data from it
3. Yield ``ParsedData`` for collected results or ``Request`` for follow-on pages

**Drivers** orchestrate everything else:

- Execute HTTP requests
- Feed responses to scraper steps
- Collect yielded data
- Handle errors, retries, rate limiting
- Manage file archival and deduplication

Because scrapers never perform I/O, they can be tested against static HTML
without mocking. The driver can be swapped depending on environment (sync for
testing, persistent for production, different http client libs swapped out, etc.).


Writing a Scraper
=================

Class Structure
---------------

Every scraper extends ``BaseScraper[ReturnType]``, where ``ReturnType`` is the
union of all data model types the scraper produces.

Each scraper class corresponds to a single web server (or closely related
cluster of servers). This 1:1 mapping means rate limiting is applied per
server, and when a court changes its website, all the affected parsing logic
is grouped together in one place, making it straightforward to update.

.. code-block:: python

    from datetime import date
    from pyrate_limiter import Duration, Rate
    from kent.data_types import BaseScraper, DriverRequirement, ScraperStatus
    from my_models import CaseData, OpinionData

    MyData = CaseData | OpinionData

    class MyCourtScraper(BaseScraper[MyData]):
        court_url = "https://example.court.gov"
        court_ids = {"example.court"}
        data_types = {"opinions", "dockets"}
        status = ScraperStatus.ACTIVE
        version = "2025-03-15"
        last_verified = "2025-04-01"
        oldest_record = date(2010, 1, 1)
        requires_auth = False
        rate_limits = [Rate(1, Duration.SECOND)]
        driver_requirements = [DriverRequirement.JS_EVAL]

Class-level attributes provide metadata:

- ``court_url``: The primary URL for this court's web server.

  .. code-block:: python

      court_url = "https://example.court.gov"

- ``court_ids``: Set of court identifiers (references ``courts.toml``).

  .. code-block:: python

      court_ids = {"ca9", "ca9b"}

- ``rate_limits``: List of ``pyrate_limiter.Rate`` objects controlling how
  fast we hit this server. A good default for testing is given above.
  Bump it up gradually to observe rate limiting or server slowdown.

  .. code-block:: python

      rate_limits = [Rate(1, Duration.SECOND)]  # 1 requests per second
      rate_limits = [Rate(100, Duration.SECOND), RATE(500, Duration.HOUR)] # 100/second and 500/hour

- ``status``: Development lifecycle status.

  .. code-block:: python

      status = ScraperStatus.ACTIVE  # or IN_DEVELOPMENT, RETIRED

- ``driver_requirements``: Capabilities the scraper needs from its driver.
  See `Driver Requirements`_ below.

  .. code-block:: python

      driver_requirements = [DriverRequirement.JS_EVAL]

- ``requires_auth``: Whether authentication is needed.

  .. code-block:: python

      requires_auth = True

- ``version``: Version string for tracking scraper changes.

  .. code-block:: python

      version = "2025-03-15"

- ``last_verified``: Date the scraper was last confirmed working.

  .. code-block:: python

      last_verified = "2025-04-01"

- ``oldest_record``: Earliest date for which records are available.

  .. code-block:: python

      oldest_record = date(2010, 1, 1)

- ``ssl_context``: Custom SSL context for servers requiring specific ciphers
  or TLS versions. Override ``get_ssl_context()`` for complex cases.

  .. code-block:: python

      @classmethod
      def get_ssl_context(cls) -> ssl.SSLContext:
          ctx = ssl.create_default_context()
          ctx.set_ciphers("AES256-SHA256")
          return ctx


Entry Points
------------

Entry points are methods decorated with ``@entry`` that produce the initial
requests for a scraping run. They are the mechanism for **parameterizing
runs** -- allowing callers to specify what to scrape, such as a date range, a
range of docket numbers, a list of courts, etc..

Each entry point declares the data type it produces, and yields requests
for URLs that are accessible to the public without authentication.

.. code-block:: python

    from kent.common.decorators import entry, step
    from kent.common.param_models import DateRange, YearlySpeculativeRange
    from kent.data_types import HTTPRequestParams, HttpMethod, Request

    # Search by date range
    @entry(CaseData)
    def search_by_date(self, date_range: DateRange) -> Generator[Request, None, None]:
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.court_url}/search?from={date_range.start}&to={date_range.end}",
            ),
            continuation=self.parse_search_results,
        )

    # Fetch a specific docket by number
    @entry(CaseData)
    def fetch_case(self, case_id: YearlySpeculativeRange) -> Request:
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.court_url}/cases/{case_id.year}/{case_id.number}",
            ),
            continuation=self.parse_case_detail,
        )

    # Scrape a fixed listing page (no parameters)
    @entry(OralArgumentData)
    def get_oral_arguments(self) -> Generator[Request, None, None]:
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.court_url}/oral-arguments",
            ),
            continuation=self.parse_oral_arguments_list,
        )

Entry points can accept typed parameters:

- **Primitives**: ``int``, ``str``, ``date``
- **Pydantic models**: ``DateRange``, ``SpeculativeRange``, ``YearlySpeculativeRange``, or custom models
- **No parameters**: For entries that always start from the same URL

An entry can return a single ``Request`` or a ``Generator[Request, None, None]``
that yields multiple starting requests.


Steps
-----

Steps are methods decorated with ``@step`` that process responses. They are the
workhorse of a scraper: each step receives a response and yields data and/or
further requests.

.. code-block:: python

    @step
    def parse_search_results(
        self,
        page: LxmlPageElement,
    ) -> Generator[ScraperYield, None, None]:
        rows = page.query_xpath(
            "//tr[@class='result-row']",
            "search result rows",
            min_count=1,
        )
        for row in rows:
            links = row.find_links(
                ".//a[@class='case-link']", "case link",
                min_count=1, max_count=1,
            )
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=links[0].url,
                ),
                continuation=self.parse_case_detail,
            )

The ``@step`` decorator auto-injects parameters based on the function
signature. You only request what you need.

**Commonly Used Parameters**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Parameter
     - Description
   * - ``page``
     - ``LxmlPageElement`` wrapper for parsed HTML (the most common choice)
   * - ``json_content``
     - Auto-parsed JSON from the response body
   * - ``text``
     - Raw ``response.text``
   * - ``accumulated_data``
     - Dict carried through the request chain (see `Accumulated Data`_)
   * - ``local_filepath``
     - Local path for archive responses (file downloads)

**Debugging Parameters**

These are useful during development and debugging but rarely needed in
production scrapers:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Parameter
     - Description
   * - ``response``
     - The full ``Response`` object (url, status_code, text, content, headers, cookies)
   * - ``request``
     - The current ``Request`` that produced this response
   * - ``lxml_tree``
     - Raw ``CheckedHtmlElement`` tree -- a lower-level view of the parsed HTML, useful for debugging selector issues
   * - ``previous_request``
     - The parent request that led to this one

Wait Lists (Playwright)
^^^^^^^^^^^^^^^^^^^^^^^^

For sites that require JavaScript evaluation and a browser, the DOM is not
ready immediately after navigation. **Wait lists** tell the Playwright driver
when to capture the DOM snapshot and hand it off to the scraper step.

.. code-block:: python

    from kent.data_types import WaitForSelector, WaitForLoadState

    @step(await_list=[
        WaitForLoadState(state="networkidle"),
        WaitForSelector("//table[@id='results']"),
    ])
    def parse_results(self, page: LxmlPageElement) -> Generator[ScraperYield, None, None]:
        ...

Available wait conditions:

- ``WaitForSelector(selector, state="visible", timeout=None)``: Wait for a CSS/XPath selector to appear in the DOM.
- ``WaitForLoadState(state="load", timeout=None)``: Wait for a load state (``"load"``, ``"domcontentloaded"``, ``"networkidle"``).
- ``WaitForURL(url, timeout=None)``: Wait for the URL to match a pattern.
- ``WaitForTimeout(timeout)``: Wait for a fixed number of milliseconds.

The HTTP-based drivers ignore ``await_list`` -- it only applies to the
Playwright driver.


Data Types
==========

Request
-------

``Request`` is the primary type you yield to ask the driver to fetch a page.

.. code-block:: python

    Request(
        request=HTTPRequestParams(
            method=HttpMethod.GET,
            url="https://example.court.gov/case/123",
        ),
        continuation=self.parse_case_detail,
    )

Important fields:

- ``request``: An ``HTTPRequestParams`` with method, URL, headers, data, etc.
- ``continuation``: The ``@step`` method to call with the response. Pass ``self.method_name`` (the decorator resolves it to a string).
- ``accumulated_data``: Dict of context to carry forward (e.g., a docket number extracted from a list page).
- ``nonnavigating``: Set ``True`` for API calls that don't change navigation context. See `Nonnavigating Requests`_.
- ``archive``: Set ``True`` for file downloads (audio, images, PDFs). See `Archive Requests`_.
- ``expected_type``: File extension hint for archive requests (``"pdf"``, ``"mp3"``, ``"jpeg"``, ``"wpd"``).
- ``priority``: Queue priority (lower = higher priority, default 9, archives default to 1).
- ``permanent``: Persistent headers/cookies that flow through the entire request chain.

ParsedData
----------

Wrap your data models in ``ParsedData`` when yielding results:

.. code-block:: python

    yield ParsedData(
        CaseData.raw(
            request_url=response.url,
            docket="BCC-2024-001",
            case_name="Ant v. Bee",
            date_filed="2024-03-15",
        )
    )

Use ``.raw()`` for **deferred validation** -- the data is validated later by the
driver rather than immediately. This is the standard pattern, and allows us to
collect invalid data for debugging purposes.

EstimateData
------------

Yield ``EstimateData`` to capture **total result counts** reported by the
website for post-hoc integrity checking. For example, when a search results
page says "355 results found", emit an ``EstimateData`` with that count. After
the scrape completes, the driver can verify that the actual number of items
collected matches the estimate.

.. code-block:: python

    total = int(page.query_xpath_strings(
        "//span[@class='total']/text()", "result count",
    )[0])

    yield EstimateData(
        expected_types=(CaseData,),
        min_count=total,
        max_count=total,
    )

    # Then yield Requests for each result page...

Response
--------

The ``Response`` object passed to steps contains:

- ``url``: Final URL (after redirects)
- ``status_code``: HTTP status code
- ``text``: Response body as text
- ``content``: Response body as bytes
- ``headers``: Response headers dict
- ``cookies``: Response cookies dict


HTML Parsing with PageElement
=============================

The ``page`` parameter (type ``LxmlPageElement``) is the primary interface for
HTML parsing. It wraps lxml with count validation -- selectors that return
unexpected counts raise ``HTMLStructuralAssumptionException`` immediately, making
it easy to detect when a website's structure changes.

``LxmlPageElement`` also provides **visual debugging capabilities** through the
``SelectorObserver`` system, which records which selectors were used and what
they matched. This is useful for diagnosing scraper failures and understanding
page structure.

Querying Elements
-----------------

.. code-block:: python

    # XPath -- returns list of LxmlPageElement
    rows = page.query_xpath(
        "//tr[@class='case-row']",
        "case rows",          # description for error messages
        min_count=1,          # at least 1 expected (default)
        max_count=None,       # no upper bound (default None)
    )

    # CSS -- same interface
    links = page.query_css(
        "a.case-link",
        "case links",
        min_count=1,
    )

Extracting Text
---------------

.. code-block:: python

    # Text from elements
    docket = page.query_xpath(
        "//*[@id='docket']", "docket", min_count=1, max_count=1
    )[0].text_content().strip()

    # XPath string expressions (text(), @attr, etc.)
    names = page.query_xpath_strings(
        "//td[@class='name']/text()",
        "case names",
        min_count=1,
    )

Finding Links and ViaLink Navigation
-------------------------------------

.. code-block:: python

    links = page.find_links(
        "//a[@class='detail-link']",
        "detail page links",
        min_count=1,
    )
    for link in links:
        yield Request(
            request=HTTPRequestParams(method=HttpMethod.GET, url=link.url),
            continuation=self.parse_detail,
        )

``find_links`` returns ``Link`` objects with ``.url`` and ``.text`` attributes.
URLs are automatically resolved relative to the page's base URL.

Requests produced by ``find_links`` automatically carry a ``ViaLink``
descriptor. When the Playwright driver processes the request, it uses this
to **click the actual link element** in the browser rather than navigating
directly to the URL. This is important for JavaScript-heavy sites where
clicking a link triggers client-side routing or other JS behavior. For the
HTTP-based drivers, the ``via`` field is simply ignored.

Form Submission and ViaFormSubmit
---------------------------------

.. code-block:: python

    form = page.find_form("//form[@id='search']", "search form")
    request = form.submit(data={
        "from_date": "2024-01-01",
        "to_date": "2024-12-31",
    })
    yield replace(request, continuation=self.parse_results)

``find_form`` parses the ``<form>`` element and returns a ``Form`` object.
``Form.submit()`` returns a ``Request`` pre-filled with the form's action URL
and method. Use ``dataclasses.replace()`` to set the continuation.

The ``Form`` and ``submit()`` helpers capture the form the way a browser would:
hidden fields, default values, and the correct encoding are all preserved. The
resulting request carries a ``ViaFormSubmit`` descriptor so the Playwright
driver can **fill and submit the form in the browser**, reproducing
browser behavior including any JavaScript event handlers. This also works
correctly with HTTP-based drivers, where the ``via`` field is ignored and the
request is sent as a standard POST (or GET, depending on the form's method).


Data Models
===========

Scraper output types extend ``ScrapedData``, which inherits from ``SQLModel``
(Pydantic + SQLAlchemy):

.. code-block:: python

    from pydantic import Field
    from kent.common.data_models import ScrapedData

    class CaseData(ScrapedData):
        docket: str = Field(..., description="Docket number")
        case_name: str = Field(..., description="Full case name")
        date_filed: date = Field(..., description="Filing date")
        status: str = Field(..., description="Current status")

All fields use Pydantic's ``Field`` with required values (``...``) and
descriptions. The ``.raw()`` classmethod creates a ``DeferredValidation``
wrapper so the driver validates data at collection time, producing
``DataFormatAssumptionException`` on failure rather than crashing the step.

Importantly, **all data is kept, even when validation fails**. Invalid data is
saved alongside valid data and can be inspected after a scrape completes. This
is extremely useful for debugging -- you can see exactly what was extracted from
the page and which fields failed validation, without losing any information.


Accumulated Data
================

``accumulated_data`` passes context through a chain of requests. A common
pattern: extract a docket number from a list page, then carry it to the detail
page parser:

.. code-block:: python

    @step
    def parse_list(self, page):
        for row in rows:
            docket = row.query_xpath(".//td[1]/text()", "docket", min_count=1)[0]
            yield Request(
                request=HTTPRequestParams(method=HttpMethod.GET, url=link.url),
                continuation=self.parse_detail,
                accumulated_data={"docket": docket},
            )

    @step
    def parse_detail(self, page, accumulated_data):
        yield ParsedData(CaseData.raw(
            docket=accumulated_data["docket"],
            case_name=...,
        ))

Each request deep-copies its ``accumulated_data``, so mutations in one branch
never affect siblings.


Error Handling
==============

The sync and async drivers expose **callbacks** for each error type, giving
callers fine-grained control over how errors are handled. The persistent and
Playwright drivers, for the most part, have **sensible defaults** that save
data and errors to the database for post-run inspection.

Structural Errors
-----------------

``HTMLStructuralAssumptionException`` is raised automatically when
``page.query_xpath()`` or ``page.query_css()`` find a count outside the
``min_count``/``max_count`` bounds. This is your primary signal that a website
has changed its HTML structure.

You do not need to catch these -- the driver handles them.

Transient Errors
----------------

``TransientException`` and its subclasses (``HTMLResponseAssumptionException``,
``RequestTimeoutException``) represent retryable failures. The driver handles
retry logic.

Sometimes servers are unreliable and return error pages or degraded responses
with a 200 status code. When you detect this in a step, it is appropriate to
raise a ``TransientException`` so the driver can retry the request:

.. code-block:: python

    from kent.common.exceptions import TransientException

    @step
    def parse_detail(self, page, response):
        if "Service Temporarily Unavailable" in response.text:
            raise TransientException("Server returned maintenance page")
        ...

Data Validation Errors
----------------------

``DataFormatAssumptionException`` is raised when deferred validation
(``.raw().confirm()``) fails. The driver routes these to the
``on_invalid_data`` callback. The invalid data is still preserved for
debugging.


Speculative Requests
====================

Speculative requests let you discover content by probing sequential IDs. The
driver automatically seeds a range of IDs and tracks which ones succeed.

To use speculation, make your ``@entry`` parameter implement the
``Speculative`` protocol. Kent provides two built-in types:

- ``SpeculativeRange``: For simple sequential IDs (e.g., ``/case/1``, ``/case/2``, ...)
- ``YearlySpeculativeRange``: For year-partitioned IDs (e.g., ``/cases/2024/1``, ``/cases/2024/2``, ...)

.. code-block:: python

    @entry(CaseData)
    def fetch_case(self, case_id: YearlySpeculativeRange) -> Request:
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.court_url}/cases/{case_id.year}/{case_id.min}",
            ),
            continuation=self.parse_case_detail,
        )

At run time, supply seed parameters:

.. code-block:: json

    [
        {"fetch_case": {"case_id": {"year": 2025, "min": 1, "gap": 15}}},
        {"fetch_case": {"case_id": {"year": 2024, "min": 1, "soft_max": 4000, "gap": 0}}}
    ]

- ``min``: Starting integer ID (inclusive floor).
- ``soft_max``: Exclusive upper bound of the explicit seed range
  (``range(min, soft_max)``). IDs past it are reached only if
  ``should_advance`` is ``True``.
- ``should_advance`` (default ``True``): When ``True`` the driver opens an
  adaptive advance window past ``soft_max``. Set ``False`` for a pure
  backfill of the explicit range.
- ``gap``: Max consecutive failures before stopping speculation. Also the
  size of the initial advance window. Set ``0`` to disable the window.

When speculating, pair it with ``fails_successfully()`` to handle soft-404 responses.

The Speculative Protocol
------------------------

The built-in ``SpeculativeRange`` and ``YearlySpeculativeRange`` cover the
common cases, but you can define your own speculative parameter types for
other patterns by implementing the ``Speculative`` protocol:

.. code-block:: python

    from kent.common.speculative import Speculative

    class AlphanumericDocketRange(BaseModel):
        prefix: str
        number: int
        soft_max: int = 0
        should_advance: bool = True
        gap: int = 10

        def seed_range(self) -> range:
            return range(self.number, self.soft_max)

        def from_int(self, n: int) -> AlphanumericDocketRange:
            return AlphanumericDocketRange(
                prefix=self.prefix, number=n,
                soft_max=self.soft_max,
                should_advance=self.should_advance,
                gap=self.gap,
            )

        def max_gap(self) -> int:
            return self.gap

Any Pydantic ``BaseModel`` that structurally implements the protocol (one
``should_advance: bool`` field plus the three methods above) is
automatically detected by the ``@entry`` decorator and the driver will
run the speculation loop for it.

Soft 404 Detection
------------------

Override ``fails_successfully()`` to detect pages that return HTTP 200 but
contain error content. This is an unfortunate necessity to get Speculative
Requests working for servers that always return 200 even when search results don't exist:

.. code-block:: python

    def fails_successfully(self, response: Response) -> bool:
        return "Case Not Found" not in response.text

Nonnavigating and Archive Requests
===================================

Nonnavigating Requests
----------------------

The ``nonnavigating`` flag controls **URL resolution** for subsequent requests.
Normally, the driver tracks the "current location" and uses it to resolve
relative URLs via ``urljoin``. A nonnavigating request fetches data without
updating this location, which is appropriate for API calls or supplementary
fetches:

.. code-block:: python

    yield Request(
        request=HTTPRequestParams(method=HttpMethod.GET, url=f"{self.court_url}/api/data"),
        continuation=self.parse_api_response,
        nonnavigating=True,
    )

Archive Requests
----------------

For downloading files (PDFs, audio, images, word processor documents):

.. code-block:: python

    yield Request(
        request=HTTPRequestParams(method=HttpMethod.GET, url=file_url),
        continuation=self.handle_download,
        archive=True, # nonnavigating=True automatically
        expected_type="pdf",
    )

The ``expected_type`` should correspond to the file extension: ``"pdf"``,
``"mp3"``, ``"jpeg"``, ``"wpd"``, ``"wav"``, etc.

The step handling an archive response receives ``local_filepath`` with the
saved file's location.


Driver Requirements
===================

Scrapers declare what capabilities they need from the driver via
``driver_requirements``. The ``kent run`` CLI reads these to auto-select the
appropriate driver and browser profile.

.. code-block:: python

    from kent.data_types import DriverRequirement

    class MyCourtScraper(BaseScraper[MyData]):
        driver_requirements = [DriverRequirement.JS_EVAL, DriverRequirement.CHROME_ALIKE]

Available requirements:

- ``DriverRequirement.JS_EVAL``: Requires JavaScript evaluation. Auto-selects the Playwright driver.
- ``DriverRequirement.FF_ALIKE``: Requires a Firefox-like browser profile.
- ``DriverRequirement.CHROME_ALIKE``: Requires a Chrome-like browser profile.
- ``DriverRequirement.HCAP_HANDLER``: Requires hCaptcha interstitial handling. Auto-selects Playwright.
- ``DriverRequirement.RCAP_HANDLER``: Requires reCAPTCHA interstitial handling. Auto-selects Playwright.

``FF_ALIKE`` and ``CHROME_ALIKE`` are mutually exclusive.


Running Scrapers
================

CLI
---

.. code-block:: bash

    # List all discovered scrapers
    kent list

    # Inspect a scraper's entry points and metadata
    kent inspect my_scrapers.court:MyCourtScraper

    # Run with the persistent driver (default)
    kent run my_scrapers.court:MyCourtScraper --params '[{"search_by_date": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}}}]'

    # Run with Playwright for JS-heavy sites
    kent run my_scrapers.court:MyCourtScraper --driver playwright --headed

Drivers
-------

- **persistent** (default): SQLite-backed. Stores all requests/responses, supports resume on failure, provides a web UI for debugging. This is the driver used in production.
- **playwright**: Browser automation for JavaScript-heavy sites. Supports headed mode for debugging. Almost always selected automatically because the scraper declares a ``driver_requirements`` entry (e.g., ``JS_EVAL``, ``HCAP_HANDLER``).
- **sync**: Synchronous, in-memory. Exists primarily as a template and for testing purposes.
- **async**: Asynchronous variant of the sync driver. Also primarily for testing.


Parameter Models
================

Entry point parameters are automatically validated and coerced by the
``@entry`` decorator. The following primitive types are handled natively:

- ``int``: Integers (e.g., a case number)
- ``str``: Strings (e.g., a docket identifier)
- ``date``: Dates (parsed from ISO 8601 strings like ``"2024-03-15"``)

For structured parameters, Kent provides common Pydantic model types in
``kent.common.param_models``:

``DateRange``
    Inclusive start/end date range. Fields: ``start: date``, ``end: date``.

``SpeculativeRange``
    Sequential integer ID probing with gap-based stopping. Fields:
    ``number: int``, ``speculate: bool``, ``threshold: int``, ``gap: int``.

``YearlySpeculativeRange``
    Year-partitioned variant of ``SpeculativeRange``. Adds a ``year: int``
    field for scrapers with year-partitioned IDs.

You can also define custom Pydantic ``BaseModel`` subclasses for
scraper-specific parameters. Any ``BaseModel`` will be automatically
validated from the JSON params passed at run time.


Additional Reference
====================

.. toctree::
   :maxdepth: 2

   cli
