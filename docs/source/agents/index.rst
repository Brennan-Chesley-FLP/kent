======
AGENTS
======

.. note::

    This document is an LLM-optimized reference for building Kent scrapers.
    It is designed to be consumed as context by an AI coding assistant so that
    it can produce working scrapers without repeatedly consulting the codebase.

Quick-Reference Cheat Sheet
============================

Imports
-------

.. code-block:: python

    from __future__ import annotations

    from collections.abc import Generator
    from dataclasses import replace

    from pyrate_limiter import Duration, Rate

    from kent.common.decorators import entry, step
    from kent.common.exceptions import TransientException
    from kent.common.lxml_page_element import LxmlPageElement
    from kent.common.param_models import DateRange, SpeculativeRange, YearlySpeculativeRange
    from kent.data_types import (
        BaseScraper,
        DriverRequirement,
        EstimateData,
        HttpMethod,
        HTTPRequestParams,
        ParsedData,
        Request,
        Response,
        ScraperStatus,
        ScraperYield,
        SkipDeduplicationCheck,
        WaitForLoadState,
        WaitForSelector,
    )


Scraper Skeleton
----------------

.. code-block:: python

    from __future__ import annotations
    from collections.abc import Generator
    from pyrate_limiter import Duration, Rate

    from kent.common.decorators import entry, step
    from kent.common.lxml_page_element import LxmlPageElement
    from kent.data_types import (
        BaseScraper, DriverRequirement, EstimateData, HttpMethod,
        HTTPRequestParams, ParsedData, Request, Response,
        ScraperStatus, ScraperYield,
    )
    from .models import CaseData  # your data model

    class MyCourtScraper(BaseScraper[CaseData]):
        court_url = "https://example.court.gov"
        court_ids = {"example.court"}
        data_types = {"dockets"}  # or {"opinions"}, {"dockets", "oral_arguments"}
        status = ScraperStatus.IN_DEVELOPMENT
        version = "2026-01-01"
        rate_limits = [Rate(1, Duration.SECOND)]

        # ── Entry points ──

        @entry(CaseData)
        def search(self) -> Generator[Request, None, None]:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.court_url}/cases",
                ),
                continuation=self.parse_list,
            )

        # ── Steps ──

        @step
        def parse_list(
            self, page: LxmlPageElement,
        ) -> Generator[ScraperYield, None, None]:
            links = page.find_links("//a[@class='case']", "case links")
            for link in links:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=link.url,
                    ),
                    continuation=self.parse_detail,
                )

        @step
        def parse_detail(
            self, page: LxmlPageElement, accumulated_data: dict,
        ) -> Generator[ScraperYield, None, None]:
            yield ParsedData(CaseData.raw(
                request_url=page.query_xpath(
                    "//*[@id='docket']", "docket",
                    min_count=1, max_count=1,
                )[0].text_content().strip(),
                docket=page.query_xpath(
                    "//*[@id='docket']", "docket",
                    min_count=1, max_count=1,
                )[0].text_content().strip(),
                # ... more fields
            ))


Data Model Template
-------------------

.. code-block:: python

    from __future__ import annotations
    from datetime import date
    from pydantic import Field
    from kent.common.data_models import ScrapedData

    class CaseData(ScrapedData):
        docket: str = Field(..., description="Docket number")
        case_name: str = Field(..., description="Full case name")
        date_filed: date = Field(..., description="Filing date")
        status: str = Field(..., description="Current case status")

Rules:

- Extend ``ScrapedData`` (not ``BaseModel``)
- Use ``Field(...)`` for required fields with descriptions
- ``ScrapedData`` inherits from SQLModel, so field types must be SQLModel-compatible
- All data is kept even when validation fails -- useful for debugging


API Reference
=============

@entry Decorator
----------------

Marks a method as a scraper entry point. Entry points parameterize runs --
they specify *what* to scrape (a date range, a docket number range, etc.)
and yield requests for publicly accessible URLs.

.. code-block:: python

    @entry(ReturnDataType)
    def method_name(self, param: ParamType) -> Request | Generator[Request, None, None]:

- Argument: the data type this entry produces
- Parameter types: ``int``, ``str``, ``date``, Pydantic ``BaseModel``, ``DateRange``, ``SpeculativeRange``, ``YearlySpeculativeRange``
- Return: single ``Request`` or ``Generator[Request, None, None]``
- A scraper can have multiple ``@entry`` methods

@step Decorator
---------------

Marks a method as a response handler.

.. code-block:: python

    @step
    def method_name(self, ...) -> Generator[ScraperYield, None, None]:

    @step(priority=5, encoding="latin-1")
    def method_name(self, ...) -> Generator[ScraperYield, None, None]:

Options:

- ``priority``: int (default 9, lower = higher priority)
- ``encoding``: str (default ``"utf-8"``)
- ``xsd``: str | None -- path to XSD schema for structural validation hints
- ``json_model``: str | None -- dotted path to Pydantic model for JSON response validation (e.g. ``"api.responses.SearchResult"``)
- ``auto_await_timeout``: int | None -- timeout in ms for Playwright autowait retry logic

Step parameters are **auto-injected by name**. Include only what you need:

======================  ====================  ===============================================
Parameter               Type                  Description
======================  ====================  ===============================================
``page``                LxmlPageElement       Parsed HTML with count-validated queries
``response``            Response              HTTP response (status_code, headers, url, text)
``json_content``        any                   Auto-parsed JSON from response body
``text``                str                   response.text
``accumulated_data``    dict                  Context carried through request chain
``local_filepath``      str | None            Local path for archive responses
``lxml_tree``           CheckedHtmlElement    Raw lxml tree (prefer ``page`` instead)
``request``             BaseRequest           The current request object
======================  ====================  ===============================================

Wait Lists (Playwright)
^^^^^^^^^^^^^^^^^^^^^^^

For sites requiring JavaScript, wait lists tell the Playwright driver when
the DOM is ready to be captured and handed to the scraper:

.. code-block:: python

    from kent.data_types import WaitForSelector, WaitForLoadState

    @step(await_list=[
        WaitForLoadState(state="networkidle"),
        WaitForSelector("//table[@id='results']"),
    ])
    def parse_results(self, page: LxmlPageElement) -> Generator[ScraperYield, None, None]:
        ...

Available: ``WaitForSelector(selector, state="visible")``,
``WaitForLoadState(state="load")``, ``WaitForURL(url)``,
``WaitForTimeout(timeout_ms)``. HTTP drivers ignore ``await_list``.

Request
-------

.. code-block:: python

    Request(
        request=HTTPRequestParams(method=HttpMethod.GET, url="..."),
        continuation=self.step_method,          # @step method reference
        accumulated_data={"key": "value"},      # context forwarding
        nonnavigating=True,                     # API call (doesn't update urljoin location)
        archive=True,                           # file download
        expected_type="pdf",                    # file extension: pdf, mp3, jpeg, wpd, wav
        priority=5,                             # queue priority (lower = first)
        permanent={"headers": {"Auth": "..."}}, # persistent through chain
        deduplication_key="case-123",           # custom dedup key (auto-generated if None)
    )

``nonnavigating`` controls URL resolution: the driver tracks a "current location"
for resolving relative URLs via ``urljoin``. Nonnavigating requests fetch data
without updating that location.

``deduplication_key`` prevents the driver from visiting the same resource twice.
By default, dedup keys are auto-generated from the URL. Set a custom key when
overlapping searches might yield the same case (e.g., ``deduplication_key=docket_id``).
Use ``SkipDeduplicationCheck()`` to disable dedup for requests like pagination
that must always execute.

HTTPRequestParams
-----------------

.. code-block:: python

    HTTPRequestParams(
        method=HttpMethod.GET,    # GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS
        url="https://...",
        params={"q": "search"},   # query string parameters
        data={"field": "value"},  # form body (POST)
        json={"key": "value"},    # JSON body (POST)
        headers={"Accept": "application/json"},
        cookies={"session": "abc"},
        timeout=30.0,
    )


PageElement API (``page``)
--------------------------

.. code-block:: python

    # Query elements -- returns List[LxmlPageElement]
    elements = page.query_xpath("//div[@class='item']", "items", min_count=1, max_count=10)
    elements = page.query_css("div.item", "items", min_count=0)

    # Query strings -- returns List[str]
    texts = page.query_xpath_strings("//td/text()", "cell text", min_count=1)
    texts = page.query_css_strings("td::text", "cell text")

    # Text content of an element
    text = element.text_content().strip()

    # Get attribute
    href = element.get_attribute("href")

    # Find links -- returns List[Link] with .url and .text
    # Automatically attaches ViaLink for Playwright browser replay
    links = page.find_links("//a[@class='detail']", "detail links", min_count=1)
    link.url   # resolved URL
    link.text  # link text

    # Find and submit a form
    # Captures hidden fields; attaches ViaFormSubmit for Playwright browser replay
    form = page.find_form("//form[@id='search']", "search form")
    request = form.submit(data={"field": "value"})
    yield replace(request, continuation=self.parse_results)

All query methods validate count bounds. Out-of-bounds raises
``HTMLStructuralAssumptionException`` (handled by the driver).

Default ``min_count`` is 1. Set ``min_count=0`` for optional elements.

``find_links`` and ``find_form``/``submit()`` attach ``ViaLink`` and
``ViaFormSubmit`` descriptors so the Playwright driver can click links and
submit forms in the browser, reproducing JavaScript behavior. HTTP drivers
ignore these descriptors.


Yielding Results
----------------

Always wrap data in ``ParsedData`` and use ``.raw()`` for deferred validation:

.. code-block:: python

    yield ParsedData(
        CaseData.raw(
            request_url=response.url,
            docket="2024-001",
            case_name="Smith v. Jones",
        )
    )


Patterns
========

Accumulated Data (Context Forwarding)
--------------------------------------

Extract data from a list page and carry it to detail pages:

.. code-block:: python

    @step
    def parse_list(self, page):
        for row in page.query_xpath("//tr", "rows"):
            docket = row.query_xpath_strings(".//td[1]/text()", "docket")[0]
            links = row.find_links(".//a", "link", min_count=1, max_count=1)
            yield Request(
                request=HTTPRequestParams(method=HttpMethod.GET, url=links[0].url),
                continuation=self.parse_detail,
                accumulated_data={"docket": docket.strip()},
            )

    @step
    def parse_detail(self, page, accumulated_data):
        yield ParsedData(CaseData.raw(
            request_url="",
            docket=accumulated_data["docket"],
            case_name=page.query_xpath("//h1", "title", max_count=1)[0].text_content().strip(),
        ))

Each request gets a deep copy of ``accumulated_data`` -- safe to mutate.

Values must be JSON-serializable (str, int, float, bool, None, list, dict)
because ``accumulated_data`` is persisted between requests. For Pydantic models,
use ``.model_dump(mode="json")`` before storing; for dates, use ``.isoformat()``.

JSON API Parsing
----------------

.. code-block:: python

    @entry(JusticeData)
    def get_justices(self) -> Generator[Request, None, None]:
        yield Request(
            request=HTTPRequestParams(method=HttpMethod.GET, url=f"{self.court_url}/api/justices"),
            continuation=self.parse_justices,
            nonnavigating=True,
        )

    @step
    def parse_justices(self, json_content: list) -> Generator[ScraperYield, None, None]:
        for item in json_content:
            yield ParsedData(JusticeData.raw(
                request_url="",
                name=item["name"],
                title=item["title"],
            ))

File Downloads (Archive)
------------------------

.. code-block:: python

    yield Request(
        request=HTTPRequestParams(method=HttpMethod.GET, url=pdf_url),
        continuation=self.handle_pdf,
        archive=True,
        expected_type="pdf",  # file extension: pdf, mp3, jpeg, wpd, wav
        accumulated_data={"docket": docket},
    )

    @step
    def handle_pdf(self, accumulated_data, local_filepath):
        yield ParsedData(OpinionData.raw(
            request_url="",
            docket=accumulated_data["docket"],
            download_url=pdf_url,
            local_path=local_filepath,
        ))

Form Submission
---------------

.. code-block:: python

    from dataclasses import replace

    @step
    def submit_search(self, page, accumulated_data):
        form = page.find_form("//form[@id='search']", "search form")
        request = form.submit(data={
            "from_date": accumulated_data["from_date"],
            "to_date": accumulated_data["to_date"],
        })
        yield replace(request, continuation=self.parse_results)

``form.submit()`` accepts ``**request_kwargs`` that are passed to the
``Request`` constructor, so you can pass ``continuation``,
``accumulated_data``, ``deduplication_key``, etc. directly:

.. code-block:: python

    yield form.submit(
        data={"field": "value"},
        continuation=self.parse_results,
        accumulated_data=accumulated_data,
    )

Or use ``dataclasses.replace`` on the returned Request (equivalent):

.. code-block:: python

    request = form.submit(data={"field": "value"})
    yield replace(request, continuation=self.parse_results)

``find_form`` and ``submit()`` capture hidden fields and default values,
producing a request that works with both HTTP and Playwright drivers.

Speculative Case Discovery
---------------------------

There are two speculation modes: simple (integer) and yearly (year + number).
This can be expanded easily via the Speculation Protocol. These are for sites
where we can't search by a date of some sort and need to get coverage in some other way.


The driver supports ``SpeculativeRange`` as an entry parameter type,
which provides ``.number``:

.. code-block:: python

    @entry(CaseData)
    def fetch_case(self, rid: SpeculativeRange) -> Request:
        return self._make_search_request(rid.number)

**Yearly speculation** -- for year-partitioned IDs (e.g., ``2024-00003``):

.. code-block:: python

    @entry(CaseData)
    def fetch_case(self, case_id: YearlySpeculativeRange) -> Request:
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.court_url}/cases/{case_id.year}/{case_id.number}",
            ),
            continuation=self.parse_case,
        )

    def fails_successfully(self, response: Response) -> bool:
        """Detect soft-404 pages (200 status but error content)."""
        return "Case Not Found" not in response.text

Seed params at runtime:

.. code-block:: json

    [{"fetch_case": {"case_id": {"year": 2025, "min": 1, "gap": 15}}}]

- ``min``: starting integer ID (inclusive floor).
- ``soft_max``: exclusive upper bound of the seed range
  (``range(min, soft_max)``); omit for pure adaptive probing.
- ``should_advance`` (default ``True``): whether the driver pushes past
  ``soft_max``. Set ``False`` for backfills.
- ``gap``: max consecutive failures before stopping; also the initial
  advance-window size. ``0`` disables the window.

For patterns beyond integer/year+int, implement the ``Speculative`` protocol
on a custom Pydantic ``BaseModel`` (one ``should_advance: bool`` field plus
three methods: ``seed_range``, ``from_int``, ``max_gap``).

Soft-404 Detection
------------------

Override ``fails_successfully()`` on the scraper class. Return ``True`` if the
response is genuinely successful, ``False`` if it's a hidden error:

.. code-block:: python

    def fails_successfully(self, response: Response) -> bool:
        if "No records found" in response.text:
            return False
        if "session expired" in response.text.lower():
            return False
        return True

Transient Error Detection
-------------------------

When a server returns a degraded or error page with a 200 status, raise
``TransientException`` so the driver retries the request:

.. code-block:: python

    from kent.common.exceptions import TransientException

    @step
    def parse_detail(self, page, text):
        if "Service Temporarily Unavailable" in text:
            raise TransientException("Server returned maintenance page")
        ...

EstimateData (Integrity Checks)
-------------------------------

When a search page reports a total result count (e.g., "355 results found"),
yield ``EstimateData`` once with that total for post-hoc verification. Do not
emit per-page counts -- only the overall total:

.. code-block:: python

    total = int(page.query_xpath_strings("//span[@class='total']/text()", "total")[0])
    yield EstimateData(expected_types=(CaseData,), min_count=total, max_count=total)


Driver Requirements (Playwright)
---------------------------------

When a site needs a JavaScript-capable browser, declare ``driver_requirements``
on the scraper class:

.. code-block:: python

    from kent.data_types import DriverRequirement

    class MyScraper(BaseScraper[MyData]):
        driver_requirements = [DriverRequirement.JS_EVAL, DriverRequirement.FF_ALIKE]

Available values:

- ``JS_EVAL`` -- requires JavaScript evaluation (Playwright)
- ``FF_ALIKE`` -- requires Firefox-like browser profile
- ``CHROME_ALIKE`` -- requires Chrome-like browser profile
- ``HCAP_HANDLER`` -- requires hCaptcha handler
- ``RCAP_HANDLER`` -- requires reCAPTCHA handler

Most scrapers do **not** need Playwright. Only add driver requirements when the
site has bot protection (CloudFlare, Akamai) or is a JavaScript SPA.


Pagination
----------

For HTML pagination, follow "Next" links:

.. code-block:: python

    @step
    def parse_results(self, page, accumulated_data):
        for row in page.query_xpath("//tr[@class='result']", "result rows"):
            yield Request(...)

        next_links = page.find_links(
            "//a[contains(text(), 'Next')]", "next page",
            min_count=0, max_count=1,
        )
        if next_links:
            yield Request(
                request=HTTPRequestParams(method=HttpMethod.GET, url=next_links[0].url),
                continuation=self.parse_results,
                accumulated_data=accumulated_data,
                deduplication_key=SkipDeduplicationCheck(),  # pagination must always execute
            )

For API pagination with page numbers:

.. code-block:: python

    @step
    def handle_search(self, json_content, accumulated_data):
        for item in json_content["results"]:
            yield Request(...)

        current_page = accumulated_data.get("page", 0)
        total_pages = json_content["totalPages"]
        if current_page + 1 < total_pages:
            accumulated_data["page"] = current_page + 1
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.court_url}/api/search",
                    params={"page": str(current_page + 1)},
                ),
                continuation=self.handle_search,
                accumulated_data=accumulated_data,
                deduplication_key=SkipDeduplicationCheck(),
            )


Exception Types
===============

You generally do not catch these -- the driver handles them.

- ``HTMLStructuralAssumptionException``: XPath/CSS count mismatch (website structure changed)
- ``DataFormatAssumptionException``: Pydantic validation failed on scraped data
- ``TransientException``: Base for retryable errors (raise manually for unreliable servers)
- ``HTMLResponseAssumptionException``: Unexpected HTTP status code
- ``RequestTimeoutException``: Request timeout


Running
=======

.. code-block:: bash

    # List scrapers
    kent list

    # Inspect metadata and entry points
    kent inspect module.path:ClassName

    # Run with persistent driver (default, SQLite-backed, resumable)
    kent run module.path:ClassName --params '[{"entry_name": {"param": "value"}}]'

    # Run with Playwright (JS sites), headed for debugging
    kent run module.path:ClassName --driver playwright --headed --params '...'


Complete Example
================

A full scraper with multiple entry points, HTML parsing, JSON API, form
submission, file archiving, and speculative discovery. This is the demo
scraper that ships with Kent:

.. code-block:: python

    from __future__ import annotations
    from collections.abc import Generator
    from dataclasses import replace
    from pyrate_limiter import Duration, Rate

    from kent.common.decorators import entry, step
    from kent.common.lxml_page_element import LxmlPageElement
    from kent.common.param_models import DateRange, YearlySpeculativeRange
    from kent.data_types import (
        BaseScraper, EstimateData, HttpMethod, HTTPRequestParams,
        ParsedData, Request, Response, ScraperYield,
    )

    # Data models (in models.py)
    # class CaseData(ScrapedData):
    #     docket: str = Field(...)
    #     case_name: str = Field(...)
    #     ...

    DemoData = CaseData | JusticeData | OralArgumentData | OpinionData

    class BugCourtDemoScraper(BaseScraper[DemoData]):
        court_url = "http://127.0.0.1:8080"
        rate_limits = [Rate(1, Duration.SECOND)]

        # ── Entry: speculative case discovery ──

        @entry(CaseData)
        def fetch_case(self, case_id: YearlySpeculativeRange) -> Request:
            return Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.court_url}/cases/{case_id.year}/{case_id.number}",
                ),
                continuation=self.parse_case_detail,
            )

        # ── Entry: oral arguments list ──

        @entry(OralArgumentData)
        def get_oral_arguments(self) -> Generator[Request, None, None]:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.court_url}/oral-arguments",
                ),
                continuation=self.parse_oral_arguments_list,
            )

        # ── Entry: JSON API ──

        @entry(JusticeData)
        def get_justices(self) -> Generator[Request, None, None]:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.court_url}/api/justices",
                ),
                continuation=self.parse_justices_json,
                nonnavigating=True,
            )

        # ── Entry: form-based search ──

        @entry(CaseData)
        def cases_by_date_filed(self, date_range: DateRange) -> Generator[Request, None, None]:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.court_url}/cases/search",
                ),
                continuation=self.submit_date_search,
                accumulated_data={
                    "from_date": date_range.start.isoformat(),
                    "to_date": date_range.end.isoformat(),
                },
            )

        # ── Soft-404 detection ──

        def fails_successfully(self, response: Response) -> bool:
            return "Case Not Found" not in response.text

        # ── Steps ──

        @step
        def parse_case_detail(self, page: LxmlPageElement):
            containers = page.query_xpath(
                "//div[@class='case-details']", "case details",
                min_count=0, max_count=1,
            )
            if not containers:
                return  # soft-404

            docket = page.query_xpath(
                "//*[@id='docket']", "docket", min_count=1, max_count=1
            )[0].text_content().strip()
            case_name = page.query_xpath(
                "//h2", "title", min_count=1, max_count=1
            )[0].text_content().strip()

            yield ParsedData(CaseData.raw(
                request_url="",
                docket=docket,
                case_name=case_name,
                # ... remaining fields ...
            ))

            # Follow opinion link
            opinion_links = page.find_links(
                "//a[@class='opinion-link']", "opinion link",
                min_count=0, max_count=1,
            )
            if opinion_links:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=opinion_links[0].url,
                    ),
                    continuation=self.parse_opinion_detail,
                    accumulated_data={"docket": docket, "case_name": case_name},
                )

        @step
        def parse_justices_json(self, json_content: list):
            yield EstimateData(
                expected_types=(JusticeData,),
                min_count=len(json_content),
                max_count=len(json_content),
            )
            for j in json_content:
                yield ParsedData(JusticeData.raw(
                    request_url="",
                    name=j["name"],
                    title=j["title"],
                    # ...
                ))

        @step
        def submit_date_search(self, page: LxmlPageElement, accumulated_data: dict):
            form = page.find_form("//form[@id='date-search']", "date search form")
            request = form.submit(data={
                "from_date": accumulated_data["from_date"],
                "to_date": accumulated_data["to_date"],
            })
            yield replace(request, continuation=self.parse_case_search_results)

        @step
        def parse_opinion_detail(self, page: LxmlPageElement, accumulated_data: dict):
            image_links = page.find_links(
                "//a[@class='opinion-image-link']", "image link",
                min_count=1, max_count=1,
            )
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET, url=image_links[0].url,
                ),
                continuation=self.archive_image,
                archive=True,
                expected_type="image",
                accumulated_data={**accumulated_data, "image_url": image_links[0].url},
            )

        @step
        def archive_image(self, accumulated_data: dict, local_filepath: str):
            yield ParsedData(OpinionData.raw(
                request_url="",
                docket=accumulated_data["docket"],
                case_name=accumulated_data["case_name"],
                image_url=accumulated_data["image_url"],
                local_path=local_filepath,
            ))
