Step 2: NavigatingRequest - Multi-Page Scraping
================================================

In Step 1, our scraper was a simple function that parsed a single HTML page.
But real-world scrapers need to navigate between pages - from a list page to
detail pages, from search results to individual documents.

This step introduces **NavigatingRequest**, which allows scrapers to request
additional pages while the driver handles all the HTTP complexity.


Overview
--------

In this step, we introduce:

1. **ParsedData** - A type wrapper for yielded data (enables pattern matching)
2. **NavigatingRequest** - A request for the driver to fetch another page
3. **Response** - The HTTP response object passed to continuation methods
4. **Scraper as a class** - Multiple methods for different page types
5. **Continuation as string** - Method names, not function references (serializable)
6. **SyncDriver class** - Manages request queue and URL resolution


Why a Class?
------------

In Step 1, our scraper was a simple function:

.. code-block:: python

    def parse_cases(html: str) -> Generator[dict, None, None]:
        # Parse and yield data
        yield {"docket": "...", ...}

But now we need multiple methods - one for the list page, one for detail pages.
We could use module-level functions, but a class provides:

- Clear grouping of related parsing logic
- A natural place for the driver to look up continuation methods
- A protocol that the Driver can rely on for getting entry points, and more later.

The Types
---------

**ParsedData** wraps yielded data to distinguish it from requests:

.. code-block:: python

    @dataclass(frozen=True)
    class ParsedData(Generic[T]):
        data: T

        def unwrap(self) -> T:
            return self.data

**NavigatingRequest** asks the driver to GET a URL (we'll expand this next):

.. code-block:: python

    @dataclass
    class NavigatingRequest:
        url: str              # Can be relative or absolute
        continuation: str     # Method name to call with Response
        method: str = "GET"   # HTTP method
        headers: dict = field(default_factory=dict)
        data: dict | bytes | None = None  # For POST requests

**Response** mirrors the httpx Response object:

.. code-block:: python

    @dataclass
    class Response:
        status_code: int
        headers: dict[str, str]
        content: bytes       # Raw bytes
        text: str            # Decoded text
        url: str             # Final URL after redirects
        request: NavigatingRequest


Why Continuation as String?
---------------------------

The ``continuation`` field is a **string** (method name), not a function
reference. This makes requests fully serializable without pickling:

.. code-block:: python

    # This can be JSON serialized
    request = NavigatingRequest(
        url="/cases/BCC-2024-001",
        continuation="parse_detail",  # String, not self.parse_detail
    )

Serializable requests enable:

- **Database persistence** - Store pending requests for crash recovery
- **Pause/resume** - Save scrape state and continue later
- **Distributed execution** - Send requests to worker processes
- **Debugging** - Inspect the request queue as data


Pattern Matching
----------------

The driver uses Python 3.10's ``match`` statement to handle yields exhaustively:

.. code-block:: python

    from typing import assert_never

    for item in continuation_method(response):
        match item:
            case ParsedData():
                self.results.append(item.unwrap())
            case NavigatingRequest():
                self.enqueue_request(request, item)
            case None:
                pass
            case _:
                assert_never(item)

The ``assert_never(item)`` call serves two purposes: at runtime it raises an
exception if reached, and at type-check time it verifies that all cases are
handled. If you add a new yield type to ``ScraperYield`` and forget to handle
it, the type checker will report an error on the ``assert_never`` line.

This pattern ensures we handle every possible yield type. As we add more types
in later steps (NonNavigatingRequest, ArchiveRequest), the type system helps
us remember to handle them.


Data Flow
---------

.. mermaid::

    sequenceDiagram
        participant D as Driver
        participant S as Scraper
        participant H as HTTP Client

        D->>H: GET /cases
        H-->>D: Response (list HTML)
        D->>S: parse_list(response)
        S-->>D: yield NavigatingRequest(/cases/BCC-2024-001)
        S-->>D: yield NavigatingRequest(/cases/BCC-2024-002)
        S-->>D: yield NavigatingRequest(...)

        loop For each NavigatingRequest
            D->>H: GET /cases/{docket}
            H-->>D: Response (detail HTML)
            D->>S: parse_detail(response)
            S-->>D: yield ParsedData({...})
        end

        D-->>D: Return all ParsedData


URL Resolution
--------------

NavigatingRequest can use relative URLs. The driver resolves them against
the parent request's URL when enqueueing:

.. code-block:: python

    # In parse_list, responding to http://bugcourt.example.com/cases
    yield NavigatingRequest(
        url="/cases/BCC-2024-001",  # Relative URL
        continuation="parse_detail",
    )
    # Driver resolves to: http://bugcourt.example.com/cases/BCC-2024-001

The ``resolve_from`` method on NavigatingRequest creates a new request with
the URL resolved against the parent request. This uses ``urllib.parse.urljoin``
for standard URL resolution behavior. This glosses over an important case
that we'll cover in the next step.


Example: Bug Court Scraper
--------------------------

Here's the complete Step 2 scraper:

.. code-block:: python
    :caption: bug_court.py - BugCourtScraper class

    """Bug Civil Court scraper example.

    This module demonstrates the scraper-driver architecture through a fictional
    court where insects file civil lawsuits. It evolves across the 29 steps of
    the design documentation.

    Step 1: A simple function that parses HTML and yields dicts.
    Step 2: A class with multiple methods, yielding ParsedData and NavigatingRequest.
    """

    from collections.abc import Generator

    from lxml import html

    from kent.data_types import (
        BaseScraper,
        NavigatingRequest,
        ParsedData,
        Response,
        ScraperYield,
    )


    class BugCourtScraper(BaseScraper[dict]):
        """Scraper for the Bug Civil Court.

        This Step 2 implementation demonstrates:
        - A scraper as a class (to bundle multiple methods)
        - Yielding NavigatingRequest to fetch detail pages
        - Yielding ParsedData with complete case information
        - Continuation methods specified by name (for serializability)

        The scraper visits two types of pages:
        1. List page (/cases) - contains basic case info and links to details
        2. Detail page (/cases/{docket}) - contains full case information
        """

        BASE_URL = "http://bugcourt.example.com"

        def get_entry(self) -> NavigatingRequest:
            """Create the initial request to start scraping."""
            return NavigatingRequest(
                url=f"{self.BASE_URL}/cases", continuation="parse_list"
            )

        def parse_list(
            self, response: Response
        ) -> Generator[ScraperYield[dict], None, None]:
            """Parse the case list page and yield requests for detail pages."""
            tree = html.fromstring(response.text)
            case_rows = tree.xpath("//tr[@class='case-row']")

            for row in case_rows:
                docket = _get_text(row, ".//td[@class='docket']")
                if docket:
                    # The URL is relative - driver will resolve against parent request
                    yield NavigatingRequest(
                        url=f"/cases/{docket}",
                        continuation="parse_detail",
                    )

        def parse_detail(
            self, response: Response
        ) -> Generator[ScraperYield[dict], None, None]:
            """Parse a case detail page and yield the complete case data."""
            tree = html.fromstring(response.text)

            yield ParsedData(
                {
                    "docket": _get_text_by_id(tree, "docket"),
                    "case_name": _get_text(tree, "//h2"),
                    "plaintiff": _get_text_by_id(tree, "plaintiff"),
                    "defendant": _get_text_by_id(tree, "defendant"),
                    "date_filed": _get_text_by_id(tree, "date-filed"),
                    "case_type": _get_text_by_id(tree, "case-type"),
                    "status": _get_text_by_id(tree, "status"),
                    "judge": _get_text_by_id(tree, "judge"),
                    "summary": _get_text_by_id(tree, "summary"),
                }
            )


    def _get_text(element, xpath: str) -> str:
        """Extract text content from an xpath query."""
        results = element.xpath(xpath)
        if results:
            return results[0].text_content().strip()
        return ""


    def _get_text_by_id(tree, element_id: str) -> str:
        """Extract text content from an element by its ID."""
        return _get_text(tree, f"//*[@id='{element_id}']")


The Driver Run Loop
-------------------

The driver's ``run()`` method implements the core execution loop. It processes
requests from a queue, calling continuation methods and handling their yields:

.. code-block:: python
    :caption: sync_driver.py - SyncDriver run() method

    def run(self) -> list[ScraperReturnDatatype]:
        """Run the scraper starting from the scraper's entry point."""
        self.results = []
        self.request_queue = [self.scraper.get_entry()]

        while self.request_queue:
            request: NavigatingRequest = self.request_queue.pop(0)
            response: Response = self.resolve_request(request)
            continuation_method: Callable[
                [Response],
                Generator[ScraperYield[ScraperReturnDatatype], None, None],
            ] = self.scraper.get_continuation(request.continuation)
            for item in continuation_method(response):
                match item:
                    case ParsedData():
                        self.results.append(item.unwrap())
                    case NavigatingRequest():
                        self.enqueue_request(request, item)
                    case None:
                        pass
                    case _:
                        assert_never(item)

        return self.results



The key points of the run loop:

1. **Queue initialization**: The loop starts by getting the entry request from
   the scraper via ``get_entry()``.

2. **Request processing**: Each request is fetched via ``resolve_request()``,
   which uses httpx to make the actual HTTP call.

3. **Continuation lookup**: The driver looks up the continuation method by name
   using ``get_continuation()``, which returns a properly-typed callable.

4. **Yield handling**: The match statement exhaustively handles all possible
   yield types from the scraper generator.

5. **URL resolution**: When a new NavigatingRequest is yielded, ``enqueue_request()``
   resolves its URL against the parent request before adding it to the queue.


Example: Using the Driver
-------------------------

.. code-block:: python

    from kent.driver.sync_driver import SyncDriver
    from tests.scraper_driver.scraper.example.bug_court import BugCourtScraper

    # Create scraper and driver
    scraper = BugCourtScraper()
    driver = SyncDriver(scraper)

    # Run the scraper (entry URL comes from scraper.get_entry())
    results = driver.run()

    # results is a list of dicts, one per case
    for case in results:
        print(f"{case['docket']}: {case['case_name']}")


What's Next
-----------

In :doc:`03_nonnavigating_request`, we introduce **NonNavigatingRequest** -
for API calls that fetch supplementary data without changing the current
location. This is useful for JSON APIs that provide additional metadata.
