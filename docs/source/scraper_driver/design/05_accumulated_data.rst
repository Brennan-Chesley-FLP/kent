Step 5: Accumulated Data - Data Flow Across Requests
=====================================================

In Step 4, our scraper downloaded and archived files using archive requests.
But what if we need to collect information from multiple pages and combine
it into a single result? This is where **accumulated_data** comes in.

This step introduces accumulated_data for flowing data through request chains,
with deep copy semantics to prevent mutation bugs.


Overview
--------

In this step, we introduce:

1. **accumulated_data** - Field on Request for flowing data across pages
2. **Deep copy semantics** - Prevent unintended sharing between sibling requests
3. **Request chain propagation** - Data flows automatically through resolve_from
4. **Multi-court data flow** - Example: appeals court → trial court aggregation
5. **No driver changes** - Data flows purely through request resolution


Why Accumulated Data?
---------------------

Consider this scenario: You're scraping appeals court opinions that reference
the original trial court case. You want to:

1. Scrape the appeals court list page (extract case_name)
2. Navigate to the appeals detail page (extract judge, date, trial docket)
3. Navigate to the trial court page (extract plaintiff, defendant)
4. Combine all this data into a single result

Without accumulated_data, you'd have to:

.. code-block:: python

    # Store data somewhere external to requests
    case_data = {}  # Global state - bad!

    def parse_appeals_detail(self, response: Response):
        case_data["appeals_judge"] = extract_judge(response)  # Mutation!
        yield Request(url=trial_url, continuation="parse_trial")

    def parse_trial(self, response: Response):
        # How do we know which case_data this belongs to?
        # What if multiple requests are in flight?
        case_data["trial_judge"] = extract_judge(response)
        yield ParsedData(case_data)  # Race conditions!

With accumulated_data, each request carries its own data:

.. code-block:: python

    def parse_appeals_list(self, response: Response):
        # Start with case_name from list page
        yield Request(
            url=appeals_url,
            continuation="parse_appeals_detail",
            accumulated_data={"case_name": case_name}
        )

    def parse_appeals_detail(self, response: Response):
        # Get existing data, add to it
        data = response.request.accumulated_data.copy()
        data["appeals_judge"] = extract_judge(response)

        yield Request(
            url=trial_url,
            continuation="parse_trial",
            accumulated_data=data
        )

    def parse_trial(self, response: Response):
        # Get all accumulated data
        data = response.request.accumulated_data.copy()
        data["trial_judge"] = extract_judge(response)

        # Yield complete combined data
        yield ParsedData(data)


The accumulated_data Field
---------------------------

Request gets a new field:

.. code-block:: python

    @dataclass(frozen=True)
    class Request:
        """Unified request type.

        Attributes:
            request: The HTTP request parameters.
            continuation: Name of the continuation method to call.
            current_location: The current URL (for relative URL resolution).
            previous_requests: Ancestry chain of requests.
            accumulated_data: Data flowing through the request chain.
        """
        request: HTTPRequestParams
        continuation: str
        current_location: str = ""
        previous_requests: list[Request] = field(default_factory=list)
        accumulated_data: dict[str, Any] = field(default_factory=dict)

        def __post_init__(self) -> None:
            """Deep copy accumulated_data to prevent unintended sharing.

            This is critical! Without deep copy, sibling requests would share
            the same dict and modifications would leak across requests.
            """
            object.__setattr__(
                self, "accumulated_data", deepcopy(self.accumulated_data)
            )

The ``__post_init__`` method uses ``object.__setattr__`` because the dataclass
is frozen. This is the only way to modify a frozen dataclass field after
initialization.


Deep Copy Semantics - Don't step on toes
----------------------------------------

**Why deep copy?** Consider this bug scenario:

.. code-block:: python

    # WITHOUT deep copy (buggy!):
    shared_data = {"metadata": {"court": "trial"}}

    request1 = Request(
        url="/case1",
        continuation="parse",
        accumulated_data=shared_data
    )

    request2 = Request(
        url="/case2",
        continuation="parse",
        accumulated_data=shared_data
    )

    # If we mutate request1's nested dict:
    request1.accumulated_data["metadata"]["court"] = "appeals"

    # BUG: request2's data also changes!
    assert request2.accumulated_data["metadata"]["court"] == "appeals"  # OOPS!

**With deep copy** (correct behavior):

.. code-block:: python

    # WITH deep copy (correct!):
    shared_data = {"metadata": {"court": "trial"}}

    request1 = Request(
        url="/case1",
        continuation="parse",
        accumulated_data=shared_data
    )
    # __post_init__ deep copies shared_data

    request2 = Request(
        url="/case2",
        continuation="parse",
        accumulated_data=shared_data
    )
    # __post_init__ deep copies shared_data again

    # Mutate shared_data:
    shared_data["metadata"]["court"] = "appeals"

    # Requests are unaffected - they have independent deep copies
    assert request1.accumulated_data["metadata"]["court"] == "trial"  # ✓
    assert request2.accumulated_data["metadata"]["court"] == "trial"  # ✓

This prevents a whole class of subtle mutation bugs where sibling requests
accidentally share data.


Request Chain Propagation
--------------------------

The accumulated_data flows automatically through to the next method in the continuation chain
when we hand it the Request.


Data Flow Diagram
-----------------

.. mermaid::

    sequenceDiagram
        participant S as Scraper
        participant D as Driver
        participant H as HTTP Client

        Note over D: Entry request with empty accumulated_data

        D->>H: GET /appeals
        H-->>D: Response (appeals list HTML)
        D->>S: parse_appeals_list(response)
        Note over S: Extract case_name from HTML
        S-->>D: yield Request(<br/>url=/appeals/BCA-2024-001,<br/>accumulated_data={case_name: "Butterfly v. Caterpillar"})

        Note over D: resolve_from propagates accumulated_data

        D->>H: GET /appeals/BCA-2024-001
        H-->>D: Response (appeals detail HTML)
        D->>S: parse_appeals_detail(response)
        Note over S: Get accumulated_data from response.request<br/>Add appeals_judge, trial_docket
        S-->>D: yield Request(<br/>url=/cases/BCC-2024-002,<br/>accumulated_data={<br/>  case_name: "Butterfly v. Caterpillar",<br/>  appeals_judge: "Judge Honeybee",<br/>  appeals_docket: "BCA-2024-001"<br/>})

        Note over D: resolve_from propagates accumulated_data

        D->>H: GET /cases/BCC-2024-002
        H-->>D: Response (trial court HTML)
        D->>S: parse_trial_court(response)
        Note over S: Get accumulated_data from response.request<br/>Add trial_judge, plaintiff, defendant
        S-->>D: yield ParsedData({<br/>  case_name: "Butterfly v. Caterpillar",<br/>  appeals_judge: "Judge Honeybee",<br/>  appeals_docket: "BCA-2024-001",<br/>  trial_judge: "Judge Mantis",<br/>  trial_docket: "BCC-2024-002",<br/>  plaintiff: "Butterfly",<br/>  defendant: "Caterpillar"<br/>})

        D-->>D: Return all ParsedData


Example: Bug Court Scraper with Accumulated Data
-------------------------------------------------

Here's a complete scraper demonstrating accumulated_data across three pages:

.. code-block:: python
    :caption: bug_court_accumulated_data.py

    """Bug Appeals Court scraper demonstrating accumulated_data."""

    from collections.abc import Generator

    from lxml import html

    from kent.data_types import (
        BaseScraper,
        HttpMethod,
        HTTPRequestParams,
        Request,
        ParsedData,
        Response,
        ScraperYield,
    )


    class BugCourtScraperWithAccumulatedData(BaseScraper[dict]):
        """Scraper for Bug Appeals Court demonstrating accumulated_data.

        This scraper visits three types of pages:
        1. Appeals list page (/appeals) - extracts case_name
        2. Appeals detail page (/appeals/{docket}) - extracts trial court docket
        3. Trial court page (/cases/{docket}) - enriches with trial court data

        The accumulated_data flows through all three pages, collecting
        information at each step before yielding the final combined result.
        """

        BASE_URL = "http://bugcourt.example.com"

        def get_entry(self) -> Request:
            """Create the initial request to start scraping."""
            return Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.BASE_URL}/appeals",
                ),
                continuation="parse_appeals_list",
            )

        def parse_appeals_list(
            self, response: Response
        ) -> Generator[ScraperYield[dict], None, None]:
            """Parse the appeals list page and extract case_name.

            This method demonstrates starting the accumulated_data flow.
            Each case gets its case_name added to accumulated_data.
            """
            tree = html.fromstring(response.text)
            case_rows = tree.xpath("//tr[@class='case-row']")

            for row in case_rows:
                docket = _get_text(row, ".//td[@class='docket']")
                case_name = _get_text(row, ".//td/a")

                if docket:
                    # Start accumulated_data with case_name from list page
                    yield Request(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"/appeals/{docket}",
                        ),
                        continuation="parse_appeals_detail",
                        accumulated_data={"case_name": case_name},
                    )

        def parse_appeals_detail(
            self, response: Response
        ) -> Generator[ScraperYield[dict], None, None]:
            """Parse appeals detail page and extract trial court docket.

            This method enriches accumulated_data with appeals court info,
            then navigates to the trial court to gather more data.
            """
            tree = html.fromstring(response.text)

            # Get accumulated_data from the request
            # Make a copy to avoid mutating the original
            data = response.request.accumulated_data.copy()

            # Enrich with appeals court data
            data["appeals_docket"] = _get_text_by_id(tree, "docket")
            data["appeals_judge"] = _get_text_by_id(tree, "judge")
            data["appeals_date_filed"] = _get_text_by_id(tree, "date-filed")

            # Get trial court docket link
            trial_court_docket = _get_text_by_id(tree, "trial-court-docket")
            # Extract just the docket number from "Trial Court Case: BCC-2024-XXX"
            if ":" in trial_court_docket:
                trial_court_docket = trial_court_docket.split(":")[-1].strip()

            # Navigate to trial court with accumulated data
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"/cases/{trial_court_docket}",
                ),
                continuation="parse_trial_court",
                accumulated_data=data,
            )

        def parse_trial_court(
            self, response: Response
        ) -> Generator[ScraperYield[dict], None, None]:
            """Parse trial court page and yield complete combined data.

            This method receives accumulated_data from the appeals pages
            and enriches it with trial court information before yielding.
            """
            tree = html.fromstring(response.text)

            # Get accumulated_data and make a copy
            data = response.request.accumulated_data.copy()

            # Enrich with trial court data
            data["trial_docket"] = _get_text_by_id(tree, "docket")
            data["trial_judge"] = _get_text_by_id(tree, "judge")
            data["trial_date_filed"] = _get_text_by_id(tree, "date-filed")
            data["plaintiff"] = _get_text_by_id(tree, "plaintiff")
            data["defendant"] = _get_text_by_id(tree, "defendant")
            data["case_type"] = _get_text_by_id(tree, "case-type")

            # Yield the combined data from all three pages
            yield ParsedData(data)


    def _get_text(element, xpath: str) -> str:
        """Extract text content from an xpath query."""
        results = element.xpath(xpath)
        if results:
            return results[0].text_content().strip()
        return ""


    def _get_text_by_id(tree, element_id: str) -> str:
        """Extract text content from an element by its ID."""
        return _get_text(tree, f"//*[@id='{element_id}']")


Example: Using the Driver
--------------------------

.. code-block:: python

    from kent.driver.sync_driver import SyncDriver
    from tests.scraper_driver.scraper.example.bug_court_accumulated_data import (
        BugCourtScraperWithAccumulatedData,
    )

    # Create scraper and driver
    scraper = BugCourtScraperWithAccumulatedData()
    driver = SyncDriver(scraper)

    # Run the scraper
    results = driver.run()

    # Results have data from all three pages
    for case in results:
        print(f"Case: {case['case_name']}")
        print(f"  Appeals: {case['appeals_docket']} (Judge {case['appeals_judge']})")
        print(f"  Trial:   {case['trial_docket']} (Judge {case['trial_judge']})")
        print(f"  Parties: {case['plaintiff']} v. {case['defendant']}")

Output:

.. code-block:: text

    Case: Butterfly v. Caterpillar (Appeal)
      Appeals: BCA-2024-001 (Judge Honeybee)
      Trial:   BCC-2024-002 (Judge Mantis)
      Parties: Butterfly v. Caterpillar


What's Next
-----------

In :doc:`06_aux_data`, we will introduce auxiliary data, for data that helps
us navigate the website, but that we aren't necessarily interested in keeping.
