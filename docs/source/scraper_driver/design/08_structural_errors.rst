Step 8: Structural Assumption Errors
======================================

In Step 7, we introduced callbacks for handling scraped data. Now we address
another operational concern: **what happens when a website's HTML structure
changes?**

Scrapers make assumptions about HTML structure - they expect certain elements
to exist at specific locations. When these assumptions are violated (e.g., the
website redesigns its pages), the scraper should fail fast with clear,
actionable error messages rather than silently returning incomplete data.

This step introduces the **CheckedHtmlElement** wrapper and **structural
assumption exceptions** to validate HTML structure and provide clear diagnostic
information when structure changes are detected.


Overview
--------

In this step, we introduce:

1. **ScraperAssumptionException** - Base exception class for assumption violations
2. **HTMLStructuralAssumptionException** - Specific exception for HTML structure mismatches
3. **CheckedHtmlElement** - Wrapper around lxml.html.HtmlElement with validated selectors


Why Structural Validation?
---------------------------

Websites change their structure. We want to know as soon as we can, and as specifically as we can that the assumptions we made about that structure aren't valid any longer.

**Problem: Vague Failure**

.. code-block:: python

    tree = html.fromstring(response.text)
    case_name_elements = tree.xpath("//h2[@class='case-name']")
    case_name = case_name_elements[0].text # may or may not blow up with a KeyError or a ValueError.


**Solution: Explicit Validation**

.. code-block:: python

    # With validation - raises clear exception
    tree = CheckedHtmlElement(html.fromstring(response.text), response.url)
    case_name_elements = tree.checked_xpath(
        "//h2[@class='case-name']",
        "case name header",
        min_count=1,
        max_count=1,
    )
    # Raises HTMLStructuralAssumptionException if not exactly 1 element found
    case_name = case_name_elements[0].text


Exception Hierarchy
-------------------

ScraperAssumptionException (Base)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The base class for all scraper assumption violations:

.. code-block:: python

    class ScraperAssumptionException(Exception):
        """Base class for scraper assumption violations."""

        def __init__(
            self,
            message: str,
            request_url: str,
            context: dict[str, Any] | None = None,
        ) -> None:
            self.message = message
            self.request_url = request_url
            self.context = context or {}

Features:

- **Contextual error messages** - Include URL and context dict
- **Structured information** - Machine-readable attributes for logging/monitoring
- **Formatted output** - Pretty-printed error messages for debugging

Example error message:

.. code-block:: text

    HTML structure mismatch: Expected exactly 1 elements for 'case name', but found 0
    URL: http://example.com/cases/12345
    Context:
      selector: //h2[@class='case-name']
      selector_type: xpath
      expected_min: 1
      expected_max: 1
      actual_count: 0


HTMLStructuralAssumptionException
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Raised when HTML structure doesn't match expectations:

.. code-block:: python

    class HTMLStructuralAssumptionException(ScraperAssumptionException):
        """Raised when HTML structure doesn't match expectations."""

        def __init__(
            self,
            selector: str,
            selector_type: str,  # "xpath" or "css"
            description: str,
            expected_min: int,
            expected_max: int | None,
            actual_count: int,
            request_url: str,
        ) -> None:
            # ... builds formatted message and context dict

This exception includes:

- **selector** - The XPath or CSS selector that was used
- **selector_type** - Whether it's an XPath or CSS selector
- **description** - Human-readable description of what was being selected
- **expected_min/max** - Expected count range
- **actual_count** - Actual number of elements found
- **request_url** - URL where the error occurred


CheckedHtmlElement Wrapper
---------------------------

The CheckedHtmlElement class wraps lxml.html.HtmlElement and provides validated
selector methods:

Basic Usage
^^^^^^^^^^^

.. code-block:: python

    from lxml import html
    from kent.common.checked_html import CheckedHtmlElement

    # Wrap the lxml element
    tree = CheckedHtmlElement(
        html.fromstring(response.text),
        request_url=response.url
    )

    # Use checked_xpath with validation
    case_rows = tree.checked_xpath(
        "//tr[@class='case-row']",
        "case rows",
        min_count=1,    # Expect at least 1 row
        max_count=100,  # Expect at most 100 rows
    )

    # Use checked_css with validation
    titles = tree.checked_css(
        "h2.case-title",
        "case titles",
        min_count=1,
    )


Validation Modes
^^^^^^^^^^^^^^^^

**Exact Count (min_count == max_count)**

.. code-block:: python

    # Expect exactly 1 case name header
    [case_name] = tree.checked_xpath(
        "//h1[@class='case-name']",
        "case name",
        min_count=1,
        max_count=1,
    )

**Minimum Count (max_count=None)**

.. code-block:: python

    # Expect at least 5 docket entries
    entries = tree.checked_xpath(
        "//div[@class='docket-entry']",
        "docket entries",
        min_count=5,
        max_count=None,  # No upper limit
    )

**Range (min_count < max_count)**

.. code-block:: python

    # Expect between 10 and 20 parties
    parties = tree.checked_xpath(
        "//li[@class='party']",
        "parties",
        min_count=10,
        max_count=20,
    )

**Default (min_count=1)**

.. code-block:: python

    # Defaults to expecting at least 1 element
    judge = tree.checked_xpath("//span[@id='judge']", "judge name")


Element Delegation
^^^^^^^^^^^^^^^^^^

CheckedHtmlElement delegates all other attribute access to the wrapped lxml
element, so it can be used as a drop-in replacement. Note however, that these returns won't be wrapped!

.. code-block:: python

    tree = CheckedHtmlElement(html.fromstring(response.text))

    # Can use all normal lxml methods
    assert tree.tag == "html"
    body = tree.find(".//body")
    all_divs = tree.findall(".//div")

    # Plus the checked methods
    case_divs = tree.checked_xpath("//div[@class='case']", "case divs")


Nested Queries
^^^^^^^^^^^^^^

**Important**: Results from ``checked_xpath()`` and ``checked_css()`` are wrapped
in ``CheckedHtmlElement`` to support nested queries:

.. code-block:: python

    tree = CheckedHtmlElement(html.fromstring(response.text))

    # First level query - returns list of CheckedHtmlElement
    containers = tree.checked_xpath("//div[@class='container']", "containers", min_count=1)

    # Nested query on each container
    for container in containers:
        # Can call checked_xpath on the result!
        items = container.checked_xpath(".//item", "items", min_count=2)
        for item in items:
            # Can nest even deeper
            title = item.checked_xpath(".//title", "title", min_count=1)[0]
            print(title.text)

This makes it easy to validate structure at each level:

.. code-block:: python

    # Validate outer structure
    table = tree.checked_xpath("//table[@id='results']", "results table",
                                min_count=1, max_count=1)[0]

    # Validate rows within table
    rows = table.checked_xpath(".//tr[@class='data-row']", "data rows",
                                min_count=10)

    # Validate cells within each row
    for row in rows:
        cells = row.checked_xpath(".//td", "cells", min_count=5, max_count=5)

**Note on text/attribute queries**: XPath queries that return text nodes or
attributes (like ``//div/text()`` or ``//a/@href``) return raw strings, not
``CheckedHtmlElement`` objects. You can add a ``type=str`` argument to your
checked_xpath call to make your types unambiguous for mypy and ty.


Example: Bug Court Scraper with Validation
-------------------------------------------

Here's how the Bug Court scraper uses CheckedHtmlElement:

.. code-block:: python

    from lxml import html
    from kent.common.checked_html import CheckedHtmlElement
    from kent.data_types import Response, ParsedData

    class BugCourtScraper(BaseScraper[dict]):
        """Bug Court scraper with structural validation."""

        def parse_detail(
            self, response: Response
        ) -> Generator[ParsedData, None, None]:
            """Parse case detail page with structural validation."""
            tree = CheckedHtmlElement(
                html.fromstring(response.text),
                response.url
            )

            # Validate that the case details container exists
            # This will raise HTMLStructuralAssumptionException if missing
            tree.checked_xpath(
                "//div[@class='case-details']",
                "case details container",
                min_count=1,
                max_count=1,
            )

            # Extract case data (using helper functions)
            yield ParsedData({
                "docket": _get_text_by_id(tree, "docket"),
                "case_name": _get_text(tree, "//h2"),
                "plaintiff": _get_text_by_id(tree, "plaintiff"),
                "defendant": _get_text_by_id(tree, "defendant"),
                # ... more fields ...
            })


When to Use Structural Validation
----------------------------------

**Always**

- Or at least whenever you can.
- Front load parsing steps with the elements that we need to be there.
  If there are optional elements, parse them out at the end of the function if possible.
  Failing faster is better.


Error Handling in Production
-----------------------------

The SyncDriver provides an ``on_structural_error`` callback parameter for handling
structural assumption errors during scraping. This allows you to:

1. **Log with full context** - The exception includes URL and selector details
2. **Alert on failures** - Notify maintainers of structure changes
3. **Continue or stop** - Return True to continue scraping, False to stop
4. **Skip gracefully** - Process other cases even when some fail

Basic Usage
^^^^^^^^^^^

.. code-block:: python

    from kent.driver.sync_driver import SyncDriver
    from kent.common.exceptions import (
        HTMLStructuralAssumptionException
    )

    def handle_structural_error(exc: HTMLStructuralAssumptionException) -> bool:
        """Log structural errors and continue scraping."""
        logger.error(
            f"Structural change detected: {exc.message}",
            extra={
                "url": exc.request_url,
                "selector": exc.selector,
                "expected_count": f"{exc.expected_min}-{exc.expected_max}",
                "actual_count": exc.actual_count,
            }
        )
        # Alert maintainers
        send_alert(f"Scraper structural assumption failed: {exc.description}")

        # Return True to continue processing other pages
        # Return False to stop the scraper immediately
        return True

    driver = SyncDriver(
        scraper,
        on_structural_error=handle_structural_error
    )
    driver.run()

Example Callback
^^^^^^^^^^^^^^^^

The framework provides a simple example callback that logs the error and stops:

.. code-block:: python

    from tests.scraper_driver.utils import log_structural_error_and_stop

    driver = SyncDriver(
        scraper,
        on_structural_error=log_structural_error_and_stop
    )
    driver.run()

Without Callback
^^^^^^^^^^^^^^^^

If you don't provide an ``on_structural_error`` callback, exceptions propagate
normally and stop the scraper:

.. code-block:: python

    driver = SyncDriver(scraper)  # No callback

    try:
        driver.run()
    except HTMLStructuralAssumptionException as e:
        # Handle the exception outside the driver
        logger.error(f"Scraper failed: {e.message}")


What's Next
-----------

In :doc:`09_data_validation`, we will look at data validation.