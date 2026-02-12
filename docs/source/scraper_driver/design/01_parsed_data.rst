 Step 1: Parsing Data - Basic Scraper Function
=============================================

This is the foundation of the scraper-driver architecture. We start with the
simplest possible implementation: a function that parses HTML and yields data.

Overview
--------

In this step, we introduce:

1. **Scraper function** - A generator that parses HTML and yields dicts
2. **Driver function** - Runs the generator and collects results

There is no HTTP, no classes, no complex state - just the basic protocol that
everything else builds upon.


The Driver
----------

The driver's job is to run the scraper generator and collect results:

.. code-block:: python

    def run(scraper_fn, html: str) -> list[dict]:
        results = []
        for item in scraper_fn(html):
            if isinstance(item, dict):
                results.append(item)
        return results

At this stage, the driver is fairly simple.
We'll be building on this to make it more capable.

Data Flow
---------

The data flows through the system like this:

.. mermaid::

    flowchart LR
        HTML[HTML String] --> Scraper[parse_cases]
        Scraper -->|yields| PD1[dict]
        Scraper -->|yields| PD2[dict]
        Scraper -->|yields| PDN[dict...]
        PD1 --> Driver[run]
        PD2 --> Driver
        PDN --> Driver
        Driver --> Results[list of dicts]

Bug Civil Court
---------------

Throughout this documentation, we use a fictional court as our example: the
**Bug Civil Court**, where insects file civil lawsuits against each other.

Sample cases include:

- *Beetle v. Ant Colony* - Property dispute over tunnel rights
- *Butterfly v. Caterpillar* - Identity theft during metamorphosis
- *Spider v. Fly* - Contract dispute over web-visiting agreement
- *Grasshopper v. Ant* - Defamation regarding work ethic


Example Code
------------

Here's the complete scraper for Step 1:

.. code-block:: python
    :caption: bug_court.py - Step 1 implementation

    """Bug Civil Court scraper example.

    This module demonstrates the scraper-driver architecture through a fictional
    court where insects file civil lawsuits. It evolves across the 29 steps of
    the design documentation.

    Step 1: A simple function that parses HTML and yields ParsedData.
    """

    from collections.abc import Generator

    from lxml import html


    def parse_cases(html_content: str) -> Generator[dict, None, None]:
        """Parse the case list page and yield ParsedData for each case.

        This is the simplest possible scraper - a function that:
        1. Receives HTML as a string
        2. Parses it using lxml
        3. Yields ParsedData for each case found

        No HTTP requests, no classes, no complex state - just parsing and yielding.

        Args:
            html_content: The HTML content of the case list page.

        Yields:
            ParsedData for each case found in the HTML.
        """
        tree = html.fromstring(html_content)

        # Find all case rows in the table
        case_rows = tree.xpath("//tr[@class='case-row']")

        for row in case_rows:
            # Extract data from each cell
            docket = _get_text(row, ".//td[@class='docket']")
            case_name = _get_text(row, ".//td[@class='case-name']")
            date_filed = _get_text(row, ".//td[@class='date-filed']")
            case_type = _get_text(row, ".//td[@class='case-type']")
            status = _get_text(row, ".//td[@class='status']")

            yield {
                "docket": docket,
                "case_name": case_name,
                "date_filed": date_filed,
                "case_type": case_type,
                "status": status,
            }


    def _get_text(element, xpath: str) -> str:
        """Extract text content from an xpath query.

        Args:
            element: The lxml element to query.
            xpath: The xpath expression.

        Returns:
            The text content, or empty string if not found.
        """
        results = element.xpath(xpath)
        if results:
            return results[0].text_content().strip()
        return ""

And the driver:

.. code-block:: python
    :caption: sync_driver.py - Step 1 implementation

    """Synchronous driver implementation.

    This module contains the sync driver that processes scraper generators.
    It evolves across the 29 steps of the design documentation.

    Step 1: A simple function that runs a scraper generator and collects results.
    """

    from collections.abc import Callable, Generator
    from typing import Any


    def run(
        scraper_fn: Callable[[str], Generator[dict, None, None]],
        html: str,
    ) -> list[dict[str, Any]]:
        """Run a scraper function and collect the results.

        This is the simplest possible driver - a function that:
        1. Calls the scraper function with HTML
        2. Iterates through the generator
        3. Collects doc_data from each ParsedData yielded

        No HTTP, no queues - just running and collecting.

        Args:
            scraper_fn: A function that takes HTML and yields ParsedData.
            html: The HTML content to pass to the scraper.

        Returns:
            List of doc_data dictionaries from all yielded ParsedData.
        """
        results = []

        for item in scraper_fn(html):
            if isinstance(item, dict):
                results.append(item)

        return results


What's Next
-----------

In :doc:`02_navigating_request`, we introduce **NavigatingRequest** - the
ability for scrapers to request additional pages.