Step 7: Callbacks - Data Events
================================

In Step 6, we introduced aux_data for navigation metadata. Now we need a way
to handle scraped data as it's produced - for persistence, logging, monitoring,
or other side effects. This is where the **on_data callback** comes in.

This step introduces a simple callback system that allows you to define custom
behavior when ParsedData is yielded, without subclassing the driver or modifying
its internals.


Overview
--------

In this step, we introduce **on_data callback** - Optional parameter on the driver


Why Callbacks?
--------------

Callbacks let the program that is invoking the driver have more flexibility about how it wants to handle results.
We can stream results to disk to deal with memory consumption issues, or collect them into a list if we want to run a simple test.


The on_data Parameter
----------------------

The driver accepts an optional callback:

.. code-block:: python

    class SyncDriver:
        def __init__(
            self,
            scraper: BaseScraper,
            storage_dir: Path | None = None,
            on_data: Callable[[dict], None] | None = None,
        ):
            """Initialize the driver.

            Args:
                scraper: Scraper instance.
                storage_dir: Directory for archived files.
                on_data: Optional callback invoked when ParsedData yielded.
                    Receives the unwrapped data dict.
            """
            self.scraper = scraper
            self.storage_dir = storage_dir or Path(gettempdir()) / "juriscraper_files"
            self.on_data = on_data

When ParsedData is yielded, the driver invokes the callback:

.. code-block:: python

    def run(self):
        # ... process requests ...
        for item in continuation_method(response):
            match item:
                case ParsedData():
                    data = item.unwrap()
                    # Invoke callback if provided
                    if self.on_data:
                        self.on_data(data)
                    self.results.append(data)


Data Flow
---------

.. mermaid::

    sequenceDiagram
        participant S as Scraper
        participant D as Driver
        participant C as Callback
        participant F as File/DB

        D->>S: parse_list(response)
        S-->>D: yield ParsedData(case1)
        D->>D: data = item.unwrap()
        D->>C: on_data(data)
        C->>F: Write data
        D->>D: results.append(data)

        D->>S: parse_detail(response)
        S-->>D: yield ParsedData(case2)
        D->>D: data = item.unwrap()
        D->>C: on_data(data)
        C->>F: Write data
        D->>D: results.append(data)

        Note over D: Repeat for all data items...

        D-->>D: Return results


Example: Complete Scraping with Callbacks
------------------------------------------

Here's a complete example using callbacks for multiple purposes:

.. code-block:: python

    from pathlib import Path
    from kent.driver.sync_driver import SyncDriver
    from kent.driver.callbacks import (
        combine_callbacks,
        save_to_jsonl_file,
        count_data,
    )
    from tests.scraper_driver.scraper.example.bug_court import BugCourtScraper

    def main():
        scraper = BugCourtScraper()
        scraper.BASE_URL = "http://bugcourt.example.com"

        # Track statistics
        counter = [0]

        # Set up output file
        output_file = Path("bug_court_cases.jsonl")

        with output_file.open("w") as f:
            # Custom callback for validation
            def validate_case(data: dict):
                required = ["docket", "case_name", "plaintiff", "defendant"]
                if not all(field in data for field in required):
                    print(f"WARNING: Incomplete case data: {data.get('docket')}")

            # Combine multiple callbacks
            driver = SyncDriver(
                scraper,
                on_data=combine_callbacks(
                    save_to_jsonl_file(f),  # Save to file
                    count_data(counter),     # Count items
                    validate_case,           # Validate data
                )
            )

            # Run scraper
            results = driver.run()

        print(f"Scraping complete!")
        print(f"  Scraped: {counter[0]} cases")
        print(f"  Saved to: {output_file}")
        print(f"  In memory: {len(results)} cases")

    if __name__ == "__main__":
        main()


What's Next
-----------

In :doc:`08_structural_errors`, we will look at our second operational concern,
structural assumptions about webpages.

