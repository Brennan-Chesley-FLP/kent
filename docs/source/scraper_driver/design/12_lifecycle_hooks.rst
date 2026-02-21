Step 12: Lifecycle Hooks
==========================

In previous steps, we introduced various callbacks for handling specific events:
``on_data`` (Step 7), ``on_structural_error`` (Step 8), ``on_invalid_data`` (Step 9),
``on_transient_exception`` (Step 10), and ``on_archive`` (Step 13).

Now in Step 14, we complete the callback system by adding **lifecycle hooks** that track
the entire scraper run from start to finish. These hooks provide visibility into when a
scraper starts, when it completes, and what happened during execution.


Overview
--------

In this step, we introduce:

1. **on_run_start** - Called at the beginning of run()
2. **on_run_complete** - Called at the end of run() (in finally block)
3. **Status tracking** - "completed" or "error"
4. **Error capture** - Exception passed to on_run_complete on failure


on_run_start Callback
---------------------

The ``on_run_start`` callback is invoked at the very beginning of ``run()``, before any
requests are made:

.. code-block:: python

    def on_run_start_callback(scraper_name: str) -> None:
        """Called when scraper run begins.

        Args:
            scraper_name: The __name__ of the scraper class (e.g., "BugCourtScraper")
        """
        print(f"Starting scraper: {scraper_name}")

    driver = SyncDriver(
        scraper=scraper,
        on_run_start=on_run_start_callback,
    )

**Callback Signature:**

- **scraper_name (str)** - The name of the scraper class being run

**When It Fires:**

- At the very beginning of ``run()``, before the entry request is created
- Before any HTTP requests are made
- Before any data is processed


on_run_complete Callback
-------------------------

The ``on_run_complete`` callback is invoked at the end of ``run()``, in a finally block
to ensure it fires even when exceptions occur:

.. code-block:: python

    def on_run_complete_callback(
        scraper_name: str,
        status: str,
        error: Exception | None,
    ) -> None:
        """Called when scraper run completes (success or failure).

        Args:
            scraper_name: The __name__ of the scraper class
            status: "completed" if successful, "error" if exception occurred
            data_count: Number of ParsedData items yielded
            request_count: Number of requests processed
            error: The exception that occurred, or None if successful
        """
        if status == "completed":
            print(f"✓ {scraper_name} completed successfully")
        else:
            print(f"✗ {scraper_name} failed: {error}")

    driver = SyncDriver(
        scraper=scraper,
        on_run_complete=on_run_complete_callback,
    )

**When It Fires:**

- In a finally block at the end of ``run()``
- Guaranteed to fire even if exceptions occur
- After all data has been processed or exception has been raised


Next Steps
----------

In :doc:`13_priority_queue`, we introduce a priority queue for request
ordering. This allows scrapers to control execution order by assigning
priorities to requests, ensuring archive requests run after detail pages
or urgent requests run first.
