Bug Civil Court Demo
====================

The **Bug Civil Court** demo is a self-contained example that showcases the
kent scraper-driver framework.  It pairs a whimsical demo website with a
fully featured scraper so you can see speculative requests, HTML page parsing,
JSON APIs, file archiving, and the PersistentDriver debugging tools in action.

Quick Start
-----------

1. Launch the demo website::

      uv run kent-demo

   Visit http://127.0.0.1:8080 in your browser.

2. In a separate terminal, run the demo scraper with the PersistentDriver::

      uv run python kent/demo/run-persistent.py

   Pass ``--help`` to see all available options::

      uv run python kent/demo/run-persistent.py --help

   You can also run the demo with the PlaywrightDriver (browser automation)::

      uv run python kent/demo/run-playwright.py --help


Exploring the Website
---------------------

The demo serves a fictional court where insects file lawsuits.

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Page
     - Content Type
     - Description
   * - ``/``
     - HTML
     - Homepage with navigation
   * - ``/cases``
     - HTML
     - Table of all 30 cases (2024--2026)
   * - ``/cases/{year}/{number}``
     - HTML
     - Case detail with optional opinion and oral argument links
   * - ``/opinions``
     - HTML
     - Published opinions list
   * - ``/opinions/{docket}``
     - HTML
     - Opinion detail with Wikimedia insect illustration
   * - ``/oral-arguments``
     - HTML
     - Oral argument recordings list
   * - ``/oral-arguments/{docket}``
     - HTML
     - Oral argument detail with link to USDA insect sound WAV
   * - ``/justices``
     - HTML
     - Justice bio cards
   * - ``/api/justices``
     - **JSON**
     - All justice bios as a JSON array
   * - ``/api/justices/{slug}``
     - **JSON**
     - Single justice bio


The code
---------------------

The demo scraper exercises the following kent features:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Feature
     - How it's used
   * - ``@entry`` with ``YearlySpeculation``
     - Speculative case discovery by year + sequential number.
       Three year-partitions (2024 frozen, 2025 frozen, 2026 live).
   * - ``@entry`` (normal)
     - Entry points for oral arguments list and justice JSON API.
   * - ``@step`` with ``page`` (PageElement)
     - HTML parsing via ``query_xpath()``, ``find_links()``,
       ``text_content()``.
   * - ``@step`` with ``json_content``
     - Parsing the ``/api/justices`` JSON endpoint.
   * - ``Request(nonnavigating=True)``
     - Fetching justice JSON without navigating away.
   * - ``Request(archive=True)``
     - Downloading USDA WAV audio and Wikimedia insect images.
   * - ``ArchiveResponse`` / ``local_filepath``
     - Processing archived files to yield final data.
   * - ``accumulated_data``
     - Passing docket and case_name from case detail through to
       the opinion/audio archive handlers.
   * - ``EstimateData``
     - Declaring expected counts for integrity checking.
   * - ``ScrapedData`` + deferred validation
     - All four data models use ``.raw()`` for deferred Pydantic
       validation.
   * - ``fails_successfully()``
     - Detecting soft-404 pages ("Case Not Found") on speculative
       requests.


Inspecting with the Web UI
--------------------------

After the scraper finishes, start the PersistentDriver web interface
to inspect the run::

   python -m uvicorn kent.driver.persistent_driver.web.app:create_app \
       --factory --port 8081

Then visit http://127.0.0.1:8081 to explore:

- **Runs** — see the scraper run, its status, and timestamps
- **Requests** — browse all HTTP requests, filter by status or URL
- **Responses** — view stored HTML/JSON responses
- **Results** — inspect all parsed data (cases, justices, opinions, etc.)
- **Errors** — diagnose any failures
- **Speculation** — monitor how the year-partitioned speculation progressed
- **Archived files** — browse downloaded WAV and image files


Debugging with ``ldd-debug``
----------------------------

The ``ldd-debug`` CLI provides offline inspection of the scraper database::

   # Overview
   uv run ldd-debug --db demo_scraper.db info

   # List all requests and their statuses
   uv run ldd-debug --db demo_scraper.db requests list

   # View a specific response
   uv run ldd-debug --db demo_scraper.db responses show <request-id>

   # List all parsed results
   uv run ldd-debug --db demo_scraper.db results list

   # Check for errors
   uv run ldd-debug --db demo_scraper.db errors list

   # Database health check
   uv run ldd-debug --db demo_scraper.db doctor health
