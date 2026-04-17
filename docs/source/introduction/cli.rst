=======================
Command-Line Tools
=======================

Kent ships two CLI tools: ``kent`` for discovering, inspecting, and running
scrapers, and ``pdd`` (Persistent Driver Debugger) for inspecting run
databases after a scrape completes.


The ``kent`` CLI
================

list
----

Discover scrapers in the current directory tree:

.. code-block:: bash

    kent list
    kent list -v    # Show import errors

Scans ``.py`` files under the working directory for ``BaseScraper`` subclasses.
Output includes the import path, status, and entry point names for each scraper.

inspect
-------

Show a scraper's metadata, entry points, and step methods:

.. code-block:: bash

    kent inspect my_scrapers.court:MyCourtScraper

Output includes class name, module, status, version, court URL, court IDs,
data types, rate limits, driver requirements, entry points (with parameter
types), and steps (with response type tags).

Generate a seed params template for ``--params``:

.. code-block:: bash

    kent inspect my_scrapers.court:MyCourtScraper --seed-params

This outputs a JSON list with example values for each entry point's
parameters, suitable for editing and passing to ``kent run --params``.

run
---

Execute a scraper:

.. code-block:: bash

    # Auto-selects driver from scraper requirements
    kent run my_scrapers.court:MyCourtScraper --params '[{"search": {}}]'

    # Explicit driver selection
    kent run my_scrapers.court:MyCourtScraper --driver persistent --db run.db

    # Playwright with visible browser
    kent run my_scrapers.court:MyCourtScraper --driver playwright --headed

    # Start with ultiple workers
    kent run my_scrapers.court:MyCourtScraper --workers 4

**Driver auto-selection:** If ``--driver`` is omitted, the driver is chosen
from the scraper's ``driver_requirements``. Any requirement involving
JavaScript or browser features (``JS_EVAL``, ``FF_ALIKE``, ``CHROME_ALIKE``,
``HCAP_HANDLER``, ``RCAP_HANDLER``) selects the Playwright driver. Otherwise,
the persistent driver is used.

**Browser profile auto-resolution:** When ``FF_ALIKE`` or ``CHROME_ALIKE`` is
declared, the CLI looks for a browser profile at
``$KENT_HOME/profiles/{firefox,chrome}/`` (defaults to ``~/.kent/profiles/``).

Options:

- ``--driver {sync,async,persistent,playwright}``: Driver selection
- ``--db PATH``: SQLite database path (persistent/playwright)
- ``--params JSON``: Seed parameters as a JSON list
- ``--workers N``: Starting workers (default: 1)
- ``--max-workers N``: Maximum workers for dynamic scaling (default: 10)
- ``--storage PATH``: Directory for downloaded files
- ``--no-resume``: Start fresh instead of resuming
- ``--headed``: Show the browser window (playwright only)
- ``--browser-profile PATH``: Path to a browser profile directory
- ``--skip-archive``: Skip archive requests
- ``-v, --verbose``: Verbose logging

migrate
-------

Apply pending database schema migrations. This is mostly done automatically when needed:

.. code-block:: bash

    kent migrate run.db
    kent migrate run.db --target 16    # Migrate to a specific version

serve
-----

Start the persistent driver's web UI:

.. code-block:: bash

    kent serve
    kent serve --runs-dir ./my_runs --port 9000

Options:

- ``--runs-dir PATH``: Directory containing run databases (default: ``runs``)
- ``--host HOST``: Host to bind to (default: ``127.0.0.1``)
- ``--port PORT``: Port to bind to (default: ``8000``)
- ``-v, --verbose``: Verbose logging


The ``pdd`` CLI
===============

The Persistent Driver Debugger inspects and manipulates run databases. It is
the primary tool for post-hoc analysis of scraper runs.

The ``--db`` option specifies the database and can be placed at any level:

.. code-block:: bash

    pdd --db run.db requests list
    pdd requests --db run.db list
    pdd requests list --db run.db

All commands support output format selection:

- ``--format default``: Human-readable Jinja2-templated output
- ``--format json``: JSON output
- ``--format jsonl``: One JSON object per line

info
----

Show run metadata and summary statistics:

.. code-block:: bash

    pdd --db run.db info

requests
--------

Browse and manage the request queue:

.. code-block:: bash

    pdd --db run.db requests list                       # List all requests
    pdd --db run.db requests list --status completed     # Filter by status
    pdd --db run.db requests list --continuation parse_detail  # Filter by step
    pdd --db run.db requests show 42                     # Show request details
    pdd --db run.db requests search --text "docket"      # Search response content
    pdd --db run.db requests search --xpath "//div[@class='result']"  # XPath search
    pdd --db run.db requests cancel 42                   # Cancel a pending request

Pagination: ``--limit`` (default: 100), ``--offset`` (default: 0).

responses
---------

Inspect stored HTTP responses:

.. code-block:: bash

    pdd --db run.db responses list
    pdd --db run.db responses show 42
    pdd --db run.db responses search --text "error"

results
-------

View and validate parsed data:

.. code-block:: bash

    pdd --db run.db results list
    pdd --db run.db results list --valid          # Only valid results
    pdd --db run.db results list --invalid        # Only invalid results
    pdd --db run.db results validate              # Re-validate all results
    pdd --db run.db results export output.jsonl   # Export to JSONL

errors
------

Inspect and manage errors:

.. code-block:: bash

    pdd --db run.db errors list
    pdd --db run.db errors list --unresolved      # Only unresolved errors
    pdd --db run.db errors diagnose 42            # Structured diagnosis

doctor
------

Run health checks on the database:

.. code-block:: bash

    pdd --db run.db doctor health

Checks for ghost requests (queued but never executed), orphan responses,
and general database integrity.

scrape
------

Analyze scrape quality:

.. code-block:: bash

    pdd --db run.db scrape health      # Overall scrape health check
    pdd --db run.db scrape estimates   # Compare EstimateData against actual counts

compression
-----------

Manage response compression:

.. code-block:: bash

    pdd --db run.db compression stats        # Compression statistics
    pdd --db run.db compression train        # Train compression dictionaries
    pdd --db run.db compression recompress   # Recompress with new dictionaries

step
----

Re-evaluate steps and analyze selectors:

.. code-block:: bash

    pdd --db run.db step re-evaluate parse_detail   # Re-run a step against stored responses
    pdd --db run.db step xpath-stats parse_detail    # XPath selector statistics

compare
-------

Compare step execution results:

.. code-block:: bash

    pdd --db run.db compare <request_id>   # Compare dry-run vs stored execution


seed-error-patch-rerun
----------------------

Create patch databases from errored requests for targeted re-runs.
This finds the terminal ancestor requests for all errors and seeds a new database with those requests:

.. code-block:: bash

    pdd --db run.db seed-error-patch-rerun patch.db
