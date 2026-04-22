============================
CLI and Debugger Internals
============================

This page covers the implementation details of the ``kent`` and ``pdd``
command-line tools, the Jinja2 templating system, and the shared debugger
library that underpins both the CLI and the web UI.


The ``kent`` CLI
================

Location: ``kent/cli.py``

Built with Click. The top-level group is ``cli()`` with subcommands:
``list``, ``inspect``, ``migrate``, ``serve``, ``run``.

Driver Auto-Selection
---------------------

The ``run`` command implements driver auto-selection based on
``scraper.driver_requirements``:

1. If any requirement is ``JS_EVAL``, ``FF_ALIKE``, ``CHROME_ALIKE``,
   ``HCAP_HANDLER``, or ``RCAP_HANDLER`` → Playwright
2. Otherwise → persistent

For ``FF_ALIKE`` / ``CHROME_ALIKE``, the CLI also auto-resolves a browser
profile from ``$KENT_HOME/profiles/{firefox,chrome}/``.

Scraper Discovery
-----------------

``kent list`` uses ``kent/discovery.py`` to scan ``.py`` files under the
working directory, importing each and checking for ``BaseScraper`` subclasses.
Results are sorted by module path.

Seed Params Generation
----------------------

``kent inspect --seed-params`` introspects entry point parameter types and
generates example values (``1`` for int, ``"example"`` for str,
``"2025-01-01"`` for date, and recursively for Pydantic models). This gives
users a template to edit for ``kent run --params``.


The ``pdd`` CLI
===============

Location: ``kent/driver/persistent_driver/cli/``

The PDD CLI is a hierarchical Click command system with 15+ command groups
and 50+ subcommands.

Architecture
------------

.. code-block:: text

    pdd (top-level group)
    |-- info
    |-- requests/
    |   |-- list, show, search, cancel, ...
    |-- responses/
    |   |-- list, show, search, ...
    |-- results/
    |   |-- list, show, validate, export, ...
    |-- errors/
    |   |-- list, diagnose, resolve, ...
    |-- doctor/
    |   |-- health
    |-- scrape/
    |   |-- health, estimates
    |-- compression/
    |   |-- stats, train, recompress
    |-- step/
    |   |-- re-evaluate, xpath-stats
    |-- compare/
    |-- export/
    |-- cancel/
    |-- incidental/
    |-- seed-error-patch-rerun/

The ``--db`` Option
-------------------

The ``--db`` option uses a **sticky pattern**: it can be placed at any level
in the command hierarchy, and child commands inherit it from their parent
context. The ``_resolve_db_path()`` helper walks the Click context chain
to find the nearest ``--db`` value.

.. code-block:: bash

    # All equivalent:
    pdd --db run.db requests list
    pdd requests --db run.db list
    pdd requests list --db run.db

Standardized Argument Patterns
------------------------------

Commands follow consistent patterns:

**Pagination:** ``--limit`` (default: 100) and ``--offset`` (default: 0)
for all list commands.

**Output format:** ``--format {default,json,jsonl}`` on all commands.
``--template <name>`` selects an alternative Jinja2 template.

**Filtering:** Command-specific but consistent naming:

- ``--status``: Filter by request/result status
- ``--continuation``: Filter by step name
- ``--resolved`` / ``--unresolved``: Filter errors
- ``--valid`` / ``--invalid``: Filter results

**Search modes:** Mutually exclusive options on search commands:

- ``--text``: Plain text search
- ``--regex``: Regular expression search
- ``--xpath``: XPath query against stored HTML

Data Flow
---------

Every command follows the same pattern:

1. Resolve ``--db`` path
2. Open a ``LocalDevDriverDebugger`` async context
3. Call debugger methods to query/manipulate the database
4. Pass the result (a JSON-serializable dict) to ``render_output()``
5. ``render_output()`` dispatches to JSON, JSONL, or Jinja2 template


The Templating System
=====================

Location: ``kent/driver/persistent_driver/cli/templating.py``

PDD uses Jinja2 for human-readable output. The ``render_output()`` function
is the single entry point.

Template Resolution
-------------------

Templates are resolved via a two-level search path:

1. **User-local**: ``~/.config/kent/templates/<command>/<subcommand>/<name>.jinja2``
2. **Built-in**: ``cli/templates/<command>/<subcommand>/<name>.jinja2``

The user-local path is checked first, so users can override any built-in
template by placing a file at the same relative path.

Built-in templates are organized by command group:

.. code-block:: text

    cli/templates/
    |-- _macros.jinja2              # Shared macros
    |-- info/
    |-- requests/
    |   |-- list/default.jinja2
    |   |-- show/default.jinja2
    |   |-- search/default.jinja2
    |   |-- ...
    |-- responses/
    |-- results/
    |-- errors/
    |-- doctor/
    |-- scrape/
    |-- compression/
    |-- step/
    |-- compare/
    |-- seed_error_patch_rerun/

Shared Macros
-------------

``_macros.jinja2`` provides reusable macros:

- ``pagination(total, count, offset, limit)``: Standard pagination header
- ``section(title)``: Section header (``=== Title ===``)
- ``kv(key, value)``: Key-value line
- ``table(items, columns)``: Fixed-width column table. ``columns`` is a list
  of ``{"key": str, "width": int, "header": str}`` dicts.

Example template (``requests/list/default.jinja2``):

.. code-block:: jinja

    {% from "_macros.jinja2" import pagination %}
    {% set rows = data["items"] %}
    {{ pagination(data.total, rows | length, data.offset, data.limit) }}
    {% if rows %}
    {{ "id" | ljust(15) }}  {{ "status" | ljust(15) }}  {{ "url" | ljust(50) }}
    {{ "-" * 85 }}
    {% for r in rows %}
    {{ r.id | string | ljust(15) }}  {{ r.status | ljust(15) }}  {{ r.url | truncate_str(50) | ljust(50) }}
    {% endfor %}
    {% else %}
    No requests found
    {% endif %}

Custom Filters
--------------

- ``checkmark``: Renders boolean as ``✓`` or ``✗``
- ``ljust(width)``: Left-justify to *width* characters
- ``truncate_str(max_len)``: Truncate string with no ellipsis marker
- ``format_bytes``: Format byte count with thousands separators

Globals: ``json_dumps`` is available in all templates.

Adding a New Command
--------------------

To add a new PDD command:

1. Create a command module in ``cli/`` (e.g., ``my_command.py``)
2. Define Click commands that call ``LocalDevDriverDebugger`` methods
3. Pass results to ``render_output(data, template_path="my_command/subcommand")``
4. Create ``cli/templates/my_command/subcommand/default.jinja2``
5. Register the command group in ``cli/__init__.py``


The LocalDevDriverDebugger
==========================

Location: ``kent/driver/persistent_driver/debugger/``

The ``LocalDevDriverDebugger`` (LDDD) is the shared data access layer used
by both the ``pdd`` CLI and the web UI. It provides a high-level API for
inspecting and manipulating run databases without requiring the full driver
runtime.

Architecture
------------

The debugger is composed of mixins that separate concerns:

.. code-block:: python

    class LocalDevDriverDebugger(
        InspectionMixin,      # list_requests, get_request, list_results, ...
        ManipulationMixin,    # cancel_request, resolve_error, ...
        IntegrityMixin,       # check_integrity, get_ghost_requests, ...
        ValidationMixin,      # validate_results, check_estimates, ...
        ExportSearchMixin,    # export_results_jsonl, search, ...
        ComparisonMixin,      # compare_continuation, ...
        DebuggerBase,         # open(), connection management
    ):
        pass

``DebuggerBase`` provides:

- ``open(db_path, read_only=False)``: Async context manager for connection lifecycle
- SQLite connection management via ``SQLManager``
- Read-only mode enforcement via SQLite connection flags

Usage Pattern
-------------

.. code-block:: python

    async with LocalDevDriverDebugger.open(db_path) as debugger:
        metadata = await debugger.get_run_metadata()
        requests = await debugger.list_requests(status="failed")
        await debugger.cancel_request(request_id=42)

Read-only mode is used by the CLI for inspection commands, preventing
accidental writes to running databases:

.. code-block:: python

    async with LocalDevDriverDebugger.open(db_path, read_only=True) as debugger:
        stats = await debugger.get_stats()

Sharing Between CLI and Web UI
------------------------------

Both the ``pdd`` CLI and the web UI (``persistent_driver/web/``) use the
same ``LocalDevDriverDebugger`` for all database access. This ensures:

- Consistent behavior between CLI and web UI
- Single source of truth for query logic
- Mixins can be tested independently of either interface

The web UI routes (in ``web/routes/``) call the same debugger methods as
the CLI commands, then return Pydantic response models for the HTTP API.
The CLI commands call the same methods, then pass results to the Jinja2
templating system.
