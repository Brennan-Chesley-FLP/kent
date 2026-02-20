================================
The Scraper-Driver Architecture
================================

The scraper-driver architecture is a complete rewrite of how kent
handles web scraping. It introduces a separation of concerns that makes
scrapers easier to write, test, and maintain.

The Primary Goal
----------------

**Scrapers should be pure functions.** They should parse HTML and yield data,
nothing more. All side effects - HTTP requests, file storage, database writes,
rate limiting - belong in the driver.

This separation enables:

- **Testability**: Scrapers can be tested with static HTML, no mocking needed
- **Reproducibility**: Record HTTP traffic once, replay forever
- **Flexibility**: Swap drivers for different environments (local dev vs production)
- **Clarity**: Scraper code focuses on parsing, driver code handles orchestration


Why a Rewrite?
--------------

A rewrite can simplify the code we maintain and allow us to address several issues that we've seen pop up.
See :doc:`issues` for GitHub issues that motivated this work, including:

- **Rate limiting** - We could benefit from a standardized approach
- **Court blocks** - Detection and recovery is ad-hoc
- **Validation** - No standardized way to validate scraped data
- **Testing** - Difficult to test scrapers without network access
- **Error handling** - Inconsistent error types and recovery strategies
- **Multiple return types** - Standardize a method for returning multiple different types of data


Architecture Overview
---------------------

The architecture has two main components:

**Scrapers** are generator functions that:

1. Receive HTML (or JSON, or binary data)
2. Parse it using lxml, BeautifulSoup, or other parsers
3. Yield data (dicts) or requests for more pages

**Drivers** orchestrate the scraping:

1. Execute HTTP requests
2. Feed responses to scrapers
3. Collect yielded data
4. Handle errors, retries, rate limiting
5. Manage file archival, deduplication, persistence

.. md-mermaid::
    :class: align-center

    flowchart TB
        subgraph Driver["Driver (Side Effects)"]
            HTTP[HTTP Client]
            Queue[Request Queue]
            Storage[File Storage]
            Hooks[Event Hooks]
        end

        subgraph Scraper["Scraper (Pure)"]
            Parse[Parse HTML]
            Yield[Yield Data/Requests]
        end

        HTTP -->|Response| Parse
        Parse --> Yield
        Yield -->|Data| Hooks
        Yield -->|Request| Queue
        Queue --> HTTP


Design Steps
------------

The architecture is documented through incremental steps, each building
on the previous. Hopefully this helps readers have a good grasp of the
design and extensibility of the system

.. toctree::
   :maxdepth: 1
   :caption: Design Documentation

   design/01_parsed_data
   design/02_navigating_request
   design/03_nonnavigating_request
   design/04_archive_request
   design/05_accumulated_data
   design/06_aux_data
   design/07_callbacks
   design/08_structural_errors
   design/09_data_validation
   design/10_transient_exceptions
   design/11_archive_callback
   design/12_lifecycle_hooks
   design/13_priority_queue
   design/14_deduplication
   design/15_permanent_data
   design/16_step_decorators
   design/17_search_and_standardization
   design/18_async_driver
   design/19_speculative_request


Inspiration
-----------

**Scrapy**

    The scraper-driver architecture split draws a great deal of inspiration from `Scrapy <https://scrapy.org>`_.
    Scrapy makes some decisions that we probably don't want to be bound to (it's build on Twisted),
    and it addresses a lot of concerns that we don't need to since it's intended for fairly wide usage.

    Key concepts borrowed from Scrapy:

    - Spiders yield items and requests (our scrapers yield data and requests)
    - Engine handles scheduling, HTTP, and middleware (our driver does this)



Additional Resources
--------------------

.. toctree::
   :maxdepth: 1

   issues
