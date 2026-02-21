"""Bug Civil Court demo for the kent scraper-driver framework.

This package provides a whimsical demo website and scraper showcasing
kent's features: speculative requests, HTML page parsing, JSON APIs,
file archiving, Pydantic validation, and the PersistentDriver tooling.

Requires the ``demo`` extra::

    pip install kent[demo]
"""

try:
    import fastapi  # noqa: F401
    import uvicorn  # noqa: F401
except ImportError:
    raise ImportError(
        "Demo features require the 'demo' extra. "
        "Install with: pip install kent[demo]"
    )
