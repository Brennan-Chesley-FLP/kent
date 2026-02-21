"""CLI entry point to launch the Bug Civil Court demo website."""

from __future__ import annotations


def main() -> None:
    """Start the Bug Civil Court demo on http://127.0.0.1:8080."""
    import uvicorn

    from kent.demo.app import app

    print("Starting Bug Civil Court demo at http://127.0.0.1:8080")
    print("Press Ctrl+C to stop.\n")
    uvicorn.run(app, host="127.0.0.1", port=8080, log_level="info")


if __name__ == "__main__":
    main()
