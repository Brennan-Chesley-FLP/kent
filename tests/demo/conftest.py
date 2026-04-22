"""Shared fixtures for the demo integration tests."""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Generator
from contextlib import closing

import pytest
import uvicorn

from kent.demo.app import app as demo_app


def _find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def demo_server_url() -> Generator[str, None, None]:
    """Start the demo FastAPI app in a background thread."""
    port = _find_free_port()
    host = "127.0.0.1"

    config = uvicorn.Config(
        demo_app,
        host=host,
        port=port,
        log_level="error",
    )
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for the server to be ready.
    for _ in range(50):
        try:
            with socket.create_connection((host, port), timeout=0.1):
                break
        except OSError:
            time.sleep(0.1)

    url = f"http://{host}:{port}"
    yield url

    server.should_exit = True
    thread.join(timeout=2.0)
