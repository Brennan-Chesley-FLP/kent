"""Callback functions for the driver's on_data parameter.

This module provides common callback functions that can be passed to the driver's
on_data parameter for side effects like persistence, logging, and monitoring.

Step 7 introduces the on_data callback system, allowing users to define custom
behavior when ParsedData is yielded without subclassing the driver.

Example::

    from kent.driver.callbacks import save_to_jsonl_file
    from kent.driver.sync_driver import SyncDriver

    with open("output.jsonl", "w") as f:
        driver = SyncDriver(scraper, on_data=save_to_jsonl_file(f))
        results = driver.run()
"""

import json
from collections.abc import Callable
from pathlib import Path
from typing import TextIO


def save_to_jsonl_file(file_handle: TextIO) -> Callable[[dict], None]:
    """Create a callback that saves each data item to a JSONL file.

    This is a factory function that returns a callback. The callback writes
    each data item as a JSON line to the provided file handle.

    Args:
        file_handle: An open file handle to write JSON lines to.
            The caller is responsible for opening and closing the file.

    Returns:
        A callback function that can be passed to driver's on_data parameter.

    Example::

        with open("output.jsonl", "w") as f:
            driver = SyncDriver(scraper, on_data=save_to_jsonl_file(f))
            results = driver.run()
        # File now contains one JSON object per line
    """

    def callback(data: dict) -> None:
        json.dump(data, file_handle)
        file_handle.write("\n")
        file_handle.flush()  # Ensure data is written immediately

    return callback


def save_to_jsonl_path(file_path: Path | str) -> Callable[[dict], None]:
    """Create a callback that appends each data item to a JSONL file.

    This is a factory function that manages the file handle internally.
    Each data item is appended to the file as a new JSON line.

    Warning:
        This opens the file in append mode ("a"). If you want to overwrite,
        delete the file first or use save_to_jsonl_file() with mode="w".

    Args:
        file_path: Path to the JSONL file to append to.

    Returns:
        A callback function that can be passed to driver's on_data parameter.

    Example::

        driver = SyncDriver(scraper, on_data=save_to_jsonl_path("output.jsonl"))
        results = driver.run()
        # File contains one JSON object per line
    """
    # Convert to Path if string
    path = Path(file_path) if isinstance(file_path, str) else file_path

    # Open file in append mode - will be kept open for duration
    file_handle = path.open("a")

    def callback(data: dict) -> None:
        json.dump(data, file_handle)
        file_handle.write("\n")
        file_handle.flush()

    # Note: File handle never explicitly closed - relies on process exit
    # For long-running processes, prefer save_to_jsonl_file() with context manager
    return callback


def print_data(prefix: str = "") -> Callable[[dict], None]:
    """Create a callback that prints each data item to stdout.

    Useful for debugging or monitoring scraper output during development.

    Args:
        prefix: Optional prefix to print before each data item.

    Returns:
        A callback function that can be passed to driver's on_data parameter.

    Example::

        driver = SyncDriver(scraper, on_data=print_data("SCRAPED: "))
        results = driver.run()
        # Prints: SCRAPED: {"docket": "...", ...}
    """

    def callback(data: dict) -> None:
        print(f"{prefix}{json.dumps(data, indent=2)}")

    return callback


def count_data(counter: list[int] | None = None) -> Callable[[dict], None]:
    """Create a callback that counts data items.

    The count is stored in a mutable list so it can be accessed after the driver
    finishes running.

    Args:
        counter: Optional list to store the count in. If None, creates a new list.
            The count is stored at index 0.

    Returns:
        A callback function that can be passed to driver's on_data parameter.
        The counter list is also returned indirectly (if you created it).

    Example::

        count = [0]  # Mutable container
        driver = SyncDriver(scraper, on_data=count_data(count))
        results = driver.run()
        print(f"Scraped {count[0]} items")
    """
    if counter is None:
        counter = [0]

    def callback(data: dict) -> None:
        counter[0] += 1

    return callback


def combine_callbacks(
    *callbacks: Callable[[dict], None],
) -> Callable[[dict], None]:
    """Combine multiple callbacks into a single callback.

    This allows you to run multiple side effects for each data item,
    such as saving to a file AND printing to stdout.

    Args:
        *callbacks: Variable number of callback functions to combine.

    Returns:
        A single callback function that invokes all provided callbacks.

    Example::

        driver = SyncDriver(
            scraper,
            on_data=combine_callbacks(
                save_to_jsonl_path("output.jsonl"),
                print_data("SCRAPED: "),
                count_data(my_counter),
            )
        )
        results = driver.run()
    """

    def callback(data: dict) -> None:
        for cb in callbacks:
            cb(data)

    return callback


def validate_data(
    validator: Callable[[dict], bool],
    on_invalid: Callable[[dict], None] | None = None,
) -> Callable[[dict], None]:
    """Create a callback that validates data items.

    This allows you to check data items as they're scraped and optionally
    handle invalid items differently.

    Args:
        validator: Function that returns True if data is valid, False otherwise.
        on_invalid: Optional callback to invoke for invalid items.
            If None, invalid items are silently ignored.

    Returns:
        A callback function that can be passed to driver's on_data parameter.

    Example::

        def is_complete(data: dict) -> bool:
            return "docket" in data and "case_name" in data

        def log_invalid(data: dict):
            print(f"INVALID: {data}")

        driver = SyncDriver(
            scraper,
            on_data=validate_data(is_complete, on_invalid=log_invalid)
        )
        results = driver.run()
    """

    def callback(data: dict) -> None:
        if validator(data):
            pass  # Valid - continue normally
        elif on_invalid:
            on_invalid(data)

    return callback
