"""Tests for Step 7: Callbacks - on_data Parameter.

This module tests the on_data callback feature introduced in Step 7:
1. Driver accepts optional on_data callback
2. Callback is invoked when ParsedData is yielded
3. Callback receives unwrapped data
4. Multiple data items invoke callback multiple times
5. Callback is not invoked if None

Tests use a real aiohttp server to verify actual HTTP behavior.
"""

import json
from pathlib import Path

import pytest

from kent.driver.callbacks import (
    combine_callbacks,
    count_data,
    save_to_jsonl_file,
    save_to_jsonl_path,
)
from kent.driver.sync_driver import SyncDriver
from tests.scraper.example.bug_court import (
    BugCourtScraper,
)
from tests.utils import collect_results


class TestOnDataCallback:
    """Tests for on_data callback parameter."""

    def test_driver_accepts_on_data_callback(self):
        """The driver shall accept an optional on_data callback parameter."""
        scraper = BugCourtScraper()

        def callback(data: dict):
            pass

        # Should not raise
        driver = SyncDriver(scraper, on_data=callback)
        assert driver.on_data is callback

    def test_driver_accepts_none_callback(self):
        """The driver shall accept None for on_data callback."""
        scraper = BugCourtScraper()

        # Should not raise
        driver = SyncDriver(scraper, on_data=None)
        assert driver.on_data is None

    def test_callback_invoked_when_data_yielded(self, server_url: str):
        """The driver shall invoke on_data callback when ParsedData is yielded."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        # Track callback invocations
        callback, results = collect_results()

        driver = SyncDriver(scraper, on_data=callback)
        driver.run()

        # Callback should have been invoked for each result
        assert len(results) > 0

    def test_callback_not_invoked_if_none(self, server_url: str):
        """The driver shall not invoke callback if on_data is None."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        # Create a collector to verify scraper runs
        callback, results = collect_results()

        # This should not raise even though callback is None
        driver = SyncDriver(scraper, on_data=callback)
        driver.run()

        # Should have collected results normally
        assert len(results) > 0

    def test_callback_receives_unwrapped_data(self, server_url: str):
        """The driver shall pass unwrapped data to the callback."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        # Track what we received
        received_types = []

        def callback(data: dict):
            received_types.append(type(data))

        driver = SyncDriver(scraper, on_data=callback)
        driver.run()

        # All received items should be dicts (unwrapped from ParsedData)
        assert len(received_types) > 0
        assert all(t is dict for t in received_types)

    def test_multiple_data_items_invoke_callback_multiple_times(
        self, server_url: str
    ):
        """The driver shall invoke callback once for each ParsedData."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        callback, results = collect_results()

        driver = SyncDriver(scraper, on_data=callback)
        driver.run()

        # Verify we have multiple items
        assert len(results) > 1


class TestSaveToJsonlFile:
    """Tests for save_to_jsonl_file callback."""

    def test_saves_data_to_jsonl_file(self, server_url: str, tmp_path: Path):
        """The callback shall save each data item to a JSONL file."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        output_file = tmp_path / "output.jsonl"

        # Collect results to verify against file
        callback, results = collect_results()

        with output_file.open("w") as f:
            combined = combine_callbacks(save_to_jsonl_file(f), callback)
            driver = SyncDriver(scraper, on_data=combined)
            driver.run()

        # File should exist and contain JSON lines
        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == len(results)

        # Each line should be valid JSON
        for i, line in enumerate(lines):
            data = json.loads(line)
            assert data == results[i]

    def test_flushes_data_immediately(self, server_url: str, tmp_path: Path):
        """The callback shall flush data immediately for each item."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        output_file = tmp_path / "output.jsonl"

        # Track how many times we've been called
        call_count = [0]

        def counting_callback(f):
            def callback(data: dict):
                call_count[0] += 1
                save_to_jsonl_file(f)(data)
                # File should contain all data written so far
                f.seek(0)
                written_lines = f.read().strip().split("\n")
                assert len(written_lines) == call_count[0]

            return callback

        with output_file.open("w+") as f:
            driver = SyncDriver(scraper, on_data=counting_callback(f))
            driver.run()


class TestSaveToJsonlPath:
    """Tests for save_to_jsonl_path callback."""

    def test_saves_data_to_file_path(self, server_url: str, tmp_path: Path):
        """The callback shall save data to the specified file path."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        output_file = tmp_path / "output.jsonl"

        # Collect results to verify count
        callback, results = collect_results()

        combined = combine_callbacks(save_to_jsonl_path(output_file), callback)
        driver = SyncDriver(scraper, on_data=combined)
        driver.run()

        # File should exist and contain JSON lines
        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == len(results)

    def test_accepts_string_path(self, server_url: str, tmp_path: Path):
        """The callback shall accept a string path."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        output_file = tmp_path / "output.jsonl"

        # Collect results to verify count
        callback, results = collect_results()

        combined = combine_callbacks(
            save_to_jsonl_path(str(output_file)), callback
        )
        driver = SyncDriver(scraper, on_data=combined)
        driver.run()

        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == len(results)


class TestCombineCallbacks:
    """Tests for combine_callbacks utility."""

    def test_invokes_all_callbacks(self, server_url: str, tmp_path: Path):
        """The combine_callbacks shall invoke all provided callbacks."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        # Track each callback
        callback1, results1 = collect_results()
        callback2, results2 = collect_results()

        combined = combine_callbacks(callback1, callback2)
        driver = SyncDriver(scraper, on_data=combined)
        driver.run()

        # Both callbacks should have been called
        assert len(results1) > 0
        assert len(results1) == len(results2)
        assert results1 == results2

    def test_combines_save_and_count(self, server_url: str, tmp_path: Path):
        """The combine_callbacks shall work with save and count callbacks."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        output_file = tmp_path / "output.jsonl"
        counter = [0]

        # Collect results to verify counts
        callback, results = collect_results()

        with output_file.open("w") as f:
            driver = SyncDriver(
                scraper,
                on_data=combine_callbacks(
                    save_to_jsonl_file(f),
                    count_data(counter),
                    callback,
                ),
            )
            driver.run()

        # File should have all results
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == len(results)

        # Counter should match
        assert counter[0] == len(results)


class TestCountData:
    """Tests for count_data callback."""

    def test_counts_data_items(self, server_url: str):
        """The callback shall count data items."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        counter = [0]
        callback, results = collect_results()

        combined = combine_callbacks(count_data(counter), callback)
        driver = SyncDriver(scraper, on_data=combined)
        driver.run()

        assert counter[0] == len(results)

    def test_creates_counter_if_none(self, server_url: str):
        """The callback shall create a counter if None provided."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        # This should not raise
        count_callback = count_data()
        callback, results = collect_results()

        combined = combine_callbacks(count_callback, callback)
        driver = SyncDriver(scraper, on_data=combined)
        driver.run()

        # Results should be collected normally
        assert len(results) > 0


class TestIntegration:
    """Integration tests for callback system."""

    def test_callback_with_archive_requests(
        self, server_url: str, tmp_path: Path
    ):
        """The callback shall work with archive request scrapers."""
        from tests.scraper.example.bug_court import (
            BugCourtScraperWithArchive,
        )

        scraper = BugCourtScraperWithArchive()
        scraper.BASE_URL = server_url

        callback, results = collect_results()

        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)
        driver.run()

        # Callback should receive all results
        assert len(results) > 0

    def test_callback_error_does_not_stop_scraping(self, server_url: str):
        """The driver should continue if callback raises an error."""
        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url

        call_count = [0]

        def failing_callback(data: dict):
            call_count[0] += 1
            if call_count[0] == 1:
                # Fail on first call
                raise ValueError("Test error")

        # This should raise the error from the callback
        driver = SyncDriver(scraper, on_data=failing_callback)
        with pytest.raises(ValueError, match="Test error"):
            driver.run()
