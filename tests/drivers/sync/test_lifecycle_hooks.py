"""Tests for Step 14: Lifecycle Hooks.

This module tests the on_run_start and on_run_complete lifecycle hooks
that track scraper runs.

Key behaviors tested:
- on_run_start is called at the beginning of run()
- on_run_complete is called at the end of run() (in finally block)
- Callbacks receive correct parameters (scraper_name, status, counts)
- Status reflects outcome: "completed" for success, "error" for exceptions
- Counters track data_count and request_count accurately
- Hooks fire even when exceptions occur
"""

from collections.abc import Generator
from pathlib import Path

from kent.common.exceptions import (
    HTMLStructuralAssumptionException,
)
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
)
from kent.driver.sync_driver import SyncDriver
from tests.utils import collect_results


class TestRunStartHook:
    """Tests for the on_run_start lifecycle hook."""

    def test_run_start_hook_is_called(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_run_start hook shall be called at the beginning of run()."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"text": response.text})

        # Track hook invocations
        hook_calls: dict[str, bool | str] = {}

        def on_run_start_callback(scraper_name: str) -> None:
            hook_calls["called"] = True
            hook_calls["scraper_name"] = scraper_name

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_run_start=on_run_start_callback,
        )

        driver.run()

        # Verify hook was called
        assert hook_calls["called"] is True
        assert hook_calls["scraper_name"] == "SimpleScraper"

    def test_run_start_receives_correct_scraper_name(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_run_start hook shall receive the scraper class name."""

        class MyCustomScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"text": response.text})

        # Track scraper name
        scraper_names = []

        def on_run_start_callback(scraper_name: str) -> None:
            scraper_names.append(scraper_name)

        scraper = MyCustomScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_run_start=on_run_start_callback,
        )

        driver.run()

        assert len(scraper_names) == 1
        assert scraper_names[0] == "MyCustomScraper"


class TestRunCompleteHook:
    """Tests for the on_run_complete lifecycle hook."""

    def test_run_complete_hook_is_called(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_run_complete hook shall be called at the end of run()."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"text": response.text})

        # Track hook invocations
        hook_calls: dict[str, bool | str | int | Exception | None] = {}

        def on_run_complete_callback(
            scraper_name: str,
            status: str,
            error: Exception | None,
        ) -> None:
            hook_calls["called"] = True
            hook_calls["scraper_name"] = scraper_name
            hook_calls["status"] = status
            hook_calls["error"] = error

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_run_complete=on_run_complete_callback,
        )

        driver.run()

        # Verify hook was called
        assert hook_calls["called"] is True
        assert hook_calls["scraper_name"] == "SimpleScraper"
        assert hook_calls["status"] == "completed"
        assert hook_calls["error"] is None

    def test_run_complete_status_is_completed_on_success(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_run_complete hook shall receive status='completed' on successful run."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"text": response.text})

        statuses = []

        def on_run_complete_callback(
            scraper_name: str,
            status: str,
            error: Exception | None,
        ) -> None:
            statuses.append(status)

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_run_complete=on_run_complete_callback,
        )

        driver.run()

        assert len(statuses) == 1
        assert statuses[0] == "completed"

    def test_run_complete_status_is_error_on_exception(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_run_complete hook shall receive status='error' when exception occurs."""

        class FailingScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                raise HTMLStructuralAssumptionException(
                    selector="//test",
                    selector_type="xpath",
                    description="Intentional error for testing",
                    expected_min=1,
                    expected_max=1,
                    actual_count=0,
                    request_url=response.url,
                )

        hook_data: dict[str, str | Exception | None] = {}

        def on_run_complete_callback(
            scraper_name: str,
            status: str,
            error: Exception | None,
        ) -> None:
            hook_data["status"] = status
            hook_data["error"] = error

        scraper = FailingScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_run_complete=on_run_complete_callback,
        )

        # Run should raise exception
        try:
            driver.run()
            raise AssertionError(
                "Should have raised HTMLStructuralAssumptionException"
            )
        except HTMLStructuralAssumptionException:
            pass

        # Verify hook was called with error status
        assert hook_data["status"] == "error"
        assert hook_data["error"] is not None
        assert isinstance(
            hook_data["error"], HTMLStructuralAssumptionException
        )

    def test_run_complete_hook_fires_in_finally_block(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_run_complete hook shall fire even when exceptions occur."""

        class FailingScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                raise HTMLStructuralAssumptionException(
                    selector="//test",
                    selector_type="xpath",
                    description="Intentional error for testing",
                    expected_min=1,
                    expected_max=1,
                    actual_count=0,
                    request_url=response.url,
                )

        hook_called = {"called": False}

        def on_run_complete_callback(
            scraper_name: str,
            status: str,
            error: Exception | None,
        ) -> None:
            hook_called["called"] = True

        scraper = FailingScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_run_complete=on_run_complete_callback,
        )

        # Run should raise exception, but hook should still fire
        try:
            driver.run()
        except HTMLStructuralAssumptionException:
            pass

        assert hook_called["called"] is True


class TestBothHooksTogether:
    """Tests for using both on_run_start and on_run_complete together."""

    def test_both_hooks_fire_in_order(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_run_start hook shall fire before on_run_complete."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"text": response.text})

        # Track call order
        call_order = []

        def on_run_start_callback(scraper_name: str) -> None:
            call_order.append("start")

        def on_run_complete_callback(
            scraper_name: str,
            status: str,
            error: Exception | None,
        ) -> None:
            call_order.append("complete")

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_run_start=on_run_start_callback,
            on_run_complete=on_run_complete_callback,
        )

        driver.run()

        # Verify order
        assert call_order == ["start", "complete"]
