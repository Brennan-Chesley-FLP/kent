"""Regression tests for persistent-HTTP-on-speculative-request handling.

When a request born from speculation machinery returns a persistent-
classified HTTP code (e.g. 500 for a record that doesn't exist), the
framework should treat it as a speculation outcome, not an error: the
errors table stays clean, ``speculation_tracking.consecutive_failures``
increments, the request is marked completed, and its continuation does
not run.

These tests cover:
- The request-manager routing (``SpeculationHTTPFailure`` replaces
  ``PersistentHTTPResponseException`` when ``is_speculative`` is True).
- The worker branch that converts the exception into a synthetic
  ``Response`` and feeds it to ``_track_speculation_outcome``.
- End-to-end: the Nevada backfill shape
  (``min=X, soft_max=X+1, should_advance=False, gap=1``) with a 500
  response stub produces zero error rows.
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import BaseModel

from kent.common.decorators import entry, step
from kent.common.exceptions import (
    PersistentHTTPResponseException,
    SpeculationHTTPFailure,
)
from kent.common.request_manager import SyncRequestManager
from kent.data_types import (
    BaseRequest,
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
    ScraperYield,
)
from kent.driver.persistent_driver.persistent_driver import PersistentDriver
from kent.driver.persistent_driver.testing import MockRequestManager

# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


class _BackfillId(BaseModel):
    """Speculative backfill param — Nevada shape."""

    min: int
    soft_max: int = 0
    should_advance: bool = True
    gap: int = 1

    def seed_range(self) -> range:
        return range(self.min, self.soft_max)

    def from_int(self, n: int) -> _BackfillId:
        return _BackfillId(
            min=n,
            soft_max=self.soft_max,
            should_advance=self.should_advance,
            gap=self.gap,
        )

    def max_gap(self) -> int:
        return self.gap


class _BackfillScraper(BaseScraper[dict]):
    """Speculative entry fetching by internal id."""

    @entry(dict)
    def fetch_by_id(self, rid: _BackfillId) -> Request:
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"https://example.com/records/{rid.min}",
            ),
            continuation="parse_record",
        )

    parse_called_with: list[int] = []  # type: ignore[misc]

    @step
    def parse_record(
        self, response: Response
    ) -> Generator[ScraperYield, None, None]:
        rid = int(response.url.rsplit("/", 1)[-1])
        type(self).parse_called_with.append(rid)
        yield ParsedData({"rid": rid})


# --------------------------------------------------------------------------
# Request manager routing
# --------------------------------------------------------------------------


def _mock_httpx_response(status_code: int) -> Mock:
    m = Mock()
    m.status_code = status_code
    m.headers = {}
    m.content = b"oops"
    m.text = "oops"
    return m


def _fake_baserequest(
    url: str = "https://example.com/",
    is_speculative: bool = False,
) -> BaseRequest:
    req: BaseRequest = Request(
        request=HTTPRequestParams(method=HttpMethod.GET, url=url),
        continuation="noop",
    )
    if is_speculative:
        req = req.speculative("state_key", 0, 1)  # type: ignore[attr-defined]
    return req


class TestRequestManagerSpeculativeRouting:
    def test_persistent_on_non_speculative_raises_persistent(self) -> None:
        rm = SyncRequestManager(scraper=BaseScraper)
        rm._client.request = Mock(return_value=_mock_httpx_response(500))
        with pytest.raises(PersistentHTTPResponseException):
            rm.resolve_request(_fake_baserequest())

    def test_persistent_on_speculative_raises_speculation_failure(
        self,
    ) -> None:
        rm = SyncRequestManager(scraper=BaseScraper)
        rm._client.request = Mock(return_value=_mock_httpx_response(500))
        with pytest.raises(SpeculationHTTPFailure) as ei:
            rm.resolve_request(_fake_baserequest(is_speculative=True))
        assert ei.value.status_code == 500
        # Not a PersistentHTTPResponseException — the worker must be able
        # to dispatch on SpeculationHTTPFailure without falling into the
        # persistent branch.
        assert not isinstance(ei.value, PersistentHTTPResponseException)

    def test_transient_on_speculative_still_raises_transient(self) -> None:
        """Transient codes aren't rerouted for speculative requests —
        they still retry via the normal TransientException path."""
        from kent.common.exceptions import HTMLResponseAssumptionException

        rm = SyncRequestManager(scraper=BaseScraper)
        rm._client.request = Mock(return_value=_mock_httpx_response(503))
        with pytest.raises(HTMLResponseAssumptionException):
            rm.resolve_request(_fake_baserequest(is_speculative=True))


# --------------------------------------------------------------------------
# Worker + DB end-to-end
# --------------------------------------------------------------------------


class TestWorkerSpeculationFailureBranch:
    async def test_nevada_backfill_500_produces_speculation_outcome(
        self, tmp_path: Path
    ) -> None:
        """Backfill one known-missing ID: no error row, speculation state
        bumped, continuation never invoked."""
        _BackfillScraper.parse_called_with = []
        db_path = tmp_path / "test.db"
        target_url = "https://example.com/records/29498"

        manager = MockRequestManager()
        manager.add_error(
            target_url,
            SpeculationHTTPFailure(500, target_url),
        )

        async with PersistentDriver.open(
            _BackfillScraper(),
            db_path,
            enable_monitor=False,
            request_manager=manager,
            seed_params=[
                {
                    "fetch_by_id": {
                        "rid": {
                            "min": 29498,
                            "soft_max": 29499,
                            "should_advance": False,
                            "gap": 1,
                        }
                    }
                }
            ],
        ) as driver:
            await driver.run(setup_signal_handlers=False)

            import sqlalchemy as sa

            async with driver.db._session_factory() as session:
                req_rows = (
                    await session.execute(
                        sa.text(
                            "SELECT status, retry_count, is_speculative "
                            "FROM requests"
                        )
                    )
                ).all()
                err_rows = (
                    await session.execute(
                        sa.text("SELECT COUNT(*) FROM errors")
                    )
                ).scalar_one()
                spec_rows = (
                    await session.execute(
                        sa.text(
                            "SELECT consecutive_failures "
                            "FROM speculation_tracking"
                        )
                    )
                ).all()

        assert len(req_rows) == 1
        status, retry_count, is_speculative = req_rows[0]
        assert status == "completed"
        assert retry_count == 0
        assert is_speculative == 1
        assert err_rows == 0
        # Speculation state was touched — at least one failure recorded.
        assert spec_rows and spec_rows[0][0] >= 1
        # Continuation never ran (no parse_record invocation).
        assert _BackfillScraper.parse_called_with == []
