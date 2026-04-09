"""Shared parameter models for scraper @entry functions.

These Pydantic BaseModel subclasses define common parameter types
that scrapers can use in their @entry-decorated entry points.

Example::

    from kent.common.param_models import DateRange, SpeculativeRange

    @entry(Docket)
    def search_by_date(self, date_range: DateRange) -> Generator[...]:
        ...

    @entry(Docket)
    def fetch_by_id(self, rid: SpeculativeRange) -> Request:
        return Request(url=f"/docket/{rid.number}", ...)
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class DateRange(BaseModel):
    """Date range with start and end bounds.

    Both bounds are inclusive. Used for filtering by date range
    in scraper entry points.

    Attributes:
        start: Start date (inclusive).
        end: End date (inclusive).
    """

    start: date
    end: date


class SpeculativeRange(BaseModel):
    """Speculative parameter for sequential integer ID probing.

    Implements the ``Speculative`` protocol.  Use as a parameter type
    on an ``@entry`` function to enable automatic speculation.

    Seeding starts at ``number`` and goes upward.  IDs below
    ``threshold`` are seeded unconditionally (non-speculative);
    IDs at or above ``threshold`` enter the gap-based tracking window.

    Attributes:
        number: Starting integer ID (the floor for seeding).
        speculate: Whether to run adaptive extension/tracking.
            Set False to seed the non-speculative range only.
        threshold: IDs >= threshold are evaluated for success/failure.
            IDs below threshold are seeded unconditionally.
        gap: Max consecutive failures before stopping.
            Also the size of the initial speculative window.
            Set 0 for frozen ranges (no speculative seeding).

    Example::

        @entry(CaseData)
        def fetch_case(self, rid: SpeculativeRange) -> Request:
            return Request(
                request=HTTPRequestParams(url=f"/case/{rid.number}"),
                continuation=self.parse_case,
            )

        # seed_params: [{"fetch_case": {"rid": {"number": 1, "gap": 20}}}]
    """

    number: int
    speculate: bool = True
    threshold: int = 0
    gap: int = 10

    def should_speculate(self) -> bool:
        return self.speculate

    def to_int(self) -> int:
        return self.number

    def from_int(self, n: int) -> SpeculativeRange:
        return SpeculativeRange(
            number=n,
            speculate=self.speculate,
            threshold=self.threshold,
            gap=self.gap,
        )

    def check_success(self) -> bool:
        return self.number >= self.threshold

    def max_gap(self) -> int:
        return self.gap


class YearlySpeculativeRange(BaseModel):
    """Speculative parameter for year-partitioned integer ID probing.

    Implements the ``Speculative`` protocol.  Like ``SpeculativeRange``
    but includes a ``year`` field for scrapers that partition IDs by year
    (e.g. docket numbers of the form ``2025-00123``).

    Seeding starts at ``number`` and goes upward.  Supply one template
    per year via ``seed_params``.

    Attributes:
        year: The calendar year for this partition.
        number: Starting integer ID (the floor for seeding).
        speculate: Whether to run adaptive extension/tracking.
        threshold: IDs >= threshold are evaluated for success/failure.
        gap: Max consecutive failures before stopping.

    Example::

        @entry(CaseData)
        def fetch_case(self, case_id: YearlySpeculativeRange) -> Request:
            return Request(
                request=HTTPRequestParams(
                    url=f"/cases/{case_id.year}/{case_id.number}"
                ),
                continuation=self.parse_case,
            )

        # seed_params: [
        #     {"fetch_case": {"case_id": {"year": 2024, "number": 1, "gap": 0, "threshold": 4000}}},
        #     {"fetch_case": {"case_id": {"year": 2025, "number": 1, "gap": 15}}},
        # ]
    """

    year: int
    number: int
    speculate: bool = True
    threshold: int = 0
    gap: int = 10

    def should_speculate(self) -> bool:
        return self.speculate

    def to_int(self) -> int:
        return self.number

    def from_int(self, n: int) -> YearlySpeculativeRange:
        return YearlySpeculativeRange(
            year=self.year,
            number=n,
            speculate=self.speculate,
            threshold=self.threshold,
            gap=self.gap,
        )

    def check_success(self) -> bool:
        return self.number >= self.threshold

    def max_gap(self) -> int:
        return self.gap
