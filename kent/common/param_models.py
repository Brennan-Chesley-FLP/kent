"""Shared parameter models for scraper @entry functions.

These Pydantic BaseModel subclasses define common parameter types
that scrapers can use in their @entry-decorated entry points.

Example::

    from kent.common.param_models import DateRange

    @entry(Docket)
    def search_by_date(self, date_range: DateRange) -> Generator[...]:
        ...
"""

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
