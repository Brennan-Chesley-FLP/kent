"""Speculation configuration types for the @entry decorator.

Defines SimpleSpeculation and YearlySpeculation, which replace the old
`speculative=True` boolean flag with structured configuration objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta


@dataclass(frozen=True)
class SimpleSpeculation:
    """Speculation config for single-integer-param entry points.

    Used with @entry(speculative=SimpleSpeculation(...)) for scrapers
    that probe sequential integer IDs (e.g., case numbers, record IDs).

    Attributes:
        highest_observed: Highest ID known to exist.
        largest_observed_gap: Largest gap observed in the ID sequence.
            Also controls how many consecutive failures before stopping.
        observation_date: Date when the metadata was last verified.
    """

    highest_observed: int = 1
    largest_observed_gap: int = 10
    observation_date: date | None = None


@dataclass(frozen=True)
class YearPartition:
    """Configuration for a single year within YearlySpeculation.

    Attributes:
        year: The calendar year.
        number: Tuple (start, end) defining the range of the number param.
        frozen: If True, this is a backfill-only range with no adaptive
            extension. If False, the driver will extend past the upper
            bound when it finds successes.
    """

    year: int
    number: tuple[int, int]
    frozen: bool = False


@dataclass(frozen=True)
class YearlySpeculation:
    """Speculation config for year-partitioned two-param entry points.

    Used with @entry(speculative=YearlySpeculation(...)) for scrapers
    that probe docket numbers of the form {prefix}-{year}-{number}.

    The entry function MUST have exactly two int params: one named
    ``year`` and one other (the speculative axis).

    Each year gets an independent SpeculationState in the DB, keyed
    as ``func_name:year``.

    Attributes:
        backfill: Year partitions to seed. Frozen partitions are
            backfill-only; non-frozen partitions extend adaptively.
        trailing_period: After Jan 1 of the current year, keep probing
            the previous year for this duration before dropping it.
        largest_observed_gap: Max consecutive failures before stopping
            a non-frozen partition.
    """

    backfill: tuple[YearPartition, ...] = ()
    trailing_period: timedelta = field(
        default_factory=lambda: timedelta(days=60)
    )
    largest_observed_gap: int = 10


SpeculationType = SimpleSpeculation | YearlySpeculation | None
