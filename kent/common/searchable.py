"""Speculative function configuration for scraper drivers.

This module provides configuration for speculative scraping functions,
controlling how far the driver will speculate beyond known IDs.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SpeculateFunctionConfig:
    """Configuration for a single speculative entry function.

    Holds the definite_range and plus settings for a speculative function.
    These control how far the driver will speculate beyond known IDs.

    Attributes:
        definite_range: Tuple (start, end) of IDs to fetch with certainty.
            Defaults to (1, highest_observed) from decorator metadata.
        plus: Number of consecutive failures to tolerate beyond highest_successful_id.
            Defaults to largest_observed_gap from decorator metadata.
    """

    definite_range: tuple[int, int] | None = None
    plus: int | None = None
