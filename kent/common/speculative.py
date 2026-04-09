"""Speculative protocol for entry point parameter models.

Defines a runtime-checkable Protocol that Pydantic BaseModel classes can
implement to power the speculation system. Instead of kent owning the
speculation configuration classes, scraper
authors define their own Pydantic models with speculation semantics.

Example::

    class DocketId(BaseModel):
        year: int
        number: int
        speculate: bool = True
        threshold: int = 0
        gap: int = 3

        def should_speculate(self) -> bool:
            return self.speculate

        def to_int(self) -> int:
            return self.number

        def from_int(self, n: int) -> DocketId:
            return DocketId(
                year=self.year, number=n, speculate=self.speculate,
                threshold=self.threshold, gap=self.gap,
            )

        def check_success(self) -> bool:
            return self.number >= self.threshold

        def max_gap(self) -> int:
            return self.gap
"""

from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

T = TypeVar("T", covariant=True)


@runtime_checkable
class Speculative(Protocol[T]):
    """Protocol for entry-point parameter models that support speculation.

    When an ``@entry`` function has a parameter whose type implements this
    protocol, the driver detects it automatically and runs the speculation
    loop: seeding requests, tracking success/failure, and adaptively
    extending or stopping based on gap thresholds.

    Methods:
        should_speculate: Whether the driver should run the adaptive
            speculation loop (extension + gap-based stopping). When False
            the driver seeds non-speculative requests only (phase 1) and
            stops.
        to_int: Returns the integer representation of this instance. On the
            template, this is the **starting point** for seeding (the floor).
        from_int: Creates a new instance for integer ID *n*, preserving all
            other fields (year, config, etc.) from the template.
        check_success: Whether the instance at this ID should be evaluated
            for success/failure. The driver seeds upward from ``to_int()``
            — while this returns False, requests are seeded as non-speculative
            (unconditional). Once it returns True, the non-speculative phase
            ends and the speculative window begins.
        max_gap: Maximum consecutive failures to tolerate before stopping.
            Also controls the size of the initial speculative window.
            Return 0 for frozen ranges (no speculative seeding).
    """

    def should_speculate(self) -> bool: ...

    def to_int(self) -> int: ...

    def from_int(self, n: int) -> T: ...

    def check_success(self) -> bool: ...

    def max_gap(self) -> int: ...
