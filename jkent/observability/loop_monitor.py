"""Event-loop lag monitor — the keystone provisioning signal.

All Runs in a Prefect worker share one asyncio loop, so per-request wall time
is misleading: a request may spend most of its "duration" waiting for the loop
while another worker compresses or parses synchronously. This monitor measures
that directly. It sleeps for a fixed interval and records how much longer than
the interval it actually took to wake up; the excess is time the loop spent
unable to schedule this task — i.e. blocked on synchronous work somewhere.

Cheap by construction (one short timer), and started only by the host process
(en_banc); it is off unless explicitly started. Disable with
``JKENT_OTEL_LOOP_MONITOR=0``.
"""

from __future__ import annotations

import asyncio
import os

from jkent.observability.metrics import instruments

DEFAULT_INTERVAL = 0.2


def _enabled() -> bool:
    return os.environ.get("JKENT_OTEL_LOOP_MONITOR", "1") not in (
        "0",
        "false",
        "False",
    )


class LoopLagMonitor:
    """Samples event-loop scheduling latency into ``jkent.event_loop.lag``.

    Usage (host process, once)::

        monitor = LoopLagMonitor()
        monitor.start()
        ...
        await monitor.stop()
    """

    def __init__(self, interval: float = DEFAULT_INTERVAL) -> None:
        self._interval = interval
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Launch the sampling task (no-op if disabled or already running)."""
        if not _enabled() or self._task is not None:
            return
        self._task = asyncio.ensure_future(self._run())

    async def stop(self) -> None:
        """Cancel and await the sampling task."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        lag = instruments().loop_lag
        while True:
            expected = loop.time() + self._interval
            await asyncio.sleep(self._interval)
            # Negative drift is impossible in principle; floor at 0 to guard
            # against clock granularity producing a tiny negative.
            lag.record(max(0.0, loop.time() - expected))
