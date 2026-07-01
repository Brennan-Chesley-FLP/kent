"""jkent observability — OpenTelemetry instrumentation, API-only.

This package uses the OpenTelemetry **API** only; it never configures an SDK,
exporter, or sampler. With no SDK installed (the default, and always the case
when jkent is imported by juriscraper) every span and metric is a no-op, so the
instrumentation costs effectively nothing. The host process (en_banc) owns SDK
setup and exporter policy — see ``EN_BANC_OTEL.md`` at the repo root.

Public surface:

* :func:`instruments` / :func:`labeled` / :func:`current_labels` — metrics.
* :func:`request_span` / :func:`phase` / :func:`flow_run_id` — tracing.
* :class:`LoopLagMonitor` — the shared-loop lag sampler (started by the host).
"""

from __future__ import annotations

from jkent.observability.instrumented_lock import InstrumentedLock
from jkent.observability.loop_monitor import LoopLagMonitor
from jkent.observability.metrics import (
    current_labels,
    instruments,
    labeled,
)
from jkent.observability.tracing import (
    flow_run_id,
    phase,
    request_span,
    tracer,
)

__all__ = [
    "InstrumentedLock",
    "LoopLagMonitor",
    "current_labels",
    "flow_run_id",
    "instruments",
    "labeled",
    "phase",
    "request_span",
    "tracer",
]
