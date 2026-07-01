# jkent telemetry catalog

Everything jkent measures, and how to read it. This is the reference for the
signals; `../../EN_BANC_OTEL.md` is the host-side setup contract (SDK,
exporters, the `flow_run_id` baggage, the provisioning decision tree).

**API-only.** jkent uses the OpenTelemetry *API* against the global providers
and never configures an SDK. With no SDK installed every instrument and span
below is a no-op — so nothing here emits until the host process (en_banc)
installs an SDK and exporters. See `EN_BANC_OTEL.md`.

- **Meter name:** `jkent.driver`
- **Tracer name:** `jkent.driver`
- **Source:** `jkent/observability/` — `metrics.py` (instruments + labels),
  `tracing.py` (spans), `instrumented_lock.py` (lock), `loop_monitor.py` (lag).

---

## Conventions

**Dimensions (dynamic attributes).** Kept deliberately low-cardinality:

| Attribute | Values | On |
|---|---|---|
| `scraper` | scraper class name | most metrics, request span |
| `step` | continuation/step name | request metrics, request span; on lock metrics only when the lock is taken inside a request |
| `phase` | see [phases](#phases) | `request.duration`, `request.cpu_time` |
| `kind` | `compress` / `train` / `recompress` | compression + compaction metrics |
| `outcome` | see [outcomes](#outcomes) | request span only (**not** a metric dimension) |
| `flow_run_id` | host-provided id (from baggage) | request span + the two per-run gauges only |

`flow_run_id` is high-cardinality, so it rides spans and the per-run gauges but
**never the histograms**. Correlate histogram data to a run in the backend via
`scraper` + time window, or drill via the sampled request spans.

**Resource attributes** (`service.name`, `worker.pool`, `worker.concurrency`,
`worker.host`) are set once by the host on the providers and apply to *every*
metric and span automatically — they are never attached per-instrument here.

**Units.** `s` = seconds (wall unless noted), `1` = dimensionless ratio/count.

---

## Metrics

### Event loop

| Metric | Type | Unit | Attrs | Emitted from |
|---|---|---|---|---|
| `jkent.event_loop.lag` | histogram | s | *(resource only)* | `LoopLagMonitor` (`loop_monitor.py`) |

**The keystone signal.** All Runs in a worker share one asyncio loop; this
samples scheduling latency (measured drift of a fixed-interval sleep). High
values mean something is blocking the loop synchronously (compression, lxml
parsing) and *every* co-resident worker and Run stalls with it. Per-request
wall time is misleading without this — a request's "duration" can be mostly
time spent waiting for the loop while another worker hogs it. Started only by
the host; toggle with `JKENT_OTEL_LOOP_MONITOR=0`.

### Per-request timing

| Metric | Type | Unit | Attrs | Emitted from |
|---|---|---|---|---|
| `jkent.request.duration` | histogram | s | `scraper`, `step`, `phase` | `request_span` (`phase=total`) + `phase()` (`tracing.py`), wired in `worker.py` |
| `jkent.request.cpu_time` | histogram | s | `scraper`, `step`, `phase` | `compress_response` (`compression.py`) |

`request.duration` is recorded once per [phase](#phases) per request, plus a
`phase=total` covering the whole request. The `rate_limiter.gate` slice is the
rate-limiter token wait — a large one means rate-limit-bound, not
throughput-bound.

`request.cpu_time` is on-loop CPU (`time.thread_time`) for synchronous phases
we can measure without pollution from co-scheduled tasks. Today that is
`phase=compress` only (a sync leaf with no `await` inside). A phase whose
`cpu_time` ≈ its `duration` is hogging the loop; one whose `cpu_time` ≪
`duration` is awaiting I/O.

### Database lock

| Metric | Type | Unit | Attrs | Emitted from |
|---|---|---|---|---|
| `jkent.db.lock.wait` | histogram | s | `scraper`, `step`? | `InstrumentedLock.acquire` (`instrumented_lock.py`) |
| `jkent.db.lock.hold` | histogram | s | `scraper`, `step`? | `InstrumentedLock.release` |

The run holds a **single** `asyncio.Lock` serializing all SQLite access
(dequeue, restamp, store, staged flush, dedup, counts). `lock.wait` is nonzero
only under contention — a rising `wait` is the "lock getting fought over".
`lock.hold` shows how long each holder keeps it. (`step?`: present only when
the lock is taken inside a request; absent for dequeue / monitor / seed paths.)

### Compression

| Metric | Type | Unit | Attrs | Emitted from |
|---|---|---|---|---|
| `jkent.compression.duration` | histogram | s | `scraper`, `step`, `kind=compress` | `compress_response` (`compression.py`) |
| `jkent.compression.ratio` | histogram | 1 | `scraper`, `step` | `compress_response` |
| `jkent.compaction.duration` | histogram | s | `scraper`, `step`, `kind` | `train_compression_dict` / `recompress_responses` |

`compression.duration` is per-response zstd time (currently synchronous on the
loop — pair it with `event_loop.lag` and `request.cpu_time{phase=compress}` to
prove whether it is a loop-blocker). `ratio` is compressed/original (lower is
better). `compaction.duration` is the one-shot burst at a step's threshold:
`kind=train` (train a dictionary) and `kind=recompress` (recompress the step's
stored responses) — a single large loop-blocking event, measured separately
from steady-state compression.

### Per-run state (gauges)

| Metric | Type | Unit | Attrs | Emitted from |
|---|---|---|---|---|
| `jkent.worker.active` | gauge | 1 | `scraper`, `flow_run_id` | `ScrapeRun._publish_worker_active` (`run.py`), at spawn/retire |
| `jkent.queue.pending` | gauge | 1 | `scraper`, `flow_run_id` | `ScrapeRun._counted_pending` (`run.py`), each monitor poll |

`worker.active` is the live continuation-worker count (updated promptly on
spawn/retire); `queue.pending` is the backlog (refreshed on the monitor's poll
cycle — coarser). Together they show whether the pool is keeping up with the
queue and whether the monitor is scaling toward `max_workers`.

---

## Spans

One trace tree per request (sampled by the host). httpx / SQLAlchemy /
botocore auto-instrumentation spans nest under the relevant phase span once the
host enables those instrumentors.

| Span | Parent | Attributes / notes |
|---|---|---|
| `jkent.request` | root | `jkent.scraper`, `jkent.step`, `jkent.flow_run_id`, `jkent.outcome` |
| `jkent.rate_limiter.gate` | `jkent.request` | rate-limiter token wait |
| `jkent.transport.resolve` | `jkent.request` | the fetch; httpx client spans nest here |
| `jkent.continuation` | `jkent.request` | response store + scraper continuation; SQLAlchemy spans nest here |

---

## Glossary

### Phases

Values of the `phase` attribute (on `request.duration` / `request.cpu_time`):

| `phase` | Meaning |
|---|---|
| `total` | whole request, dequeue-to-done (duration only) |
| `rate_limiter.gate` | waiting for a rate-limiter token |
| `transport.resolve` | fetching the response (network / browser) |
| `continuation` | storing the response + running the scraper continuation |
| `compress` | the synchronous zstd compress leaf (`cpu_time` only) |

### Outcomes

Values of `jkent.outcome` on the request span:

| `outcome` | Meaning |
|---|---|
| `ok` | request completed and continuation ran |
| `halt` | `RequestFailedHalt` — propagated, stops the run |
| `skip` | skipped by an `on_transient_exception` callback |
| `transient` | transient failure → retried (or failed after max backoff) |
| `speculation_http` | persistent HTTP on a speculative probe (recorded as a speculation outcome) |
| `persistent_http` | classifier said the status is persistent → no retry |
| `error` | unexpected exception → marked failed, error row stored |

---

## Reading them together

Short version (full decision tree in `EN_BANC_OTEL.md` §5):

- **High `event_loop.lag` + high `request.cpu_time{phase=compress}`** → sync
  compression is blocking the shared loop → offload it / reduce per-loop
  concurrency; adding workers to the same loop makes it worse.
- **Low lag, high `db.lock.wait`** → the single per-run DB lock is the ceiling;
  more workers or flow-run concurrency won't help a per-run lock.
- **Low lag, low lock wait, large `request.duration{phase=rate_limiter.gate}`**
  → rate-limit-bound; more workers are pointless.
- **All low, throughput still capped** → I/O-bound → raise `max_workers` /
  worker concurrency. Cheapest win.

---

## Toggles

- `OTEL_SDK_DISABLED=true` (host) — whole SDK no-op; jkent's API calls stay but
  cost nothing.
- `JKENT_OTEL_LOOP_MONITOR=0` — disable just the event-loop-lag sampler.
