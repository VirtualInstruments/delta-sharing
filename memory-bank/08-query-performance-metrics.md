# Query Performance Metrics — Design

Status: **P1 implemented** (ZING-45330, under ZING-45093); P2/P3 still proposed. Metrics are
exported to Google Cloud Monitoring and are **off by default** -- see section 6.2 for the
two-step rollout. Sections 4.4, 4.5 and the P2/P3 phases remain design only.

Goal: customers issue very different query shapes — plain snapshot reads, structured-streaming
incremental reads, CDF reads, and metadata/version polls — and only some of them are slow. This
document defines the metrics needed to tell *which shape* is slow, *which stage* of that shape burns
the time, and *whether the cause is our work or the client's request size*.

---

## 1. Query taxonomy

Latency has to be sliced by query class, because the code paths behind them barely overlap.

| Class | Wire request | Distinguishing params | Server path |
|-------|--------------|-----------------------|-------------|
| `version` | `GET .../tables/{t}/version`, `HEAD .../tables/{t}` | — | Kernel (`DeltaSharedTableKernel.getTableVersion`) |
| `metadata` | `GET .../tables/{t}/metadata` | — | Kernel, `query(includeFiles=false)` |
| `snapshot` | `POST .../tables/{t}/query` | no `predicateHints`, no `maxFiles`, no `startingVersion`, no `pageToken` | **Kernel** (`DeltaSharedTableKernel.query`) |
| `snapshot_filtered` | `POST .../query` | any of `predicateHints` / `maxFiles` / `pageToken` set | **Standalone** (`DeltaSharedTable.query`, `includeFiles` branch) |
| `snapshot_asof` | `POST .../query` | `version` or `timestamp` set | Standalone or Kernel depending on the other params |
| `incremental` (streaming) | `POST .../query` | `startingVersion` (+ optional `endingVersion`) | Standalone `queryDataChangeSinceStartVersion` |
| `cdf` | `GET .../tables/{t}/changes` | `startingVersion`/`startingTimestamp` … | Standalone `queryCDF` → `DeltaSharingCDCReader` |
| `page` (continuation) | any of the above | `pageToken` present | same path, resumed |
| `catalog` | `/shares`, `/schemas`, `/tables`, `/all-tables` | — | `SharedTableManager` (config-only, no I/O) |

The routing split matters and is easy to miss:
[`DeltaSharingService.scala:744-751`](../server/src/main/scala/io/delta/sharing/server/DeltaSharingService.scala#L744)
sends the *simplest* query to the Kernel implementation and everything else to Delta Standalone. So
the most common customer query runs through the path with the **least** instrumentation (see §3).

Spark structured streaming against a shared table produces a repeating pattern per micro-batch:
`version` poll → `query` with `startingVersion` (or `changes` when `readChangeFeed` is on) → paged
continuations. A streaming customer's latency is therefore dominated by `version` + `incremental`,
while a BI/ad-hoc customer's is dominated by `snapshot`.

---

## 2. Request lifecycle and stage boundaries

### 2.1 Snapshot query (standalone path)

| Stage | Meaning | Where |
|-------|---------|-------|
| `deltaLogUpdate` | `deltaLog.update()` — lists `_delta_log`, replays new commits | [`DeltaSharedTableLoader.scala:88-91`](../server/src/main/scala/io/delta/sharing/server/DeltaSharedTableLoader.scala#L88) |
| `snapshotResolve` | pick snapshot: latest / `getSnapshotForVersionAsOf` / `…ForTimestampAsOf` | [`DeltaSharedTable.scala:396-429`](../server/src/main/scala/io/delta/standalone/internal/DeltaSharedTable.scala#L396) |
| `replayOrPrepare` | materialize `state.activeFiles`, json/partition predicate filtering, page-token cut, response action build | [`DeltaSharedTable.scala:467-571`](../server/src/main/scala/io/delta/standalone/internal/DeltaSharedTable.scala#L467) |
| `signing` | `fileSigner.sign` per file (accumulated, excluded from `replayOrPrepare`) | [`DeltaSharedTable.scala:530-536`](../server/src/main/scala/io/delta/standalone/internal/DeltaSharedTable.scala#L530) |
| `serialize/stream` | JSON-ify actions, stream NDJSON to client | `streamingOutput`, [`DeltaSharingService.scala:867+`](../server/src/main/scala/io/delta/sharing/server/DeltaSharingService.scala#L867) |
| `egressLog` | access-log/pricing-tier emission (buffered, should be ~0) | `emitQueryEgressMetric` |

### 2.2 Incremental / streaming query

| Stage | Meaning | Where |
|-------|---------|-------|
| `deltaLogUpdate` | as above | loader |
| `snapshotResolve` | resolve snapshot at `startingVersion` | `DeltaSharedTable.scala:396` |
| `timestampIndex` | `getTimestampsByVersion(start, end+1)` — one commit-file stat/read per version | [`DeltaSharedTable.scala:634-642`](../server/src/main/scala/io/delta/standalone/internal/DeltaSharedTable.scala#L634) |
| `changeReplay` | `deltaLog.getChanges(start)` iteration, action conversion, page cut | [`DeltaSharedTable.scala:678-739`](../server/src/main/scala/io/delta/standalone/internal/DeltaSharedTable.scala#L678) |
| `signing` | `parallelSign` over the batch (32-thread shared pool) | [`DeltaSharedTable.scala:740-743`](../server/src/main/scala/io/delta/standalone/internal/DeltaSharedTable.scala#L740) |

`timestampIndex` and `changeReplay` scale with **versions iterated**, not with bytes. A streaming
consumer that falls behind by thousands of commits pays here — this is the classic
"streaming query got slow" shape, and it is invisible without a `versionsIterated` metric next to
the latency.

### 2.3 CDF query

Stages: `deltaLogUpdate` → `protocolSnapshot` (`getSnapshotForVersionAsOf`) → `timestampIndex` →
`getChanges` (materialize) → `cdcSpecBuild` → `signing` → `responseBuild`. Boundaries exist as
`CdfQueryTimings` ([`DeltaSharedTableProtocol.scala:30-42`](../server/src/main/scala/io/delta/sharing/server/DeltaSharedTableProtocol.scala#L30)),
fed from `DeltaSharingCDCReader`.

### 2.4 Kernel path (snapshot / metadata / version)

Stages: `getTableAndEngine` → `getSharedTableSnapshot` (protocol+metadata resolve) →
`scanBuilder`/predicate build → `getScanFiles` batch iteration → **serial** `fileSigner.sign` per
file (`DeltaSharedTableKernel.scala:710`, plus a second `sign` per deletion vector at `:724`).

Two things to note here, both of which the metrics are meant to expose:
- The Kernel table object is constructed fresh per request
  ([`DeltaSharedTableLoader.scala:56-69`](../server/src/main/scala/io/delta/sharing/server/DeltaSharedTableLoader.scala#L56)) —
  it does **not** use `deltaSharedTableCache`, and reports `updateNs = 0`.
- Signing on this path is serial, unlike `parallelSign` on the standalone path. For a wide snapshot
  this is the single most likely explanation of "simple queries are slow".

---

## 3. What exists today, and the gaps

**Today (`perfLoggingEnabled: true`):**
- One INFO line per `query` / `changes` request with the stage breakdown above
  (`logTableQueryComplete` / `logCdfRequestComplete`,
  [`DeltaSharingService.scala:360-428`](../server/src/main/scala/io/delta/sharing/server/DeltaSharingService.scala#L360)).
- A WARN line when wall time exceeds 75% of `requestTimeoutSeconds` (request level) or when
  in-table work does (`warnIfNearRequestTimeout`, `DeltaSharedTable.scala:1095`).
- A `deltaLog.update took Nms` line from the loader.
- The egress access-log Delta table (`access_log_br__system`): per-request bytes, tenant, share,
  table, pricing tier — **no latency fields**. See [06](06-egress-monitoring.md), [07](07-access-log-table-reference.md).

**Since P1** (§6.2), the metrics below also exist, once `metrics.enabled` is turned on. The gap
table that follows is the pre-P1 state, kept because it is what the metric set was designed
against; the "closed by" column says where each gap now stands.

**Gaps:**

| # | Gap | Consequence | Status |
|---|-----|-------------|--------|
| G1 | No aggregated/queryable time series — only free-text log lines | Can't get p95 per tenant, can't chart a trend, can't alert | closed (P1) |
| G2 | Kernel path returns `QueryResult(..., timings = None)` (`DeltaSharedTableKernel.scala:380`) | The most common query class has only a total, no stage breakdown | open (P2) |
| G3 | `version` / `metadata` / catalog endpoints have no timing at all | Streaming poll latency invisible | request latency closed (P1); no stage breakdown |
| G4 | No work-volume counters (files scanned vs. returned vs. signed, versions iterated, response bytes) | Can't tell "we are slow" from "the client asked for a lot" | closed (P1) |
| G5 | No saturation signals: signing pool queue, Armeria event-loop lag, JVM/GC, table-cache hit rate | Latency spikes can't be attributed to contention | open (P2) |
| G6 | No outcome dimension (status class, error code, timeout, client disconnect) | Failed/timed-out requests silently excluded from any latency view | closed (P1) |
| G7 | Serialization/streaming time is inside neither the stage totals nor `warnIfNearRequestTimeout` | Large responses under-attributed | partly closed (P1: response bytes + TTFB) |
| G8 | `signingNs` is summed wall time across 32 threads on the standalone path | Looks like the dominant stage even when it isn't; needs a per-file view | open (P2) |

---

## 4. Proposed metric catalog

Names use the `delta_sharing_` prefix, Prometheus conventions (`_seconds`, `_bytes`, `_total`).
"Priority" is the implementation phase from §8.

### 4.1 Request level (golden signals) — P1

| Metric | Type | Labels | Why |
|--------|------|--------|-----|
| `delta_sharing_request_duration_seconds` | histogram | `endpoint`, `query_class`, `outcome`, `paged` | The headline SLI. Buckets: 5ms…300s (see §5) |
| `delta_sharing_requests_total` | counter | `endpoint`, `query_class`, `status_class`, `error_code` | Rate + error rate |
| `delta_sharing_requests_in_flight` | gauge | `endpoint` | Concurrency / queue build-up |
| `delta_sharing_request_timeouts_total` | counter | `endpoint`, `query_class` | Fires with `requestTimeoutSeconds` (300s today) |
| `delta_sharing_request_near_timeout_total` | counter | `endpoint`, `query_class` | Promotes the existing 75% WARN into a countable signal |
| `delta_sharing_response_bytes` | histogram | `endpoint`, `query_class` | Payload size — the main covariate of serialize time |
| `delta_sharing_time_to_first_byte_seconds` | histogram | `endpoint`, `query_class` | Clients time out on TTFB, not on total; the whole response is currently built before the first byte |

### 4.2 Stage latency — P1 (standalone), P2 (kernel)

One metric with a `stage` label rather than one metric per stage, so a dashboard can stack it:

`delta_sharing_query_stage_duration_seconds{stage, query_class, path}` (histogram)

`stage` ∈ `delta_log_update`, `snapshot_resolve`, `protocol_snapshot`, `timestamp_index`,
`change_replay`, `replay_or_prepare`, `predicate_filter`, `cdc_spec_build`, `signing`,
`response_build`, `serialize`, `egress_log`.
`path` ∈ `standalone`, `kernel`.

Closing G2/G3 means Kernel's `query`/`queryCDF`/`getTableVersion` must populate `TableQueryTimings`
(new fields: `engineInitNs`, `scanBuildNs`, `scanFilesNs`, `signingNs`) instead of returning `None`.

Add an explicit residual — `total − Σ stages` — as `stage="unattributed"`. Without it, drift between
wall time and the sum of stages goes unnoticed (today's log lines already have that drift).

### 4.3 Work volume — P1

Latency is meaningless without the size of the request it served.

| Metric | Type | Labels | Why |
|--------|------|--------|-----|
| `delta_sharing_files_scanned` | histogram | `query_class` | Files considered before filtering |
| `delta_sharing_files_returned` | histogram | `query_class` | Files in the response |
| `delta_sharing_files_signed` | histogram | `query_class` | Signing work (= returned + DVs) |
| `delta_sharing_predicate_pruning_ratio` | histogram | `query_class`, `predicate_kind` | `1 − returned/scanned`; shows whether json predicates earn their cost |
| `delta_sharing_versions_iterated` | histogram | `query_class` | The streaming/CDF backlog signal (§2.2) |
| `delta_sharing_version_span` | histogram | `query_class` | `end − start` requested |
| `delta_sharing_snapshot_files_total` | gauge | `share`, `table` | Table growth — the slow-drift cause of snapshot latency regressions |
| `delta_sharing_egress_bytes_total` | counter | `query_class`, `pricing_tier` | Already in the Delta table; useful as a live counter too |

Derived on the dashboard, not stored: **ms per signed file** (`signing / files_signed`) and
**ms per version** (`(timestamp_index + change_replay) / versions_iterated`). These two are the most
useful single numbers for "is it us or the request?" — they are flat when the server is healthy and
spike under GCS or pool contention.

### 4.4 Saturation and dependencies — P2

| Metric | Type | Labels | Why |
|--------|------|--------|-----|
| `delta_sharing_signing_pool_active` / `_queue_depth` | gauge | — | The 32-thread `signingExecutionContext` is shared across *all* tables and requests (`DeltaSharedTable.scala:1214-1223`); it is the most likely cross-tenant interference point |
| `delta_sharing_signing_wait_seconds` | histogram | — | Queue wait, separated from actual sign time — closes G8 |
| `delta_sharing_gcs_operation_duration_seconds` | histogram | `operation` (`sign`, `list`, `read_commit`, `read_checkpoint`), `outcome` | Distinguishes GCS slowness from our own |
| `delta_sharing_gcs_operations_total` | counter | `operation`, `outcome` | Retry/error rate against GCS |
| `delta_sharing_table_cache_requests_total` | counter | `result` (`hit`/`miss`/`load`) | `deltaSharedTableCache` effectiveness (standalone only — kernel bypasses it, G2) |
| `delta_sharing_table_cache_size` | gauge | — | vs. `deltaTableCacheSize` (100) |
| `delta_sharing_access_log_buffer_size` / `_flush_duration_seconds` / `_dropped_total` | gauge/hist/counter | — | The telemetry writer must never become the latency source |
| JVM: heap, GC pause, event-loop lag, thread counts | — | — | Standard; GC pauses show up as unattributable p99 |

### 4.5 Client-behaviour metrics — P3

`delta_sharing_pages_per_query` (histogram), `delta_sharing_page_token_expired_total`,
`delta_sharing_refresh_token_reuse_total`, `delta_sharing_poll_interval_seconds{tenant}`.
These explain *self-inflicted* latency: a client using `maxFiles=100` on a 10k-file table pays 100
round trips, each with its own `deltaLogUpdate`.

---

## 5. Dimensions, cardinality, buckets

**Allowed labels** (deliberately small):

| Label | Values | Cardinality |
|-------|--------|-------------|
| `endpoint` | 9 route templates (never raw paths) | ~9 |
| `query_class` | §1 taxonomy | ~9 |
| `path` | `standalone`, `kernel` | 2 |
| `stage` | §4.2 | ~11 |
| `outcome` | `ok`, `client_error`, `server_error`, `timeout`, `client_disconnect` | 5 |
| `status_class` | `2xx`, `4xx`, `5xx` | 3 |
| `paged` | `first`, `continuation` | 2 |
| `tenant` | derived by `extractTenantId(share)` | **bounded — see below** |
| `pricing_tier` | fixed enum from `GcpPricingTier` | ~15 |

**Rules:**
1. Never label with `share`/`schema`/`table` on latency histograms — table counts are unbounded and
   grow with onboarding. Per-table detail belongs in the access-log Delta table (§6), not in the
   time series. The one exception is the low-frequency gauge `delta_sharing_snapshot_files_total`.
2. `tenant` goes only on: `requests_total`, `request_duration_seconds`, `egress_bytes_total`. If
   tenants exceed ~200, switch to a top-N allowlist plus an `other` bucket.
3. Never label with `queryId`, `pageToken`, client IP, or version numbers.

Rough budget: stage histogram ≈ 11 stages × 9 classes × 2 paths × ~14 buckets ≈ 2.8k series;
request histogram ≈ 9 × 9 × 5 × 2 × 14 ≈ 11k; with tenant on the request histogram at 50 tenants,
plan for ~50–80k series per pod. That is fine for Prometheus, but it is the reason for rule 1.

**Latency buckets** (seconds) — must cover four orders of magnitude, from a `version` poll to a
near-timeout CDF read, and must include a bucket edge *at* the client-visible limits:

```
0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10 30 60 120 300
```

300 = `requestTimeoutSeconds`; 120 = `idleTimeoutSeconds`. Having exact edges there makes
"fraction of requests near timeout" a straight bucket ratio instead of an interpolated quantile.

---

## 6. Where the metrics go

Three candidate paths:

| Option | Pros | Cons |
|--------|------|------|
| **A. Prometheus `/metrics` on the server** (Micrometer/Armeria `PrometheusMeterRegistry`, Armeria has first-class support) | Standard, cheap, real-time, aggregation-friendly, alerting-ready; scrape annotations already the norm in these manifests | Needs a scrape target + a port; loses per-request detail |
| **B. Latency columns on the existing access-log Delta table** | Reuses shipped infrastructure (`AccessLogEmitter`, buffered writer); exact per-request rows; joins to tenant/egress/pricing already there; queryable from BigQuery/Spark | Minutes-late (60s flush + commit), not alertable, only covers `query`/`changes`, and needs a three-repo schema change with a migration the admin tooling does not currently perform (§6.1) |
| **C. OTLP export from the server** | Sidecar `zc-api-proxy` already has OTLP metric env vars (currently `0`); fits a wider platform story | More moving parts; depends on a collector we don't control here |

**Recommendation: A + B.** They answer different questions and the cost of doing both is low
because the stage timings already exist in one place.

- **A** for monitoring and alerting: SLIs, dashboards, alerts, saturation. Expose on a separate
  admin port (e.g. `9090`) so `/metrics` is never reachable through the customer-facing
  VirtualService, and gate it behind `metrics.enabled` in `ServerConfig` (default on, as
  `perfLoggingEnabled` is).
- **B** for forensics and per-tenant reporting: add `durationMs` plus the stage breakdown, plus
  `filesSigned`, `versionsIterated`, `queryClass`, and `outcome` to the access-log row. That makes
  "which tenant's tables are slowest this week" a SQL query, and it needs no new infrastructure.
  All added columns must be nullable and appended — the writer targets a pre-created protocol (1,2)
  table it does not own.
- Keep the existing perf log lines. They are the only per-request signal available when someone is
  debugging a single customer complaint from a pod log, and they cost nothing.

Whatever the transport, the collection point should be one place: a small `QueryMetrics` recorder in
`io.delta.sharing.server.telemetry` that takes the already-existing `QueryResultTimings` plus a
request context, and fans out to the registry, the access-log emitter, and the log line. Do not
scatter `nanoTime()` calls further through the query paths.

### 6.1 Access-log schema change: what it actually takes

The table is owned by [`deltalake-admin`](https://github.com/zenoss/deltalake-admin) and defined
declaratively, so option B is a coordinated change across two repos plus a one-off migration.

**The schema lives in three places that must agree, column-for-column and in order:**

| # | Where | What |
|---|-------|------|
| 1 | `deltalake-admin/templates/access_log_br.json` | The 13-column table definition; validated against `schemas/table_schema.json`. Unpartitioned — there are **no** `year`/`month`/`day` columns |
| 2 | `deltalake-admin/configs/<env>/config-_system.yaml` | Maps template `access_log_br` → table `access_log_br__system`, sets `target_gcs_path` and table properties (`autoOptimize.autoCompact`, 90-day log/deleted-file retention, `zorder_by: [share]`) |
| 3 | `delta-sharing/…/telemetry/DeltaAccessLogWriter.scala:78-93` | A **hardcoded** Parquet `MessageType` — currently an exact match for the template, including which fields are `required` vs `optional` |

**The gap: the admin tooling cannot evolve an existing table's schema.** `create-tables` calls
`DeltaTable.createIfNotExists(...).addColumn(...)`
(`deltalake-admin/src/delta_operations.py:44-68`), and `--update-properties` only issues
`ALTER TABLE … SET TBLPROPERTIES` (`src/cli.py:289-300`). There is no `ADD COLUMNS` path anywhere in
the repo. So editing the template alone changes nothing on an existing table — and, since Delta
validates the requested schema against the existing table on a create, it may well make the next
`create-tables` run fail on the mismatch rather than no-op. Verify that with `--dry-run` against
zing-dev before assuming either behaviour.

**Proposed columns** (all nullable, appended after `isGcpIp`, never reordered):

| Column | Type | Notes |
|--------|------|-------|
| `durationMs` | LONG | Request wall time |
| `queryClass` | STRING | §1 taxonomy |
| `outcome` | STRING | `ok` / `client_error` / `server_error` / `timeout` |
| `stageDeltaLogUpdateMs`, `stageSnapshotResolveMs`, `stageReplayMs`, `stageSigningMs` | LONG | Stage breakdown; leave CDF-only stages out and fold them into `stageReplayMs` to avoid a 10-column tail |
| `filesSigned`, `versionsIterated` | LONG | Work volume (§4.3) |

**Migration order matters.** Delta resolves columns by name against the *table* schema, so:

1. Add the columns to `templates/access_log_br.json`.
2. Migrate each existing table with a one-off `ALTER TABLE delta.\`gs://…/access_log_br__system\`
   ADD COLUMNS (…)` — 5 environments. The clean version of this is a new `evolve-schema`
   step in the admin CLI that diffs template vs. live schema and emits `ADD COLUMNS` for
   additive-only drift; that is worth doing once rather than five ad-hoc Spark shells.
3. Only then deploy the writer with the extra fields. If the writer ships first, its extra Parquet
   columns are silently ignored by readers using the table schema — data written in that window is
   lost, not backfilled.

Old rows read back as `NULL` for the new columns, which is why every added column must be nullable;
`requestType` and the audit fields set that precedent already.

**Two stale claims in [07-access-log-table-reference.md](07-access-log-table-reference.md)** turned
up while checking this, worth knowing before anyone designs against that file: it lists
`year`/`month`/`day` as required columns "computed by the writer" (the template has no such columns
and the writer emits none), and it says Change Data Feed is enabled (no
`delta.enableChangeDataFeed` is set in any `config-_system.yaml`, and the share sets
`historyShared: false`). The header caveat there already defers to
[06-egress-monitoring.md](06-egress-monitoring.md) for the writer.


### 6.2 What P1 actually shipped

Metrics are pushed from the server process to the Cloud Monitoring API on a fixed interval. That
choice is forced rather than preferred: Google Managed Prometheus is switched off on these clusters
(`monitoring_config { managed_prometheus { enabled = false } }` in
`zing-infrastructure/zing/modules/zing/gke.tf`), so exposing a `/metrics` endpoint would leave the
data with nothing to scrape it.

**Where the code lives**

| File | Role |
|------|------|
| `telemetry/QueryClass.scala` | The section 1 taxonomy as constants, plus classification from request params and from the matched route |
| `telemetry/QueryMetrics.scala` | The `QueryMetrics` trait, the Micrometer implementation, metric names, bucket boundaries, and the stage flattening of `QueryResultTimings` |
| `telemetry/MetricsRegistries.scala` | Builds the Cloud Monitoring registry from config; returns a no-op recorder on any failure |
| `telemetry/RequestMetrics.scala` | Armeria decorator for request-level metrics, and the context attributes handlers use to refine the query class |
| `config/ServerConfig.scala` | `MetricsConfig` |

The collection point is single, as section 6 called for: the query paths were not touched. The two
existing completion methods (`logTableQueryComplete`, `logCdfRequestComplete`) now hand their
already-measured `QueryResultTimings` to `queryMetrics.queryCompleted`, and the decorator covers
everything else.

The one query-path change since is the `response_build` stage: `CdfQueryTimings` gained
`responseBuildNs`, covering the classification of replayed actions and the construction of signed
response actions around `signingNs` in `queryCDF`. It was added because the `unattributed`
residual pointed straight at it, and it follows the rule above rather than breaking it -- the
boundary lives in the timing case class, so it reaches the stage metric, the perf log line and the
near-timeout check together. `stage="response_build"` is a real value on
`delta_sharing.query.stage.duration` for the `cdf` class.

**Metrics emitted**, as `custom.googleapis.com/delta_sharing/...` in Cloud Monitoring:

| Metric | Type | Labels |
|--------|------|--------|
| `delta_sharing.request.duration` | distribution | `endpoint`, `query_class`, `outcome` (+ `tenant`) |
| `delta_sharing.requests` | counter | `endpoint`, `query_class`, `status_class` (+ `tenant`) |
| `delta_sharing.requests.in_flight` | gauge | `endpoint` |
| `delta_sharing.request.timeouts` | counter | `endpoint`, `query_class` |
| `delta_sharing.request.near_timeouts` | counter | `endpoint`, `query_class` |
| `delta_sharing.request.time_to_first_byte` | distribution | `endpoint`, `query_class` |
| `delta_sharing.response.bytes` | distribution | `endpoint`, `query_class` |
| `delta_sharing.query.stage.duration` | distribution | `stage`, `query_class`, `engine` |
| `delta_sharing.query.files_signed` | distribution | `query_class` |
| `delta_sharing.query.versions_iterated` | distribution | `query_class` |

The `engine` label (`standalone` / `kernel`) is derived from the same condition that picks the
engine in `listFiles`, so it always names the path actually taken -- which is what makes the
kernel-vs-standalone comparison in section 2.4 measurable rather than theoretical.

Two deviations from the section 4/5 design, both forced by Cloud Monitoring rather than by taste:

- **Cardinality is cheaper than section 5 assumed.** A Micrometer distribution becomes one Cloud
  Monitoring `DISTRIBUTION` time series, not one series per bucket, so the bucket count does not
  multiply anything. The estimate in section 5 is a Prometheus estimate; the real figure here is a
  few hundred series per pod. The `tenant` label is still off by default (`tenantLabelEnabled`),
  because tenants do multiply series and Cloud Monitoring bills per series.
- **`generic_task`, not `global`.** Under `global` every replica writes to the same time series and
  Cloud Monitoring rejects points written more than once per sampling period. With an HPA in front
  of this deployment that would silently discard most of the data. Each pod therefore gets a
  `task_id` from `POD_NAME` (downward API, added to the deployment), falling back to the hostname.

**Rollout is two steps, and step one is not in this repo.**

1. Confirm `roles/monitoring.metricWriter` on the `dl-sharing` GCP service account that Workload
   Identity binds to the Kubernetes SA. It is declared in
   `zing-infrastructure/zing/modules/zing/zing-svc-service-accounts.tf` as
   `google_project_iam_member.dl-sharing-metricWriter`, whose `contains([...])` guard covers
   `zing-dev-197522`, `zing-preview`, `zcloud-prod`, `zcloud-prod2` and `zcloud-prod3`.
   **Done for all five (ZING-45349, 2026-09-02):** the binding is applied, not merely declared, in
   every one of those projects -- checked with `gcloud projects get-iam-policy <project>` filtered
   on `dl-sharing@<project>.iam.gserviceaccount.com`, which in each case also returns
   `roles/storage.objectAdmin` and `roles/iam.serviceAccountTokenCreator`. Step 1 needs no change
   for these environments.
2. Set `metrics.enabled: true` in the environment's `manifests/<env>/configmap.yaml` and deploy.

**`zcloud-emea`: Delta Sharing is not deployed there** (ZING-45349, verified 2026-09-02). The
zcloud-emea cluster has no `delta-sharing-server` deployment and no `dl-sharing` Kubernetes service
account, and `dl-sharing@zcloud-emea.iam.gserviceaccount.com` does not exist -- consistent with
zcloud-emea being absent from *every* `dl-sharing` resource in that Terraform file (the GCP SA, the
`workloadIdentityUser` binding, `monitoring.metricWriter`, `storage.objectAdmin`,
`serviceAccountTokenCreator`, the Kubernetes SA, the `delta-sharing-auth` secret, and
`random_bytes.delta-sharing-bearer-token` in `zing-secrets.tf`). So the `manifests/zcloud-emea/`
overlay is inert: it is now marked `NOT DEPLOYED` at the top of both its files rather than deleted,
so the region-specific values survive for a future rollout. Enabling metrics there is not a matter
of one IAM binding -- without the SA and `storage.objectAdmin`, GCS reads would not work either.

**Enabled so far** -- check any claim here with `kustomize build manifests/<env>` rather than by
reading an overlay, since zing-dev takes its value from `manifests/base/configmap.yaml`:

| Environment | `metrics.enabled` |
|-------------|-------------------|
| zing-dev | true (from `manifests/base/configmap.yaml`; it has no overlay patch) |
| zing-preview | true |
| zcloud-prod | true |
| zcloud-prod2 | false |
| zcloud-prod3 | false |
| zcloud-emea | false |

`roles/monitoring.metricWriter` was confirmed applied -- not merely declared in Terraform -- on
`dl-sharing@zing-dev-197522.iam.gserviceaccount.com`. The two remaining prod environments are
unblocked on IAM and gated only on the cost check: Cloud Monitoring bills per time series, so the
bill from the enabled environments gets a look before `zcloud-prod2` and `zcloud-prod3` follow.

The `MetricsConfig` *default* is disabled, so a deployment that omits the block gets no exporter
at all; enabling is a per-environment decision because it starts billable ingestion, and in
`zcloud-emea` it would write nothing regardless. Nothing else about the server changes while it is
off
-- `MetricsRegistries.create` returns a no-op recorder, and an exporter that fails to construct is
logged and degraded to that same no-op rather than taking the server down.

**Not shipped in P1:** the access-log latency columns from section 6.1. They need the `deltalake-admin`
template change and the per-environment `ADD COLUMNS` migration, which cannot be done from this
repo. Everything else in P1 is independent of them, as section 9 anticipated.

---

## 7. SLIs, SLOs, alerts

**SLIs** (per query class, since a shared SLO across classes is meaningless — a `version` poll and a
10k-file CDF read differ by three orders of magnitude):

| Query class | SLI | Starting SLO (to be validated against a week of real traffic) |
|-------------|-----|-----------------------------------|
| `version` | p95 duration | < 250 ms |
| `metadata` | p95 duration | < 500 ms |
| `snapshot` | p95 duration | < 5 s |
| `incremental` | p95 duration | < 5 s |
| `cdf` | p95 duration | < 15 s |
| all | availability = 1 − 5xx/total | > 99.9% |
| all | timeout rate | < 0.1% |

The SLO numbers are placeholders on purpose: the first deliverable after instrumenting is a week of
baseline data, then these get set from the observed distribution. Publishing an unvalidated SLO is
worse than publishing none.

**Alerts** (P1 unless noted):

| Alert | Condition | Rationale |
|-------|-----------|-----------|
| Query latency SLO burn | p95 of a class > 2× its SLO for 15 min | Primary customer-visible symptom |
| Requests near timeout | `near_timeout_total` rate > 1/min for 10 min | Predicts client-visible failures before they happen |
| Timeouts | any `request_timeouts_total` increase over 5 min | Hard failure |
| 5xx rate | > 1% for 5 min | Availability |
| Signing pool saturated | `signing_pool_queue_depth` > 0 for 5 min (P2) | Cross-tenant interference |
| GCS degraded | p95 `gcs_operation_duration_seconds{operation="sign"}` > 200 ms (P2) | Dependency, not us |
| Streaming backlog | p95 `versions_iterated{query_class="incremental"}` > 500 | A consumer is falling behind; latency will follow |
| Access-log writer stalled | `access_log_buffer_size` > 80% capacity or `dropped_total` > 0 (P2) | Telemetry loss, and a latency risk |
| Table cache thrash | hit ratio < 50% for 30 min (P3) | `deltaTableCacheSize` too small for the onboarded table count |

---

## 8. Dashboard layout

1. **Overview** — request rate, error rate, p50/p95/p99 duration, in-flight, all split by
   `query_class`. Answers "is it broken, and for whom".
2. **Stage breakdown** — stacked p95 stage latency per query class; the `unattributed` band is the
   honesty check.
3. **Work volume** — files scanned/returned/signed, versions iterated, response bytes, plus the two
   derived ratios (ms/file, ms/version).
4. **Dependencies & saturation** — GCS op latency, signing pool, table cache, JVM/GC, event loop.
5. **Per-tenant** — request rate, p95, egress by tenant; top-N slow tables (from the Delta table,
   not from Prometheus).
6. **Streaming health** — `version` poll latency, poll interval, versions iterated, pages per query.

---

## 9. Phased plan

**P1 — request + standalone stages + work volume. DONE** (see §6.2). Micrometer registry exporting
to Cloud Monitoring, `query_class` classification, request histogram/counters/gauges, stage
histogram wired from the existing `TableQueryTimings`/`CdfQueryTimings`, files/versions histograms.
The P1 metrics needed no new timing instrumentation — the boundaries in §2.1–2.3 already existed;
the `response_build` stage was added afterwards, on the evidence the metrics themselves produced. This closes G1, G4, G6 and most of G7, and G3 for latency (the `version` and `metadata`
endpoints now have request-level timing, though still no stage breakdown).

The access-log latency columns were the one part of P1 left undone: they cross into
`deltalake-admin` and need the migration ordering in §6.1 — template, then `ADD COLUMNS` per
environment, then the writer. Nothing else in P1 depended on them.

**P2 — kernel path + saturation.** Populate timings on the Kernel path (`query`, `queryCDF`,
`getTableVersion`) — closes G2/G3; wrap `fileSigner.sign` so GCS latency and signing-pool wait are
measured on both paths (G8); signing pool, table cache, JVM, access-log-writer metrics (G5).
Expect this phase to also answer whether the Kernel path's serial signing and per-request table
construction are the actual cause of the reported "simple query" latency — if so, that is a fix
ticket, not a metrics ticket.

**P3 — client behaviour + polish.** Paging/token metrics, per-tenant poll interval, TTFB,
dashboards as code, SLOs set from the P1/P2 baseline, alerts promoted from warning to paging.

---

## 10. Open questions

1. Which Prometheus/Grafana stack scrapes these pods, and does it already scrape the sidecar? That
   decides A-vs-C in §6 and whether an admin port needs a NetworkPolicy.
2. Access-log schema change — the mechanics are settled (§6.1); what is left is a decision: do we
   add an additive `evolve-schema` step to the `deltalake-admin` CLI, or run the five `ADD COLUMNS`
   statements by hand once? And who runs the migration in prod, given `create-tables` goes through
   a Jenkins job (`ci/create-tables.groovy`)?
3. Tenant cardinality: how many tenants are onboarded per environment today, and what is the
   projected count? Sets the rule-2 threshold in §5.
4. Are per-tenant SLOs contractual anywhere, or are these internal-only targets?
5. Do we need trace-level (per-request span) data for the worst offenders, or are histograms plus
   the access-log rows enough? Tracing is deliberately out of scope here.
