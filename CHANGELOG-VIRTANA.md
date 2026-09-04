# Virtana Changelog

Everything Virtana has changed on top of the upstream [delta-io/delta-sharing](https://github.com/delta-io/delta-sharing) reference server.

Fork point: upstream [`branch-1.3`](https://github.com/delta-io/delta-sharing/tree/branch-1.3) at commit `793cc19b2a3434227ebdc7f34bb2141047a67925`.

```bash
git diff --stat 793cc19b2a3434227ebdc7f34bb2141047a67925..HEAD
```

**Keep this file current.** This isn't a formal historical log — just a running, informal overview of
everything Virtana has changed in this fork, organized by area. Every task that changes behaviour,
configuration, build setup, or deployment updates the relevant area below (or adds a new one if it
doesn't fit). Upstream files not listed here are unmodified — when fixing a bug, first check whether
it lives in Virtana code or upstream code.

## Changes

### Telemetry package (largest addition)

`server/src/main/scala/io/delta/sharing/server/telemetry/`, entirely Virtana-authored:

| File | Purpose |
|------|---------|
| `AccessLogEmitter.scala` | `AccessLogEntry` / `PricingContextLogEntry` models; JSON emitter |
| `DeltaAccessLogWriter.scala` | Async buffered writer to the GCS Delta table |
| `GcpPricingTier.scala` | Egress cost classification by source→destination region pair |
| `GcpIpRangeLookup.scala` | GCP IP range → region detection (refreshed from `cloud.json`) |

Each has a matching suite under `server/src/test/scala/.../telemetry/`.

### Query performance metrics (Virtana addition)

- `server/.../telemetry/QueryMetrics.scala` — `QueryMetrics` trait, Micrometer implementation and
  the no-op used when metrics are disabled; flattens the existing `TableQueryTimings` /
  `CdfQueryTimings` into per-stage latency distributions plus an `unattributed` residual
- `server/.../telemetry/QueryClass.scala` — request taxonomy (snapshot / snapshot_filtered /
  snapshot_asof / incremental / cdf / version / metadata / catalog) and classification from request
  params or matched route
- `server/.../telemetry/MetricsRegistries.scala` — builds the Google Cloud Monitoring
  (Stackdriver) registry from config; any construction failure degrades to the no-op recorder
  rather than failing startup
- `server/.../telemetry/RequestMetrics.scala` — Armeria decorator recording duration, outcome,
  status class, response bytes, TTFB, in-flight and timeout/near-timeout counters for **every**
  endpoint, plus the request-context attributes handlers use to refine the query class
- `ServerConfig` — new `metrics` block (`MetricsConfig`). Disabled by default so the first enable
  is deliberate: it starts billable Cloud Monitoring ingestion, and `zcloud-emea` has no
  `dl-sharing` service account at all (ZING-45349)
- `DeltaSharingService` — registers the metrics decorator **after** the authorization decorator.
  Armeria runs the most recently registered decorator outermost, so registering it earlier drops
  every 401 from the metrics silently (regression test in `RequestMetricsIntegrationSuite`),
  classifies `query`/`changes` requests, feeds the existing completion paths into the recorder, and
  flushes the exporter from the shutdown hook. The metrics themselves needed no new timing calls in
  the query paths; `responseBuildNs` was added afterwards as a real stage (below)
- `build.sbt` — `micrometer-registry-stackdriver` 1.6.5 (the version Armeria 1.6.0 already puts on
  the classpath) with `google-cloud-monitoring` pinned to 3.0.4 and `gax-grpc` to 2.7.1; the
  transitive default drags in gax-grpc 1.56, which is incompatible with the gax 2.7.1 that
  `google-cloud-storage` requires
- `DeltaSharedTableProtocol.scala` / `standalone/internal/DeltaSharedTable.scala` — new
  `response_build` CDF stage (`CdfQueryTimings.responseBuildNs`): the classification of replayed
  actions and construction of signed response actions around `signingNs`. Added on the evidence of
  the `unattributed` residual, which showed most CDF time falling outside the existing boundaries
- `QueryResult.signedFiles` — the accurate pre-signed-file count, carried out of the standalone and
  CDF paths. `actions.length - 2` also counts protocol, metadata (historical metadata included) and
  the end-stream action, so it over-reported the `files_signed` metric; the Kernel path reports
  `None` and the metric is then skipped rather than guessed
- Design, metric catalog and rollout:
  [memory-bank/08-query-performance-metrics.md](memory-bank/08-query-performance-metrics.md)

### Upstream server files, modified

| File | Virtana change |
|------|----------------|
| `DeltaSharingService.scala` | ~600 added lines: access log emission per query/CDF request, client region + IP header extraction, egress byte accounting, idle timeout config |
| `config/ServerConfig.scala` | New `AccessLoggingConfig` case class; new `perfLoggingEnabled`, `idleTimeoutSeconds`, and `signingThreadPoolSize` options |
| `DeltaSharedTableProtocol.scala` | New `CdfQueryTimings` / `TableQueryTimings` / `QueryResultTimings` observability models; `QueryResult` gained a `timings` field |
| `DeltaSharedTableLoader.scala` | `loadTableWithUpdateCost` returns `deltaLog.update()` elapsed time for perf logging |
| `standalone/internal/DeltaSharedTable.scala` | Per-phase timing instrumentation (snapshot resolve, replay, signing); near-timeout warnings; **parallel GCS V4 signing** refactor (collects paths in order, signs in parallel on a shared process-wide thread pool sized by `signingThreadPoolSize`, reassembles results in original order) — reduces signing wall time by ~50% for CDF batches with 10+ versions |
| `standalone/internal/DeltaSharingCDCReader.scala` | CDF stream timing instrumentation |

### Build & infrastructure

| File | Virtana change |
|------|----------------|
| `build.sbt` | Global slf4j binding exclusions (`slf4j-log4j12`, `slf4j-reload4j`); per-dependency `ExclusionRule("org.slf4j")`; swapped `slf4j-simple` → `logback-classic`; `delta-standalone` changed from `provided` to a compile dependency (required by the Delta log writer); pinned `dockerBaseImage := "eclipse-temurin:8-jre"` |
| `server/src/main/resources/logback.xml` | New — preserves log severity in GCP Cloud Console |
| `scalastyle-config.xml` | License header regex relaxed to allow any copyright year |

### Deployment & docs

- `manifests/` — `metrics` block added to the base config and all five environment overlays.
  Enabled in `zing-dev` (via base), `zing-preview` and `zcloud-prod`; still disabled in
  `zcloud-prod2`, `zcloud-prod3` and `zcloud-emea` (which has no `dl-sharing` service account).
  `kustomize build manifests/<env>` is the authoritative check, since zing-dev inherits from base.
  Also
  `POD_NAME` exposed to the server container via the downward API so each replica gets its own
  Cloud Monitoring `task_id` -- without it, HPA-scaled replicas collide on one time series and
  Cloud Monitoring discards the points
- `manifests/` — `requestTimeoutSeconds` raised from 180 to 300 (3 min → 5 min) across base and all environment overlays; the near-timeout warning logs (fired at 75% of the configured limit)
- `manifests/` — Kustomize base + 6 environment overlays
- `ci/` — Jenkinsfile, Makefile, `deploy.sh`, per-environment deployment YAML
- `Makefile` — image build and `deploy-*` targets
- [memory-bank/](memory-bank/README.md) — committed shared context for humans and agents: overview,
  build/test, Virtana divergence, deployment, and GCP egress pricing reference. Linked from
  [AGENTS.md](AGENTS.md). Replaces `docs/PER_SHARE_EGRESS_MONITORING.md` (now
  `memory-bank/06-egress-monitoring.md`) and `docs/Notes.md` (now
  `memory-bank/07-access-log-table-reference.md`); the old `docs/` is gone. The access log table
  description was corrected there: `access_log_br__system` is unpartitioned (matches
  `DeltaAccessLogWriter`), not partitioned by `year`/`month`/`day`.
- [memory-bank/08-query-performance-metrics.md](memory-bank/08-query-performance-metrics.md) —
  query performance monitoring (ZING-45093/ZING-45330): query taxonomy, per-stage latency
  boundaries, the gap inventory, the metric catalog, cardinality rules, SLOs and alerts. P1 is
  implemented (see the metrics section above); section 6.2 records what shipped and the two-step
  rollout, section 6.1 the access-log schema change that is still outstanding.

## Known divergence risks when merging upstream

- `DeltaSharingService.scala` is heavily modified — expect conflicts on any upstream change there.
- The `delta-standalone` scope change (`provided` → compile) must be preserved or `DeltaAccessLogWriter` fails at runtime.
- slf4j exclusions must be preserved or the server emits "multiple SLF4J bindings" and loses Cloud Console severity.
