# What Virtana Changed

[CHANGELOG-VIRTANA.md](../CHANGELOG-VIRTANA.md) is the authoritative log; this file is the shape of
the divergence. When fixing a bug, check here first to tell whether the code is ours or upstream's.

## New — telemetry package (largest addition)

`server/src/main/scala/io/delta/sharing/server/telemetry/`, entirely Virtana-authored:

| File | Purpose |
|------|---------|
| `AccessLogEmitter.scala` | `AccessLogEntry` / `PricingContextLogEntry` models; JSON emitter |
| `DeltaAccessLogWriter.scala` | Async buffered writer to the GCS Delta table |
| `GcpPricingTier.scala` | Egress cost classification by source→destination region pair (e.g. `same_region`, `internet_to_na_eu`); refreshes GCP IP ranges from gstatic.com every 24h |
| `GcpIpRangeLookup.scala` | GCP IP range → region detection |

Each has a matching suite under `server/src/test/scala/.../telemetry/`.

### Config block

```yaml
accessLogging:
  enabled: true
  sourceRegion: "us-central1"           # GCP region of this server's data bucket
  detectGcpTraffic: true                # classify inter-GCP traffic for pricing tier
  clientRegionHeader: "x-client-region"
  clientIpHeader: "x-forwarded-for"
  deltaTablePath: "gs://bucket/path/tenant/_system"
  deltaFlushIntervalSeconds: 60
  deltaFlushBatchSize: 1000
```

### Output table

`access_log_br__system` — a single consolidated, **unpartitioned** table; `tenantId` is a column so
queries can filter by tenant. Protocol (1,2) for Delta Standalone compatibility. The table must be
pre-created by `deltalake-admin`; the writer does not create the schema.

Background: [06-egress-monitoring.md](06-egress-monitoring.md),
[07-access-log-table-reference.md](07-access-log-table-reference.md).

## Modified — upstream server files

| File | Virtana change |
|------|----------------|
| `DeltaSharingService.scala` | ~600 added lines: access log emission per query/CDF request, client region + IP header extraction, egress byte accounting, idle timeout config |
| `config/ServerConfig.scala` | New `AccessLoggingConfig` case class; new `perfLoggingEnabled` and `idleTimeoutSeconds` options |
| `DeltaSharedTableProtocol.scala` | New `CdfQueryTimings` / `TableQueryTimings` / `QueryResultTimings` observability models; `QueryResult` gained a `timings` field |
| `DeltaSharedTableLoader.scala` | `loadTableWithUpdateCost` returns `deltaLog.update()` elapsed time for perf logging |
| `standalone/internal/DeltaSharedTable.scala` | Per-phase timing instrumentation (snapshot resolve, replay, signing); near-timeout warnings |
| `standalone/internal/DeltaSharingCDCReader.scala` | CDF stream timing instrumentation |

## Modified — build & infrastructure

| File | Virtana change |
|------|----------------|
| `build.sbt` | Global slf4j binding exclusions (`slf4j-log4j12`, `slf4j-reload4j`); per-dependency `ExclusionRule("org.slf4j")`; `slf4j-simple` → `logback-classic`; `delta-standalone` moved from `provided` to a compile dependency; pinned `dockerBaseImage := "eclipse-temurin:8-jre"` |
| `server/src/main/resources/logback.xml` | New — preserves log severity in GCP Cloud Console |
| `scalastyle-config.xml` | License header regex relaxed to allow any copyright year |

## New — deployment & docs

`manifests/` (Kustomize base + 6 overlays), `ci/` (Jenkinsfile, Makefile, `deploy.sh`,
per-environment deployment YAML), root `Makefile`, `memory-bank/`.

## Merge risks when pulling from upstream

1. `DeltaSharingService.scala` is heavily modified — expect conflicts on any upstream change there.
2. The `delta-standalone` scope change (`provided` → compile) must be preserved or
   `DeltaAccessLogWriter` fails at runtime.
3. The slf4j exclusions must be preserved or the server emits "multiple SLF4J bindings" and loses
   Cloud Console severity.
