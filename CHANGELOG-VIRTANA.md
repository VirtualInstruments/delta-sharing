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

### Upstream server files, modified

| File | Virtana change |
|------|----------------|
| `DeltaSharingService.scala` | ~600 added lines: access log emission per query/CDF request, client region + IP header extraction, egress byte accounting, idle timeout config |
| `config/ServerConfig.scala` | New `AccessLoggingConfig` case class; new `perfLoggingEnabled` and `idleTimeoutSeconds` options |
| `DeltaSharedTableProtocol.scala` | New `CdfQueryTimings` / `TableQueryTimings` / `QueryResultTimings` observability models; `QueryResult` gained a `timings` field |
| `DeltaSharedTableLoader.scala` | `loadTableWithUpdateCost` returns `deltaLog.update()` elapsed time for perf logging |
| `standalone/internal/DeltaSharedTable.scala` | Per-phase timing instrumentation (snapshot resolve, replay, signing); near-timeout warnings |
| `standalone/internal/DeltaSharingCDCReader.scala` | CDF stream timing instrumentation |

### Build & infrastructure

| File | Virtana change |
|------|----------------|
| `build.sbt` | Global slf4j binding exclusions (`slf4j-log4j12`, `slf4j-reload4j`); per-dependency `ExclusionRule("org.slf4j")`; swapped `slf4j-simple` → `logback-classic`; `delta-standalone` changed from `provided` to a compile dependency (required by the Delta log writer); pinned `dockerBaseImage := "eclipse-temurin:8-jre"` |
| `server/src/main/resources/logback.xml` | New — preserves log severity in GCP Cloud Console |
| `scalastyle-config.xml` | License header regex relaxed to allow any copyright year |

### Deployment & docs

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

## Known divergence risks when merging upstream

- `DeltaSharingService.scala` is heavily modified — expect conflicts on any upstream change there.
- The `delta-standalone` scope change (`provided` → compile) must be preserved or `DeltaAccessLogWriter` fails at runtime.
- slf4j exclusions must be preserved or the server emits "multiple SLF4J bindings" and loses Cloud Console severity.
