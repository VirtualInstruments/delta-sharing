# Delta Sharing – Agent Instructions

Virtana fork of [delta-io/delta-sharing](https://github.com/delta-io/delta-sharing), extending the reference server with GCS-backed telemetry and multi-environment Kubernetes deployments.

Deeper background lives in the [memory bank](memory-bank/README.md) — overview, build/test, Virtana divergence, deployment, and GCP egress pricing.

## Rules for Every Task

1. **Update [CHANGELOG-VIRTANA.md](CHANGELOG-VIRTANA.md).** Any change to behaviour, configuration, build setup, or deployment gets reflected in the relevant area section (add a new area if none fits). It's an informal running overview organized by area, not a dated/versioned log — no need to track release status. That file is the record of how this fork diverges from upstream.
2. **Run the tests** for whatever you touched (see [Build & Test](#build--test)). `server/test` runs scalastyle first and fails on style violations.
3. **Verify the image builds** when you change `build.sbt`, dependencies, or anything packaging-related:
   ```bash
   DOCKER_DEFAULT_PLATFORM=linux/amd64 make image
   ```
   The env var is required — the deployment target is amd64 and building on an arm64 Mac without it produces an unusable image.

## Branching

**`master` is the working branch** — do development and target PRs here. CI/CD builds and deploys from it.

`master` was forked from upstream [`delta-io/delta-sharing` `branch-1.3`](https://github.com/delta-io/delta-sharing/tree/branch-1.3) at commit `793cc19b2a3434227ebdc7f34bb2141047a67925`.

| Branch | Purpose |
|--------|---------|
| `master` | Working + release branch — CI/CD deploys from here |
| `virtana-1.3` | Historical fork branch |
| `main` | Upstream delta-io/delta-sharing — **not** the deployment branch |

Never target `main` for Virtana changes.

## Divergence from Upstream

See [CHANGELOG-VIRTANA.md](CHANGELOG-VIRTANA.md) for the full inventory of Virtana-modified and Virtana-authored files, plus the merge risks to watch when pulling from upstream. When fixing a bug, check that file first to tell whether the code is ours or upstream's.

## Build & Test

Use the wrapper script — plain `sbt` may not be in PATH:

```bash
./build/sbt server/compile          # Scala 2.12 (server only)
./build/sbt client/compile          # Cross-compiled 2.12 + 2.13
./build/sbt spark/compile           # Scala 2.13 only

./build/sbt server/test             # Runs scalastyle before tests
./build/sbt client/test
./build/sbt spark/test

# Image build — always pin the platform, the deploy target is amd64
DOCKER_DEFAULT_PLATFORM=linux/amd64 make image
DOCKER_DEFAULT_PLATFORM=linux/amd64 ./build/sbt server/docker:publishLocal
```

**Scala versions**: server → 2.12.18 + Spark 3.5.3 (Java 8); spark connector → 2.13.13 + Spark 4.0.0 (Java 17).

scalastyle runs as part of `server/test` and fails the build on violations. Config: [scalastyle-config.xml](scalastyle-config.xml). Use `org.scalatest.FunSuite` (not `AnyFunSuite`) for test suites.

Python tests:
```bash
python/dev/pytest
```

## Project Structure

```
server/src/main/scala/io/delta/sharing/server/   REST endpoints (Armeria), main entry point
server/src/main/scala/.../server/config/         ServerConfig, AccessLoggingConfig, Share/Table config
server/src/main/scala/.../server/common/         CloudFileSigner (GCS + S3), JsonPredicates
server/src/main/scala/.../server/telemetry/      Access log emission, GCP pricing tier, Delta writer
server/src/main/scala/.../kernel/                Delta Lake kernel integration
manifests/                                       Kustomize overlays per environment
ci/                                              Jenkins pipeline, deploy scripts
python/delta_sharing/                            Python client library
```

## Server Configuration

The server takes `--config <path-to-yaml>`. Key settings (from [manifests/base/configmap.yaml](manifests/base/configmap.yaml)):

```yaml
host: "0.0.0.0"
port: 8080
endpoint: "/delta-sharing"
preSignedUrlTimeoutSeconds: 3600
deltaTableCacheSize: 100
evaluateJsonPredicateHints: true
evaluateJsonPredicateHintsV2: true
requestTimeoutSeconds: 300
idleTimeoutSeconds: 120     # Must exceed the proxy's IdleConnTimeout or clients see EOF errors
queryTablePageSizeLimit: 10000
perfLoggingEnabled: true

authorization:
  bearerToken: "<token>"     # Injected from $BEARER_TOKEN at deploy time

shares:
  - name: "share_name"
    schemas:
      - name: "schema_name"
        tables:
          - name: "table_name"
            location: "gs://bucket/path"
            cdfEnabled: false
```

## GCS Integration

**Authentication**: Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON path. In Kubernetes, Workload Identity is used via the `dl-sharing` service account — no key file needed in production.

**Dependencies** (in [build.sbt](build.sbt)):
- `com.google.cloud:google-cloud-storage` — GCS SDK
- `com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.2.4` — Hadoop FS integration

**GCS signing**: `server/src/main/scala/.../server/common/CloudFileSigner.scala` — generates pre-signed GCS URLs using `GoogleHadoopFileSystem` and `StorageResourceId`.

Use `gs://` table paths (not `s3://` or `s3a://`) for GCS-backed tables.

**GCS environments**:

| Environment | Bucket | Region |
|------------|--------|--------|
| zing-dev | `gs://zing-dev-197522-dl-v1/` | us-central1 |
| zing-preview | `gs://zing-preview-dl-v1/` | us-central1 |
| zcloud-prod | `gs://zcloud-prod-dl-v1/` | us-central1 |
| zcloud-prod2 | `gs://zcloud-prod2-dl-v1/` | us-west4 |
| zcloud-prod3 | `gs://zcloud-prod3-dl-v1/` | australia-southeast1 |
| zcloud-emea | GCS bucket | europe-west3 |

## Access Logging (Virtana Extension)

Virtana-added feature that writes structured access log entries to a Delta table on GCS after each query/CDF request. See [memory-bank/06-egress-monitoring.md](memory-bank/06-egress-monitoring.md) and [memory-bank/07-access-log-table-reference.md](memory-bank/07-access-log-table-reference.md).

**Config block**:
```yaml
accessLogging:
  enabled: true
  sourceRegion: "us-central1"           # GCP region of this server's data bucket
  detectGcpTraffic: true                # Classify inter-GCP traffic for pricing tier
  clientRegionHeader: "x-client-region"
  clientIpHeader: "x-forwarded-for"
  deltaTablePath: "gs://bucket/path/tenant/_system"
  deltaFlushIntervalSeconds: 60
  deltaFlushBatchSize: 1000
```

**Key classes**:
- `AccessLogEmitter` / `DeltaAccessLogWriter` — buffered async writer to GCS Delta table
- `GcpPricingTier` — classifies egress by region pair (e.g. `internet_to_na_eu`, `same_region`); refreshes GCP IP ranges from gstatic.com every 24h
- `GcpIpRangeLookup` — IP range → GCP region detection

**Delta table** (`access_log_br__system`): a single consolidated, unpartitioned table with `tenantId` as a column for per-tenant filtering. Protocol (1,2) for Delta Standalone compatibility; pre-created by `deltalake-admin` — the writer does not create the schema.

## Kubernetes Deployment

Manifests use Kustomize overlays at [manifests/](manifests/). Each environment overlay extends `manifests/base/`.

Deployment pattern:
- Init container merges base config + shares config using `envsubst` (substitutes `$BEARER_TOKEN`, `$GCP_PROJECT_ID`)
- Sidecar container `zc-api-proxy` handles JWT/Auth0 authentication in front of the sharing server on `localhost:8080`
- `dl-sharing` Kubernetes SA is bound to a GCP SA via Workload Identity

```bash
make deploy-dev       # Deploy to zing-dev
make deploy-preview   # Deploy to zing-preview
make deploy-prod      # Deploy to zcloud-prod
```

## Environment Variables

| Variable | Where used |
|----------|-----------|
| `BEARER_TOKEN` | Kubernetes secret → injected into server config |
| `GCP_PROJECT_ID` | Kubernetes ConfigMap → injected into server config |
| `GOOGLE_APPLICATION_CREDENTIALS` | Local dev / test — path to service account JSON |
| `AZURE_TEST_ACCOUNT_KEY` | Optional — Azure blob storage integration tests |
