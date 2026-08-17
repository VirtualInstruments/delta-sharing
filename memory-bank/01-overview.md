# Overview

Virtana fork of [delta-io/delta-sharing](https://github.com/delta-io/delta-sharing). It extends the
reference sharing server with GCS-backed access-log telemetry and multi-environment Kubernetes
deployment.

- Fork point: upstream [`branch-1.3`](https://github.com/delta-io/delta-sharing/tree/branch-1.3) at
  commit `793cc19b2a3434227ebdc7f34bb2141047a67925`.
- The org is Virtana; the git remote path is still `github.com/zenoss`.

## Branching

| Branch | Purpose |
|--------|---------|
| `master` | Working + release branch — CI/CD builds and deploys from here. Target PRs here. |
| `virtana-1.3` | Historical fork branch |
| `main` | Upstream mirror — never target it for Virtana changes |

## Layout

```
server/src/main/scala/io/delta/sharing/server/   Armeria REST endpoints, main entry point
        .../server/config/                       ServerConfig, AccessLoggingConfig, Share/Table config
        .../server/common/                       CloudFileSigner (GCS + S3), JsonPredicates
        .../server/telemetry/                    Virtana access logging + egress pricing tier
        .../kernel/                              Delta Lake kernel integration
client/, spark/, python/delta_sharing/           Client libraries
manifests/                                       Kustomize base + per-environment overlays
ci/                                              Jenkinsfile, Makefile, deploy.sh
memory-bank/                                     This documentation set
dev/                                             Local scratch scripts — out of scope
```

Server entry points worth knowing: `DeltaSharingService.scala`, `DeltaSharedTableLoader.scala`,
`DeltaSharedTableProtocol.scala`, `SharedTableManager.scala`.

## Server configuration

The server takes `--config <path-to-yaml>`. Base config lives in
[manifests/base/configmap.yaml](../manifests/base/configmap.yaml).

```yaml
host: "0.0.0.0"
port: 8080
endpoint: "/delta-sharing"
preSignedUrlTimeoutSeconds: 3600
deltaTableCacheSize: 100
evaluateJsonPredicateHints: true
evaluateJsonPredicateHintsV2: true
requestTimeoutSeconds: 300
idleTimeoutSeconds: 120      # must exceed the proxy's IdleConnTimeout or clients see EOF errors
queryTablePageSizeLimit: 10000
perfLoggingEnabled: true

authorization:
  bearerToken: "<token>"     # injected from $BEARER_TOKEN at deploy time

shares:
  - name: "share_name"
    schemas:
      - name: "schema_name"
        tables:
          - name: "table_name"
            location: "gs://bucket/path"
            cdfEnabled: false
```

`accessLogging` is a Virtana addition — see [03-virtana-changes.md](03-virtana-changes.md).

## GCS integration

- **Auth**: `GOOGLE_APPLICATION_CREDENTIALS` pointing at a service account JSON for local work; in
  Kubernetes, Workload Identity via the `dl-sharing` service account (no key file).
- **Dependencies** (in [build.sbt](../build.sbt)): `com.google.cloud:google-cloud-storage`,
  `com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.2.4`.
- **Signing**: `server/src/main/scala/.../server/common/CloudFileSigner.scala` generates pre-signed
  GCS URLs via `GoogleHadoopFileSystem` and `StorageResourceId`.
- Use `gs://` table paths, not `s3://` or `s3a://`.
