# Build & Test

Use the wrapper script — plain `sbt` may not be on PATH.

```bash
./build/sbt server/compile          # Scala 2.12 (server only)
./build/sbt client/compile          # cross-compiled 2.12 + 2.13
./build/sbt spark/compile           # Scala 2.13 only

./build/sbt server/test             # runs scalastyle first, fails on violations
./build/sbt client/test
./build/sbt spark/test

python/dev/pytest                   # Python client tests

DOCKER_DEFAULT_PLATFORM=linux/amd64 make image
DOCKER_DEFAULT_PLATFORM=linux/amd64 ./build/sbt server/docker:publishLocal
```

## Things that bite

- `DOCKER_DEFAULT_PLATFORM=linux/amd64` is mandatory. The deploy target is amd64; building on an
  arm64 Mac without it produces an unusable image.
- Test suites must use `org.scalatest.FunSuite`, **not** `AnyFunSuite`.
- scalastyle runs as part of `server/test` and fails the build. Config:
  [scalastyle-config.xml](../scalastyle-config.xml) (license header regex relaxed to allow any year).
- In [build.sbt](../build.sbt): `delta-standalone` must stay a compile dependency (not `provided`)
  or `DeltaAccessLogWriter` fails at runtime; the slf4j exclusions plus `logback-classic` must
  survive or the server emits multiple-binding warnings and loses severity in GCP Cloud Console.

## Versions

| Module | Scala | Spark | Java |
|--------|-------|-------|------|
| server | 2.12.18 | 3.5.3 | 8 |
| spark connector | 2.13.13 | 4.0.0 | 17 |

## Checklist for every change

1. Update the relevant area section under `## Changes` in
   [CHANGELOG-VIRTANA.md](../CHANGELOG-VIRTANA.md) (add a new area if none fits) for any change to
   behaviour, configuration, build setup, or deployment. It's an informal running overview organized
   by area, not a dated/versioned log.
2. Run the tests for whatever you touched.
3. Rebuild the image if `build.sbt`, dependencies, or packaging changed.
4. Write "Virtana", not "Zenoss", in docs.
