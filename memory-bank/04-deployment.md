# Deployment

Kustomize overlays live in [manifests/](../manifests/). Each environment overlay extends
`manifests/base/`: `zing-dev`, `zing-preview`, `zcloud-prod`, `zcloud-prod2`, `zcloud-prod3`,
`zcloud-emea`.

## Pod shape

- An init container merges the base config with the shares config using `envsubst`, substituting
  `$BEARER_TOKEN` and `$GCP_PROJECT_ID`.
- A sidecar container `zc-api-proxy` handles JWT/Auth0 authentication in front of the sharing
  server on `localhost:8080`.
- The `dl-sharing` Kubernetes service account is bound to a GCP service account via Workload
  Identity.

## Make targets

```bash
DOCKER_DEFAULT_PLATFORM=linux/amd64 make image   # build/sbt server/docker:publishLocal, then tag
make push-dev                                    # push to gcr.io/zing-dev-197522/
make deploy-dev                                  # ci/deploy.sh dev
make deploy-preview                              # ci/deploy.sh preview
make deploy-prod                                 # ci/deploy.sh prod
```

Version comes from [version.sbt](../version.sbt); `SERVICE_IMAGE` comes from `.env` and `IMAGE_TAG`
from `ci/.env`. CI lives in [ci/Jenkinsfile](../ci/Jenkinsfile) with per-environment
`ci/deployment-{dev,preview,prod}.yaml`.

## GCS environments

| Environment | Bucket | Region |
|-------------|--------|--------|
| zing-dev | `gs://zing-dev-197522-dl-v1/` | us-central1 |
| zing-preview | `gs://zing-preview-dl-v1/` | us-central1 |
| zcloud-prod | `gs://zcloud-prod-dl-v1/` | us-central1 |
| zcloud-prod2 | `gs://zcloud-prod2-dl-v1/` | us-west4 |
| zcloud-prod3 | `gs://zcloud-prod3-dl-v1/` | australia-southeast1 |
| zcloud-emea | GCS bucket | europe-west3 |

## Environment variables

| Variable | Where used |
|----------|-----------|
| `BEARER_TOKEN` | Kubernetes secret → injected into server config |
| `GCP_PROJECT_ID` | Kubernetes ConfigMap → injected into server config |
| `GOOGLE_APPLICATION_CREDENTIALS` | Local dev/test — path to service account JSON |
| `AZURE_TEST_ACCOUNT_KEY` | Optional — Azure blob storage integration tests |
