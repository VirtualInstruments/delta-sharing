# GCP Egress Pricing

Reference data behind `GcpPricingTier.scala` and the egress classification in the access log.

## Premium tier internet egress (VM → internet)

Tiered by monthly volume:

| Destination | 0–1 TiB | 1–10 TiB | 10 TiB+ |
|-------------|---------|----------|---------|
| North America ↔ Europe | $0.12 | $0.11 | $0.085 |
| Any GCP region → Asia (excl. Indonesia/Korea) | $0.12 | $0.11 | $0.085 |
| Any GCP region → Indonesia/Korea | $0.19 | $0.18 | $0.15 |
| Any GCP region → South America | $0.19 | $0.18 | $0.15 |

## Inter-region egress (VM → VM, VM → Google service), per GiB

| Region pair | Price |
|-------------|-------|
| North America ↔ North America | $0.02 |
| North America ↔ Europe | $0.05 |
| Asia ↔ Asia | $0.08 |
| Any ↔ Australia/Indonesia | $0.10 |
| Any ↔ South America | $0.14 |

## Geographic grouping

- **North America**: us-central1, us-east1, us-west1, us-west2, …
- **Europe**: europe-west1 (Belgium), europe-west2 (London), europe-north1 (Finland), …
- **Asia**: asia-east1, asia-northeast1, asia-southeast1, asia-south1, …
- **South America**: southamerica-east1
- **Australia**: australia-southeast1

## How the server classifies a request

Inputs: configured source region (the data bucket's region), destination (client region or IP
geolocation), egress bytes, and request type (`query` or `cdf_stream`).

Implementation in
[server/src/main/scala/io/delta/sharing/server/DeltaSharingService.scala](../server/src/main/scala/io/delta/sharing/server/DeltaSharingService.scala):

- `ClientLocationContext` — region, subdivision, IP, and pricing group.
- `buildClientLocationContext()` — resolves client location from request headers.
- `DefaultPricingGroupsByRegion` — country code → pricing group:
  - `na_eu`: US, CA, MX, GB, IE, DE, FR, NL, BE, CH, AT, ES, PT, IT, SE, NO, DK, FI, PL, CZ, HU, RO
  - `apac`: JP, KR, IN, SG, HK, TW, ID, MY, PH, TH, VN, AU, NZ
  - `latam`: BR, AR, CL, CO, PE

Header detection order:

1. Region: `x-client-region`, `x-appengine-country`, `cf-ipcountry`, `cloudfront-viewer-country`
2. Subdivision: `x-client-region-subdivision`, `x-appengine-region`
3. IP: `x-forwarded-for`, `x-envoy-external-address`, `x-real-ip`, `true-client-ip`

Resolution: try subdivision code, then region code, against the configured pricing map, then the
default map, then a `"*"` wildcard entry; fall back to the raw region, then `"unknown"`.

## Known gaps

- Client region/country often absent — needs IP geolocation or a custom header from the proxy.
- Pricing group shows `unknown` when no mapping resolves.
