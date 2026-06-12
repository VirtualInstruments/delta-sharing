# Per-Share Egress Monitoring

## Overview

Structured access logs for egress tracking with automatic GCP pricing tier classification.

When enabled, each data access emits JSON logs with:
- Share/schema/table identification
- Total egress bytes
- Pricing tier (e.g., `internet_to_na_eu`, `interregion_na_to_eu`)
- Client region code
- Audit fields for customer audits (clientIp, rawRegionHeader, isGcpIp, tenantId)

---

## Pricing Tiers

### 1. Free/Internal Traffic

| Tier | Description | Approximate Cost |
|------|-------------|------------------|
| `same_zone` | Traffic within the same GCP zone | Free |
| `same_region` | Traffic within the same region or Kubernetes cluster | ~Free |

### 2. Inter-Region GCP Traffic

Traffic between GCP services (e.g., between regions via service mesh):

| Tier | Route | Approximate Cost |
|------|-------|------------------|
| `interregion_na_to_na` | North America → North America | $0.02/GiB |
| `interregion_eu_to_eu` | Europe → Europe | $0.02/GiB |
| `interregion_na_to_eu` | North America → Europe | $0.05/GiB |
| `interregion_eu_to_na` | Europe → North America | $0.05/GiB |
| `interregion_to_apac` | Any region → Asia Pacific | $0.08/GiB |
| `interregion_to_oceania` | Any region → Australia/Oceania | $0.10/GiB |
| `interregion_to_latam` | Any region → Latin America | $0.14/GiB |

### 3. Internet Egress (Premium Tier)

Traffic leaving GCP to external clients:

| Tier | Destination | Approximate Cost |
|------|-------------|------------------|
| `internet_to_na_eu` | North America or Europe | $0.12/GiB |
| `internet_to_apac` | Asia Pacific | $0.12/GiB |
| `internet_to_latam` | Latin America | $0.19/GiB |
| `internet_to_oceania` | Australia/Oceania | $0.15/GiB |

### Special Cases

| Tier | Description |
|------|-------------|
| `unknown` | Unable to determine pricing tier |

---

## How Pricing Tiers Are Resolved

### Step 1: Determine Egress Type

```
┌─────────────────────────────────────────────────────────────────┐
│                    Egress Type Detection                        │
├─────────────────────────────────────────────────────────────────┤
│ 1. Client IP is private (10.x, 172.16-31.x, 192.168.x)?        │
│    → SAME_REGION (internal cluster traffic)                     │
│                                                                 │
│ 2. GCP IP Range Lookup (from cloud.json) finds client IP?      │
│    a. Client GCP region matches sourceRegion?                   │
│       → SAME_REGION                                             │
│    b. Client GCP region differs from sourceRegion?              │
│       → INTER_REGION (with exact destination region)            │
│                                                                 │
│ 3. Client IP looks like GCP (34.x/35.x) but not in ranges?     │
│    → INTER_REGION (conservative fallback)                       │
│                                                                 │
│ 4. Non-GCP IP with valid country code?                         │
│    → INTERNET                                                   │
│                                                                 │
│ 5. No region header?                                            │
│    → UNKNOWN                                                    │
└─────────────────────────────────────────────────────────────────┘
```

The system fetches GCP's published IP ranges from `https://www.gstatic.com/ipranges/cloud.json`
and uses a CIDR trie for efficient IP-to-region lookup (refreshed every 24 hours).

### Step 2: Determine Continents

Based on the source region (server configuration) and destination (detected from headers):

**GCP Regions → Continent Mapping:**
- `us-*`, `northamerica-*` → North America (NA)
- `europe-*` → Europe (EU)
- `asia-*` → Asia Pacific (APAC)
- `southamerica-*` → Latin America (LATAM)
- `australia-*` → Oceania (OCEANIA)

**Country Codes → Continent Mapping:**
- US, CA, MX → NA
- GB, IE, DE, FR, NL, BE, CH, AT, ES, PT, IT, SE, NO, DK, FI, PL, CZ, HU, RO, etc. → EU
- JP, KR, CN, HK, TW, SG, MY, ID, TH, VN, PH, IN, PK, BD → APAC
- AU, NZ → OCEANIA
- BR, AR, CL, CO, PE, VE, EC, etc. → LATAM

### Step 3: Calculate Pricing Tier

Based on egress type and continent pair:

| Egress Type | Calculation Method |
|-------------|-------------------|
| `SAME_ZONE` | Returns `same_zone` |
| `SAME_REGION` | Returns `same_region` |
| `INTER_REGION` | Based on source→destination continent pair |
| `INTERNET` | Based on destination continent only |

---

## Configuration

```yaml
accessLogging:
  enabled: true
  sourceRegion: "us-central1"           # GCP region where server runs
  detectGcpTraffic: true                # Enable GCP IP range lookup
  clientRegionHeader: "x-client-region" # Header with country code
  clientIpHeader: "x-forwarded-for"     # Header with client IP chain
  # Base path for the consolidated access log Delta table.
  # All access logs are written to a single table: access_log_br_system
  # Omit to disable Delta writing.
  deltaTablePath: "gs://<bucket>/datalake/data/tenant/_system"
  deltaFlushIntervalSeconds: 60         # Max seconds between Delta flushes (default: 60)
  deltaFlushBatchSize: 1000             # Records per flush before early trigger (default: 1000)
```

### GCP Load Balancer Headers

Configure custom request headers on your backend:
```
X-Client-Region: {client_region}
X-Client-Region-Subdivision: {client_region_subdivision}
```

---

## Delta Lake Storage

When `deltaTablePath` is configured, `ACCESS_LOG` entries are written asynchronously to
a consolidated Delta table on GCS in addition to the JSON log stream. This enables durable storage and
SQL-queryable access via Delta Sharing.

### Consolidated Table

All access logs are written to a single consolidated table `access_log_br__system`. The `tenantId`
field is included in each record for filtering by tenant. This simplifies cross-tenant queries
and consolidates all egress data in one location.

**IMPORTANT:** The Delta table must be pre-created by the `deltalake-admin` tool during tenant
onboarding. The Delta Sharing server does not auto-create the table schema.

| Property | Value |
|----------|-------|
| **Table Name** | `access_log_br__system` |
| **GCS Path** | `{deltaTablePath}/access_log_br__system` |
| **Partitioning** | None (unpartitioned for simplified queries) |
| **Format** | Parquet + Delta transaction log |
| **Pre-requisite** | Table must be created by `deltalake-admin` before first write |

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `logType` | string | Always "ACCESS_LOG" |
| `share` | string | Share name |
| `schema` | string | Schema name |
| `table` | string | Table name |
| `egressBytes` | long | Bytes transferred |
| `pricingTier` | string | GCP pricing tier (see Pricing Tiers) |
| `timestampMs` | long | Timestamp in milliseconds |
| `requestType` | string | "query" or "cdf_stream" |
| `clientRegion` | string | ISO 3166-1 alpha-2 country code |
| `tenantId` | string | Tenant identifier (derived from share name) |
| `clientIp` | string | Client IP address for audit |
| `rawRegionHeader` | string | Raw region header value for audit |
| `isGcpIp` | boolean | Whether client IP is in GCP ranges |

### GCS Base Paths by Environment

| Environment | Table Path |
|-------------|------------|
| zing-dev | `gs://zing-dev-197522-dl-v1/datalake/data/tenant/_system/access_log_br__system` |
| zing-preview | `gs://zing-preview-dl-v1/datalake/data/tenant/_system/access_log_br__system` |
| zcloud-prod | `gs://zcloud-prod-dl-v1/datalake/data/tenant/_system/access_log_br__system` |
| zcloud-prod2 | `gs://zcloud-prod2-dl-v1/datalake/data/tenant/_system/access_log_br__system` |
| zcloud-prod3 | `gs://zcloud-prod3-dl-v1/datalake/data/tenant/_system/access_log_br__system` |

### Delta Write Behavior

- Records are buffered in a bounded in-memory queue (capacity: 100,000) and written by a
  background daemon thread — the request path is never blocked.
- A flush is triggered when `deltaFlushBatchSize` records accumulate or `deltaFlushIntervalSeconds`
  elapses, whichever comes first.
- Queue overflow silently drops records (logged as a warning); write errors are logged to stderr
  and never propagate to clients.
- On graceful shutdown the queue is fully drained before the process exits.
- Only `ACCESS_LOG` entries are written to Delta. `PRICING_CONTEXT` and `REQUEST_HEADERS`
  entries are log-only.

---

## Log Output

**ACCESS_LOG** — Emitted for each request with non-zero egress:
```json
{
  "logType": "ACCESS_LOG",
  "share": "myshare",
  "schema": "myschema", 
  "table": "mytable",
  "egressBytes": 1048576,
  "pricingTier": "internet_to_na_eu",
  "timestampMs": 1717502400000,
  "requestType": "query",
  "clientRegion": "US",
  "tenantId": "my_tenant",
  "clientIp": "203.0.113.45",
  "rawRegionHeader": "US",
  "isGcpIp": false
}
```

**PRICING_CONTEXT** — Debug entry with detection details:
```json
{
  "logType": "PRICING_CONTEXT",
  "clientIp": "203.0.113.45",
  "isGcpIp": false,
  "egressType": "internet",
  "sourceRegion": "us-central1",
  "destinationContinent": "NA",
  "pricingTier": "internet_to_na_eu"
}
```

**REQUEST_HEADERS** — All headers for debugging (keys lowercased).

---

## Header Detection Priority

**Region Headers:**
1. Configured `clientRegionHeader` (default: `x-client-region`)
2. `x-appengine-country`
3. `cf-ipcountry`
4. `cloudfront-viewer-country`

**Client IP Headers:**
1. Configured `clientIpHeader` (default: `x-forwarded-for`)
2. `x-envoy-external-address`

---

## Implementation

| File | Purpose |
|------|---------|
| `GcpPricingTier.scala` | Continent mapping, egress type detection, pricing calculation |
| `GcpIpRangeLookup.scala` | GCP IP range fetching and CIDR trie lookup |
| `AccessLogEmitter.scala` | Log entry models with audit fields, JSON emission, composite fan-out |
| `DeltaAccessLogWriter.scala` | Async Delta Lake writer to consolidated `access_log_br__system` table |
| `DeltaSharingService.scala` | Integration for query/CDF endpoints, tenant ID extraction |
| `ServerConfig.scala` | `AccessLoggingConfig` model |

### Egress Bytes Calculation

Sum of `size` from all file actions: `AddFile`, `AddFileForCDF`, `AddCDCFile`.

---

## Notes

- JSON logs emitted to `delta.sharing.access` logger
- Zero-byte requests not logged (neither to JSON nor Delta)
- GCP IP ranges refreshed every 24 hours
- Pricing tiers match GCP documentation
- Delta writes are additive; disabling `deltaTablePath` has no effect on JSON log output
- All tenant access logs are consolidated in a single `access_log_br__system` table
- Audit fields (clientIp, rawRegionHeader, isGcpIp, tenantId) support customer audit requirements
