# Per-Share Egress Monitoring with Pricing Group Determination

## Overview

This document describes the new per-share egress monitoring feature introduced in the ZING-43690 branch. The feature provides:

1. **Share-attributed egress tracking** — Structured access logs for every query and CDF request
2. **GCP pricing tier classification** — Automatic determination of egress cost categories
3. **Traffic type detection** — Distinguishing between internet, inter-region GCP, and same-region traffic

## Feature Summary

When enabled, the Delta Sharing Server emits structured JSON log entries for each data access request. These logs include:

- **Share identification**: share name, schema, table
- **Data volume**: total egress bytes transferred
- **Pricing tier**: GCP egress pricing category (e.g., `internet_to_na_eu`, `interregion_na_to_na`)
- **Client context**: region code, request type

---

## Pricing Group Types

The system classifies egress traffic into pricing tiers based on GCP's network pricing structure. There are three main categories:

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
| `interregion_unknown` | Inter-region traffic detected but source region not configured |
| `unknown` | Unable to determine pricing tier |

---

## How Pricing Groups Are Resolved

The pricing tier is determined through a multi-step detection process:

### Step 1: Determine Egress Type

The system first classifies the traffic type based on available signals:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Egress Type Detection                        │
├─────────────────────────────────────────────────────────────────┤
│ 1. Client IP is private (10.x, 172.16-31.x, 192.168.x)?        │
│    → SAME_REGION (internal cluster traffic)                     │
│                                                                 │
│ 2. GCP detection enabled AND Envoy metadata contains region?   │
│    a. Extracted client region matches configured sourceRegion? │
│       → SAME_REGION (same-region GCP traffic)                   │
│    b. Otherwise                                                 │
│       → INTER_REGION (with specific GCP region)                 │
│                                                                 │
│ 3. Envoy metadata present AND client IP is GCP range (34.x/35.x)?│
│    → INTER_REGION (service mesh traffic)                        │
│                                                                 │
│ 4. Client IP is GCP range without Envoy metadata?              │
│    → INTER_REGION (GCP service calling directly)                │
│                                                                 │
│ 5. X-Client-Region header = "ZZ"?                               │
│    → INTER_REGION (GCP internal, location unknown)              │
│                                                                 │
│ 6. X-Client-Region contains valid country code?                 │
│    → INTERNET (external client)                                 │
│                                                                 │
│ 7. No region header present?                                    │
│    → UNKNOWN                                                    │
└─────────────────────────────────────────────────────────────────┘
```

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

Enable access logging in your server configuration:

```yaml
accessLogging:
  # Enable/disable the feature
  enabled: true
  
  # GCP region where this server runs (required for inter-region pricing)
  sourceRegion: "us-central1"
  
  # Enable GCP inter-region traffic detection
  detectGcpTraffic: true
  
  # Header containing client country code (set via load balancer)
  clientRegionHeader: "x-client-region"
  
  # Header containing client IP chain
  clientIpHeader: "x-forwarded-for"
  
  # Optional: custom pricing group mappings
  pricingGroups: {}
  
  # Fallback when no location can be resolved
  defaultPricingGroup: "unknown"
```

### GCP Load Balancer Configuration

To populate the `X-Client-Region` header, configure your GCP load balancer backend with custom request headers:

```
X-Client-Region: {client_region}
X-Client-Region-Subdivision: {client_region_subdivision}
```

---

## Log Output

### ACCESS_LOG Entry

Emitted for each query/CDF request with non-zero egress:

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
  "clientRegion": "US"
}
```

### PRICING_CONTEXT Entry

Emitted alongside ACCESS_LOG for debugging and verification:

```json
{
  "logType": "PRICING_CONTEXT",
  "share": "myshare",
  "table": "mytable",
  "timestampMs": 1717502400000,
  "clientIp": "203.0.113.45",
  "clientIpSource": "x-forwarded-for",
  "rawRegionHeader": "US",
  "regionHeaderSource": "x-client-region",
  "hasEnvoyMetadata": false,
  "isGcpIp": false,
  "egressType": "internet",
  "sourceRegion": "us-central1",
  "sourceContinent": "NA",
  "destinationContinent": "NA",
  "pricingTier": "internet_to_na_eu"
}
```

### REQUEST_HEADERS Entry

Emitted alongside ACCESS_LOG and PRICING_CONTEXT for debugging header detection issues:

```json
{
  "logType": "REQUEST_HEADERS",
  "share": "myshare",
  "table": "mytable",
  "timestampMs": 1717502400000,
  "headers": {
    "x-forwarded-for": "34.45.22.184",
    "x-client-region": "US",
    "x-envoy-peer-metadata": "eyJnY3BfbG9jYXRpb24iOiJ1cy1jZW50cmFsMS1mIn0=",
    "content-type": "application/json",
    "authorization": "Bearer ***"
  }
}
```

This log entry contains all request headers (lowercased) and is useful for debugging
why a particular pricing tier was assigned. It helps verify whether the expected
headers (like `x-envoy-peer-metadata` with `gcp_location`) are being passed correctly.

---

## Header Detection Priority

The system checks multiple headers in priority order:

**Region Headers:**
1. Configured `clientRegionHeader` (default: `x-client-region`)
2. `x-appengine-country`
3. `cf-ipcountry`
4. `cloudfront-viewer-country`

**Client IP Headers:**
1. Configured `clientIpHeader` (default: `x-forwarded-for`)
2. `x-envoy-external-address`
3. `x-real-ip`
4. `true-client-ip`

For IP chains (comma-separated), the first public IP is selected.

---

## Implementation Details

### Key Components

| File | Purpose |
|------|---------|
| `GcpPricingTier.scala` | Continent mapping, egress type detection, pricing tier calculation |
| `AccessLogEmitter.scala` | Log entry models and JSON emission |
| `DeltaSharingService.scala` | Integration points for query and CDF endpoints |
| `ServerConfig.scala` | `AccessLoggingConfig` configuration model |

### Egress Bytes Calculation

Egress bytes are calculated by summing the `size` field from all file actions in the response:
- `AddFile` — Regular query results
- `AddFileForCDF` — CDF add file actions
- `AddCDCFile` — CDF change data capture files

---

## Deployment Configurations

The feature is enabled in production environments with the following settings:

**zcloud-prod (us-central1):**
```yaml
accessLogging:
  enabled: true
  sourceRegion: "us-central1"
  detectGcpTraffic: true
```

---

## Notes

- Logs are emitted to a dedicated logger (`delta.sharing.access`) for easy filtering in Cloud Logging
- Zero-byte requests are not logged
- The `PRICING_CONTEXT` log provides full audit trail for pricing decisions
- Pricing tier strings match GCP documentation for easier cost analysis
