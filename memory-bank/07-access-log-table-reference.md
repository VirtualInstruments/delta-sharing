# System Tenant: Delta Sharing Access Logs

Technical reference for writing Delta Sharing access logs to the Data Access feature.

> The schema in this file is the original table design. The shipped writer
> (`DeltaAccessLogWriter.scala`) writes an **unpartitioned** table with additional audit columns —
> see [06-egress-monitoring.md](06-egress-monitoring.md) for the schema actually in use. Treat that
> file as authoritative for the writer; this one for table locations, retention, and query patterns.

## Overview

The Delta Sharing server (deployed in Kubernetes) writes access logs to a Delta table managed by the `_system` tenant. This enables billing analytics, usage monitoring, and audit trails for all Delta Sharing activity.

## Table Details

| Property | Value |
|----------|-------|
| **Table Name** | `access_log_br__system` |
| **Template** | `access_log_br` |
| **Partitioning** | None — the shipped writer writes unpartitioned files |
| **Change Data Feed** | Enabled |

### GCS Locations by Environment

| Environment | Table Location |
|-------------|----------------|
| zing-dev | `gs://zing-dev-197522-dl-v1/datalake/data/tenant/_system/access_log_br__system` |
| zing-preview | `gs://zing-preview-dl-v1/datalake/data/tenant/_system/access_log_br__system` |
| zcloud-prod | `gs://zcloud-prod-dl-v1/datalake/data/tenant/_system/access_log_br__system` |
| zcloud-prod2 | `gs://zcloud-prod2-dl-v1/datalake/data/tenant/_system/access_log_br__system` |
| zcloud-prod3 | `gs://zcloud-prod3-dl-v1/datalake/data/tenant/_system/access_log_br__system` |

## Schema

All columns must be provided by the writer. The `year`, `month`, and `day` partition columns are computed by the writer from `timestampMs`.

### Required Columns

| Column | Type | Description |
|--------|------|-------------|
| `logType` | STRING | Log entry type, e.g., `"ACCESS_LOG"` |
| `share` | STRING | Name of the Delta Share accessed |
| `schema` | STRING | Schema name within the share |
| `table` | STRING | Table name accessed |
| `egressBytes` | LONG | Number of bytes transferred |
| `timestampMs` | LONG | Unix timestamp in **milliseconds** |
| `year` | INT | Partition: `YEAR(timestampMs)` - computed by writer |
| `month` | INT | Partition: `MONTH(timestampMs)` - computed by writer |
| `day` | INT | Partition: `DAY(timestampMs)` - computed by writer |

### Optional Columns

| Column | Type | Description |
|--------|------|-------------|
| `pricingTier` | STRING | Egress pricing tier (e.g., `"internet_to_na_eu"`, `"same_region"`) |
| `requestType` | STRING | Type of request (e.g., `"query"`, `"metadata"`, `"getFiles"`) |
| `clientRegion` | STRING | Client's geographic region (e.g., `"US"`, `"EU"`, `"APAC"`) |

**Note**: The table uses protocol (1,2) for compatibility with Delta Standalone writers. No generated columns - the writer must compute partition values.

## Writing Access Logs

### JSON Record Format

```json
{
  "logType": "ACCESS_LOG",
  "share": "customer123_share",
  "schema": "DataAccess_v0_1",
  "table": "metric_ag_customer123",
  "egressBytes": 1048576,
  "pricingTier": "internet_to_na_eu",
  "timestampMs": 1717502400000,
  "requestType": "query",
  "clientRegion": "US"
}
```

### Using Spark/PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Schema for writing (excludes generated columns)
write_schema = StructType(
    [
        StructField("logType", StringType(), False),
        StructField("share", StringType(), False),
        StructField("schema", StringType(), False),
        StructField("table", StringType(), False),
        StructField("egressBytes", LongType(), False),
        StructField("pricingTier", StringType(), True),
        StructField("timestampMs", LongType(), False),
        StructField("requestType", StringType(), True),
        StructField("clientRegion", StringType(), True),
    ]
)

# Write to Delta table
df.write.format("delta").mode("append").save(
    "gs://<bucket>/datalake/data/tenant/_system/access_log_br__system"
)
```

### Using Delta Lake Rust/Python SDK

```python
import deltalake

# Append records
deltalake.write_deltalake(
    "gs://<bucket>/datalake/data/tenant/_system/access_log_br__system",
    data,  # pandas DataFrame or PyArrow Table
    mode="append",
)
```

## Integration Notes for Delta Sharing Server

### Kubernetes Deployment Considerations

1. **GCS Credentials**: The server pod needs a GCP service account with write access to the `_system` tenant path. Mount the service account key as a secret or use Workload Identity.

2. **Batching**: Consider batching log writes to reduce I/O overhead:
   - Buffer logs in memory (e.g., 1000 records or 60 seconds)
   - Write in batch to Delta table
   - Flush on graceful shutdown

3. **Async Writing**: Log writes should be async to avoid impacting request latency. Use a background thread/coroutine with a bounded queue.

4. **Error Handling**: Log write failures should not fail client requests. Log errors to stderr and emit metrics for monitoring.

### Environment Detection

Determine the correct GCS bucket based on the Kubernetes namespace or environment variable:

```python
ENV_BUCKETS = {
    "zing-dev": "zing-dev-197522-dl-v1",
    "zing-preview": "zing-preview-dl-v1",
    "zcloud-prod": "zcloud-prod-dl-v1",
    "zcloud-prod2": "zcloud-prod2-dl-v1",
    "zcloud-prod3": "zcloud-prod3-dl-v1",
}


def get_access_log_path(env: str) -> str:
    bucket = ENV_BUCKETS[env]
    return f"gs://{bucket}/datalake/data/tenant/_system/access_log_br__system"
```

### Timestamp Handling

- `timestampMs` must be Unix epoch in **milliseconds** (not seconds)
- Use `System.currentTimeMillis()` (Java), `time.time_ns() // 1_000_000` (Python), or `Date.now()` (JS)
- The `timestamp`, `year`, `month`, `day` columns are generated automatically on write

### Pricing Tier Values

Suggested values for `pricingTier` based on GCP egress pricing:

| Value | Description |
|-------|-------------|
| `same_region` | Client and server in same GCP region |
| `same_continent` | Cross-region, same continent |
| `internet_to_na_eu` | Internet egress to North America/Europe |
| `internet_to_apac` | Internet egress to Asia-Pacific |
| `internet_to_other` | Internet egress to other regions |

### Request Type Values

Suggested values for `requestType`:

| Value | Description |
|-------|-------------|
| `listShares` | List available shares |
| `listSchemas` | List schemas in a share |
| `listTables` | List tables in a schema |
| `getTableMetadata` | Get table metadata |
| `getTableVersion` | Get table version |
| `query` | Query/read table data |
| `getFiles` | Get files for a table version |

## Querying Access Logs

Once data is written, it's accessible via Delta Sharing under:
- **Share**: `_system_share`
- **Schema**: `SystemData_v0_1`
- **Table**: `access_log_br__system`

### Example Queries

```sql
-- Total egress by share (last 30 days)
SELECT share, SUM(egressBytes) as total_bytes
FROM access_log_br__system
WHERE year = 2026 AND month = 6
GROUP BY share
ORDER BY total_bytes DESC;

-- Request counts by type
SELECT requestType, COUNT(*) as requests
FROM access_log_br__system
WHERE timestamp >= current_timestamp - INTERVAL 7 DAYS
GROUP BY requestType;

-- Daily egress trend
SELECT year, month, day, SUM(egressBytes) as daily_bytes
FROM access_log_br__system
GROUP BY year, month, day
ORDER BY year, month, day;
```

## Retention

| Environment | Retention |
|-------------|-----------|
| zing-dev | 30 days |
| zing-preview | 90 days |
| Production (prod/prod2/prod3) | 450 days (15 months) |

## See Also

- [06-egress-monitoring.md](06-egress-monitoring.md) — how the server classifies egress and writes
  these records
- [../AGENTS.md](../AGENTS.md) — project overview and CLI commands

The schema template (`templates/access_log_br.json`) and the per-environment `_system` tenant
configs live in the `deltalake-admin` repo, which pre-creates this table during tenant onboarding.
