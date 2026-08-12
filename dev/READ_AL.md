# Read Access Logs

Query Delta Sharing access logs via Delta Sharing protocol or directly from GCS.

Access logs are stored in per-tenant Delta tables named `access_log_{tenant_id}`, where
`tenant_id` is extracted from the share name (pattern: `{tenant_id}_share`).

## Installation

```bash
# For direct GCS access (recommended)
pip3 install deltalake pandas pyarrow gcsfs

# For Delta Sharing mode
pip3 install delta-sharing pandas
```

## Modes

### Direct GCS Mode (`--direct`)

Reads directly from GCS bucket. Requires `gcloud auth application-default login`.

```bash
# Read _system tenant logs (default)
python3 read_access_logs.py --direct --env zing-dev

# Read specific tenant logs
python3 read_access_logs.py --direct --env zing-dev --tenant ipa7l25ufagwjfmv

# Filter last 7 days
python3 read_access_logs.py --direct --env zing-dev --tenant _system --days 7

# Filter by share name
python3 read_access_logs.py --direct --env zing-dev --tenant mytenantid --share myshare

# Combine filters
python3 read_access_logs.py --direct --env zcloud-prod --tenant _system --days 30 --limit 100
```

### Delta Sharing Mode (default)

Requires `_system_share` to be configured on the server.

```bash
# Using default profile
python3 read_access_logs.py --profile profile-dev.json

# List available shares/tables
python3 read_access_logs.py --profile profile-dev.json --list-tables

# Custom table URL
python3 read_access_logs.py --table-url "profile-prod.json#_system_share.SystemData_v0_1.access_log__system"
```

## Per-Tenant Table Structure

| Share accessed | Table name | GCS path |
|----------------|------------|----------|
| `_system_share` | `access_log__system` | `gs://.../tenant/_system/access_log__system` |
| `ipa7l25ufagwjfmv_share` | `access_log_ipa7l25ufagwjfmv` | `gs://.../tenant/_system/access_log_ipa7l25ufagwjfmv` |
| `hhgp5t6oz3nvczk7_share` | `access_log_hhgp5t6oz3nvczk7` | `gs://.../tenant/_system/access_log_hhgp5t6oz3nvczk7` |

## Environments

| Environment     | GCS Base Path                                      |
|-----------------|----------------------------------------------------|
| `zing-dev`      | gs://zing-dev-197522-dl-v1/datalake/data/tenant/_system |
| `zing-preview`  | gs://zing-preview-dl-v1/datalake/data/tenant/_system    |
| `zcloud-prod`   | gs://zcloud-prod-dl-v1/datalake/data/tenant/_system     |
| `zcloud-prod2`  | gs://zcloud-prod2-dl-v1/datalake/data/tenant/_system    |
| `zcloud-prod3`  | gs://zcloud-prod3-dl-v1/datalake/data/tenant/_system    |

## Output Formats

```bash
# Summary with stats (default)
python3 read_access_logs.py --direct --env zing-dev

# Full table
python3 read_access_logs.py --direct --env zing-dev --output table

# Export to CSV
python3 read_access_logs.py --direct --env zcloud-prod --output csv --output-file logs.csv

# Export to JSON
python3 read_access_logs.py --direct --env zing-preview --limit 100 --output json --output-file logs.json
```

## Options Reference

| Option           | Description                                      |
|------------------|--------------------------------------------------|
| `--direct`       | Read directly from GCS (requires gcloud auth)   |
| `--env`          | Environment for direct mode                      |
| `--tenant`       | Tenant ID for direct mode (default: `_system`)   |
| `--profile`      | Delta Sharing profile file                       |
| `--table-url`    | Full Delta Sharing table URL                     |
| `--list-tables`  | List available shares/schemas/tables             |
| `--days N`       | Filter to last N days                            |
| `--share NAME`   | Filter by share name                             |
| `--limit N`      | Limit number of records                          |
| `--output`       | Format: `summary`, `table`, `csv`, `json`        |
| `--output-file`  | Write output to file (for csv/json)              |

## Output Columns

- `timestampMs` - Request timestamp (milliseconds)
- `share` - Share name accessed
- `schema` - Schema name
- `table` - Table name
- `egressBytes` - Bytes transferred
- `pricingTier` - GCS pricing tier
- `clientRegion` - Client's region