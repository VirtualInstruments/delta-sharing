#!/usr/bin/env python3
"""
Read access logs via Delta Sharing or directly from GCS.

Two modes:
1. Via Delta Sharing (requires _system_share to be configured in server)
2. Direct from GCS (requires gcloud auth and deltalake library)

Install:
    pip3 install delta-sharing pandas
    # For direct GCS access:
    pip3 install deltalake gcsfs

Run (via Delta Sharing):
    python3 read_access_logs.py --profile profile-dev.json
    python3 read_access_logs.py --days 7 --limit 100

Run (direct GCS - when Delta Sharing share not configured):
    python3 read_access_logs.py --direct --env zing-dev --tenant _system
    python3 read_access_logs.py --direct --env zing-dev --tenant ipa7l25ufagwjfmv
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta

import pandas as pd

# GCS base paths by environment (per-tenant tables live under these)
ENV_BASE_PATHS = {
    "zing-dev": "gs://zing-dev-197522-dl-v1/datalake/data/tenant/_system",
    "zing-preview": "gs://zing-preview-dl-v1/datalake/data/tenant/_system",
    "zcloud-prod": "gs://zcloud-prod-dl-v1/datalake/data/tenant/_system",
    "zcloud-prod2": "gs://zcloud-prod2-dl-v1/datalake/data/tenant/_system",
    "zcloud-prod3": "gs://zcloud-prod3-dl-v1/datalake/data/tenant/_system",
}

# Default table URL format: <profile>#<share>.<schema>.<table>
DEFAULT_TABLE_URL = "profile-dev.json#_system_share.SystemData_v0_1.access_log__system"


def list_available_tables(profile: str) -> None:
    """List all shares, schemas, and tables available through the profile."""
    import delta_sharing

    print(f"Listing tables from profile: {profile}")
    client = delta_sharing.SharingClient(profile)

    shares = client.list_shares()
    print(f"\nFound {len(shares)} shares:")

    for share in shares:
        print(f"\n  Share: {share.name}")
        schemas = client.list_schemas(share)
        for schema in schemas:
            print(f"    Schema: {schema.name}")
            tables = client.list_tables(schema)
            for table in tables:
                table_url = f"{profile}#{share.name}.{schema.name}.{table.name}"
                print(f"      Table: {table.name}")
                print(f"        URL: {table_url}")


def read_access_logs_direct(
    env: str,
    tenant: str = "_system",
    days: int | None = None,
    share_filter: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Read access logs directly from GCS using deltalake library."""
    try:
        import deltalake
    except ImportError:
        print("Missing deltalake library. Install with: pip3 install deltalake gcsfs")
        raise SystemExit(1)

    base_path = ENV_BASE_PATHS[env]
    table_path = f"{base_path}/access_log_{tenant}"
    print(f"Reading directly from GCS: {table_path}")

    try:
        dt = deltalake.DeltaTable(table_path)
        print(f"Table version: {dt.version()}")
        print(f"Files: {len(dt.file_uris())}")

        if len(dt.file_uris()) == 0:
            print("\nWARNING: Delta table has 0 files tracked.")
            print("The parquet files may exist but aren't in the Delta log.")
            print("This happens when writes fail due to protocol version mismatch.")
            print("\nTo fix: delete the table and let the server recreate it:")
            print(f"  gsutil -m rm -r {table_path}/")
            return pd.DataFrame()

        df = dt.to_pandas()
    except Exception as e:
        print(f"Error reading Delta table: {e}")
        print("\nTrying to read orphaned parquet files directly...")
        return read_orphaned_parquet(table_path)

    return apply_filters(df, days, share_filter, limit)


def read_orphaned_parquet(table_path: str) -> pd.DataFrame:
    """Read parquet files directly when Delta log is broken."""
    try:
        import gcsfs
        import pyarrow.parquet as pq
    except ImportError:
        print("Missing gcsfs/pyarrow. Install with: pip3 install gcsfs pyarrow")
        raise SystemExit(1)

    # Strip gs:// prefix for gcsfs
    gcs_path = table_path.replace("gs://", "")
    fs = gcsfs.GCSFileSystem()

    # Find all parquet files
    parquet_files = []
    for root, dirs, files in fs.walk(gcs_path):
        for f in files:
            if f.endswith(".parquet"):
                parquet_files.append(f"gs://{root}/{f}")

    print(f"Found {len(parquet_files)} orphaned parquet files")

    if not parquet_files:
        return pd.DataFrame()

    # Read all parquet files
    dfs = []
    for pf in parquet_files[:100]:  # Limit to first 100 files
        try:
            df = pq.read_table(pf, filesystem=fs).to_pandas()
            dfs.append(df)
        except Exception as e:
            print(f"  Warning: could not read {pf}: {e}")

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def apply_filters(
    df: pd.DataFrame,
    days: int | None = None,
    share_filter: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Apply common filters to the DataFrame."""
    if df.empty:
        return df

    # Filter by date range
    if days is not None and "timestampMs" in df.columns:
        now = datetime.now(timezone.utc)
        cutoff_ms = int((now - timedelta(days=days)).timestamp() * 1000)
        df = df[df["timestampMs"] >= cutoff_ms]
        print(f"After date filter ({days} days): {len(df):,} rows")

    # Filter by share name
    if share_filter and "share" in df.columns:
        df = df[df["share"] == share_filter]
        print(f"After share filter ({share_filter}): {len(df):,} rows")

    # Sort by timestamp descending
    if "timestampMs" in df.columns:
        df = df.sort_values("timestampMs", ascending=False)

    if limit:
        df = df.head(limit)

    return df


def read_access_logs(
    table_url: str,
    days: int | None = None,
    share_filter: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Read access logs via Delta Sharing."""
    import delta_sharing

    print(f"Reading from: {table_url}")

    # Get table version first
    try:
        version = delta_sharing.get_table_version(table_url)
        print(f"Table version: {version}")
    except Exception as e:
        print(f"Warning: Could not get table version: {e}")

    # Load table as pandas DataFrame
    df = delta_sharing.load_as_pandas(table_url)
    print(f"Loaded {len(df):,} rows")

    return apply_filters(df, days, share_filter, limit)


def summarize(df: pd.DataFrame) -> None:
    """Print summary statistics."""
    print(f"\n{'=' * 60}")
    print(f"Total records: {len(df):,}")

    if df.empty:
        print("No data found.")
        print(f"{'=' * 60}\n")
        return

    # Show columns
    print(f"Columns: {list(df.columns)}")

    if "egressBytes" in df.columns:
        total_bytes = df["egressBytes"].sum()
        print(f"Total egress: {total_bytes:,.0f} bytes ({total_bytes / 1e9:.2f} GB)")

    if "share" in df.columns:
        print(f"\nBy share:")
        share_stats = (
            df.groupby("share")
            .agg(
                {
                    "egressBytes": "sum",
                }
            )
            .rename(columns={"egressBytes": "bytes"})
        )
        share_stats["requests"] = df.groupby("share").size()
        share_stats["egress_gb"] = share_stats["bytes"] / 1e9
        print(share_stats.sort_values("bytes", ascending=False).head(10).to_string())

    if "pricingTier" in df.columns:
        print(f"\nBy pricing tier:")
        tier_stats = df.groupby("pricingTier")["egressBytes"].sum()
        print(tier_stats.sort_values(ascending=False).to_string())

    if "timestampMs" in df.columns:
        ts = pd.to_datetime(df["timestampMs"], unit="ms", utc=True)
        print(f"\nTime range: {ts.min()} to {ts.max()}")

    print(f"{'=' * 60}\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Read directly from GCS instead of via Delta Sharing",
    )
    parser.add_argument(
        "--env",
        choices=list(ENV_BASE_PATHS.keys()),
        default="zing-dev",
        help="Environment for --direct mode",
    )
    parser.add_argument(
        "--tenant",
        default="_system",
        help="Tenant ID for --direct mode (e.g., _system, ipa7l25ufagwjfmv)",
    )
    parser.add_argument(
        "--table-url",
        default=DEFAULT_TABLE_URL,
        help="Delta Sharing table URL: <profile>#<share>.<schema>.<table>",
    )
    parser.add_argument(
        "--profile",
        help="Override profile file (e.g., profile-dev.json, profile-prod.json)",
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all available shares/schemas/tables and exit",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Filter to last N days",
    )
    parser.add_argument(
        "--share",
        help="Filter by share name (the 'share' column in access logs)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of records returned",
    )
    parser.add_argument(
        "--output",
        choices=["summary", "json", "csv", "table"],
        default="summary",
        help="Output format",
    )
    parser.add_argument(
        "--output-file",
        help="Write output to file (for json/csv)",
    )
    args = parser.parse_args()

    # List tables mode (via Delta Sharing only)
    if args.list_tables:
        profile = args.profile or args.table_url.split("#")[0]
        list_available_tables(profile)
        return 0

    # Read data
    try:
        if args.direct:
            df = read_access_logs_direct(
                env=args.env,
                tenant=args.tenant,
                days=args.days,
                share_filter=args.share,
                limit=args.limit,
            )
        else:
            # Build table URL
            table_url = args.table_url
            if args.profile:
                parts = table_url.split("#")
                if len(parts) == 2:
                    table_url = f"{args.profile}#{parts[1]}"
                else:
                    table_url = args.profile

            df = read_access_logs(
                table_url=table_url,
                days=args.days,
                share_filter=args.share,
                limit=args.limit,
            )
    except Exception as e:
        print(f"Error reading table: {e}")
        import traceback

        traceback.print_exc()
        return 1

    # Output
    if args.output == "summary":
        summarize(df)
        if not df.empty:
            print("Recent entries:")
            # Show a subset of columns for readability
            display_cols = [
                c
                for c in [
                    "timestampMs",
                    "share",
                    "schema",
                    "table",
                    "egressBytes",
                    "pricingTier",
                    "clientRegion",
                ]
                if c in df.columns
            ]
            print(df[display_cols].head(10).to_string())
    elif args.output == "json":
        output = df.to_json(orient="records", date_format="iso", indent=2)
        if args.output_file:
            with open(args.output_file, "w") as f:
                f.write(output)
            print(f"Wrote {len(df)} records to {args.output_file}")
        else:
            print(output)
    elif args.output == "csv":
        if args.output_file:
            df.to_csv(args.output_file, index=False)
            print(f"Wrote {len(df)} records to {args.output_file}")
        else:
            print(df.to_csv(index=False))
    elif args.output == "table":
        print(df.to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
