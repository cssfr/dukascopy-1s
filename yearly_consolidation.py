#!/usr/bin/env python3
"""
Yearly consolidation script for GitHub Actions.
Consolidates daily 1m files into yearly files for the current year only.
Designed for incremental updates with smart downloading - only processes new daily data.
"""

import os
import json
import polars as pl
import subprocess
import argparse
from datetime import datetime, date, timedelta
from pathlib import Path
import yaml

# ─── CONFIG ───────────────────────────────────────────────────────
SRC_BASE = Path("ohlcv/1s")      # Downloaded daily files
DST_BASE = Path("ohlcv/1Ys")      # Yearly consolidation output
SYMBOLS_FILE = Path("symbols.yaml")
CURRENT_YEAR = datetime.now().year
# ─────────────────────────────────────────────────────────────────

def get_dates_in_yearly(yearly_file_path: Path) -> set[date]:
    """Return the set of UTC dates already represented in the yearly parquet.

    Replaces the old `last_consolidated_date` watermark, which silently
    skipped backfilled days that fell before the yearly's max timestamp.
    """
    if not yearly_file_path.exists():
        return set()
    try:
        df = (
            pl.scan_parquet(yearly_file_path)
              .select(
                  pl.col("timestamp").cast(pl.Datetime("us", "UTC")).dt.date().alias("d")
              )
              .unique()
              .collect()
        )
        return set(df["d"].to_list())
    except Exception as e:
        print(f"Warning: Could not read dates from {yearly_file_path}: {e}")
        return set()


def list_minio_dailies_for_symbol(symbol: str) -> set[date]:
    """List all (current-year) daily parquet dates that exist for `symbol` in MinIO."""
    try:
        proc = subprocess.run(
            ["mc", "ls", "--json", f"myminio/dukascopy-node/ohlcv/1s/symbol={symbol}/"],
            capture_output=True, text=True, check=False,
        )
    except Exception as e:
        print(f"[{symbol}] Warning: mc ls failed: {e}")
        return set()
    if proc.returncode != 0:
        return set()

    dates: set[date] = set()
    for line in proc.stdout.splitlines():
        try:
            obj = json.loads(line)
            key = obj.get("key", "")
            if "date=" in key and key.endswith("/"):
                date_str = key.split("date=")[1].rstrip("/")
                d = datetime.strptime(date_str, "%Y-%m-%d").date()
                if d.year == CURRENT_YEAR:
                    dates.add(d)
        except (ValueError, json.JSONDecodeError):
            continue
    return dates


def smart_download_for_symbol(symbol: str) -> None:
    """Download every daily parquet present in MinIO whose date is missing
    from the existing yearly. Picks up backfilled historical days correctly,
    unlike the old `last_consolidated_date` watermark."""

    dst_dir = DST_BASE / f"symbol={symbol}" / f"year={CURRENT_YEAR}"
    dst_file = dst_dir / f"{symbol}_{CURRENT_YEAR}.parquet"

    dates_in_yearly  = get_dates_in_yearly(dst_file)
    dailies_in_minio = list_minio_dailies_for_symbol(symbol)

    # Don't try to pull future dates even if a stray entry shows up.
    cutoff = min(date.today(), date(CURRENT_YEAR, 12, 31))
    missing = sorted(d for d in (dailies_in_minio - dates_in_yearly) if d <= cutoff)

    if not missing:
        print(f"[{symbol}] Yearly already covers every daily in MinIO ({len(dates_in_yearly)} days)")
        return

    print(
        f"[{symbol}] Yearly has {len(dates_in_yearly)} days, "
        f"MinIO has {len(dailies_in_minio)} dailies → "
        f"{len(missing)} day(s) need merging"
    )

    files_copied = 0
    for d in missing:
        date_str = d.strftime("%Y-%m-%d")
        src_path = f"myminio/dukascopy-node/ohlcv/1s/symbol={symbol}/date={date_str}/{symbol}_{date_str}.parquet"
        dst_subdir = f"ohlcv/1s/symbol={symbol}/date={date_str}"
        subprocess.run(["mkdir", "-p", dst_subdir], check=False)
        copy_result = subprocess.run(
            ["mc", "cp", src_path, f"{dst_subdir}/"],
            capture_output=True, text=True, check=False,
        )
        if copy_result.returncode == 0:
            files_copied += 1
            print(f"[{symbol}] ✓ Downloaded {date_str}")
        else:
            # Date directory might exist without a parquet (e.g. EMPTY status day) — silent skip.
            print(f"[{symbol}] ⚠ {date_str}: no parquet at source ({copy_result.stderr.strip()})")

    print(f"[{symbol}] ✅ Smart download completed: {files_copied}/{len(missing)} files copied")


def get_daily_files_to_process(symbol: str, skip_dates: set[date]) -> list[Path]:
    """List local daily parquets for `symbol` whose date is NOT in skip_dates."""
    symbol_dir = SRC_BASE / f"symbol={symbol}"
    if not symbol_dir.exists():
        return []

    daily_files: list[Path] = []
    for date_dir in symbol_dir.glob(f"date={CURRENT_YEAR}-*"):
        if not date_dir.is_dir():
            continue
        try:
            date_str = date_dir.name.split("=")[1]
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        if file_date in skip_dates:
            continue
        expected_file = date_dir / f"{symbol}_{date_str}.parquet"
        if expected_file.exists():
            daily_files.append(expected_file)

    return sorted(daily_files)


def process_symbol_year(symbol: str) -> None:
    """Consolidate daily files into yearly file for given symbol"""

    dst_dir = DST_BASE / f"symbol={symbol}" / f"year={CURRENT_YEAR}"
    dst_file = dst_dir / f"{symbol}_{CURRENT_YEAR}.parquet"
    dst_dir.mkdir(parents=True, exist_ok=True)

    dates_in_yearly = get_dates_in_yearly(dst_file)
    daily_files     = get_daily_files_to_process(symbol, dates_in_yearly)

    if not daily_files:
        if dates_in_yearly:
            print(f"[{symbol}] No new daily files (yearly already covers {len(dates_in_yearly)} days)")
        else:
            print(f"[{symbol}] No daily files found for {CURRENT_YEAR}")
        return

    print(f"[{symbol}] Processing {len(daily_files)} daily files for {CURRENT_YEAR}")
    if dates_in_yearly:
        print(f"[{symbol}] Merging into yearly that already has {len(dates_in_yearly)} days")
    else:
        print(f"[{symbol}] Creating new yearly file")

    try:
        # Read and combine all daily files with schema casting
        dfs = []
        for f in daily_files:
            df = pl.scan_parquet(str(f)).with_columns([
                pl.col('timestamp').cast(pl.Datetime('us', 'UTC')),  # Normalize timestamp precision
                pl.col('open').cast(pl.Float64),
                pl.col('high').cast(pl.Float64),
                pl.col('low').cast(pl.Float64),
                pl.col('close').cast(pl.Float64),
                pl.col('volume').cast(pl.Float64)
            ])
            dfs.append(df)

        if not dfs:
            print(f"[{symbol}] No valid data found")
            return

        new_data = pl.concat(dfs)

        # If yearly file exists, merge with existing data
        if dst_file.exists():
            print(f"[{symbol}] Merging with existing yearly data")
            existing_df = pl.scan_parquet(dst_file).with_columns([
                pl.col('timestamp').cast(pl.Datetime('us', 'UTC')),  # Normalize timestamp precision
                pl.col('open').cast(pl.Float64),
                pl.col('high').cast(pl.Float64),
                pl.col('low').cast(pl.Float64),
                pl.col('close').cast(pl.Float64),
                pl.col('volume').cast(pl.Float64)
            ])
            combined_df = pl.concat([existing_df, new_data])
        else:
            combined_df = new_data

        # Sort, deduplicate and save
        print(f"[{symbol}] Sorting and deduplicating data")
        final_df = (combined_df
                   .sort('timestamp')
                   .unique()
                   .collect())

        # SAFETY CHECK: refuse to write a yearly that would drop existing dates.
        # If `dates_in_yearly` was non-empty but the result is missing some of
        # those dates, the workflow probably didn't sync the existing yearly
        # from MinIO — bail out rather than overwriting source-of-truth data.
        if dates_in_yearly:
            new_dates = set(
                final_df.select(
                    pl.col('timestamp').cast(pl.Datetime('us', 'UTC')).dt.date().alias('d')
                )['d'].to_list()
            )
            lost = dates_in_yearly - new_dates
            if lost:
                lost_sample = sorted(lost)[:5]
                raise RuntimeError(
                    f"[{symbol}] REFUSING to write yearly — would drop "
                    f"{len(lost)} existing date(s) (e.g. {lost_sample}). "
                    f"The existing yearly likely wasn't downloaded from MinIO "
                    f"before running this script."
                )

        final_df.write_parquet(dst_file)

        record_count = len(final_df)
        print(f"✅ [{symbol}] Saved {dst_file.relative_to(DST_BASE)} with {record_count:,} records")

    except Exception as e:
        print(f"❌ [{symbol}] Error processing: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Yearly consolidation with smart downloading")
    parser.add_argument("--download-only", action="store_true", help="Only perform smart download")
    parser.add_argument("--consolidate-only", action="store_true", help="Only perform consolidation")
    parser.add_argument("--symbol", help="Symbol to process (for download-only mode)")

    args = parser.parse_args()

    if args.download_only:
        if not args.symbol:
            raise SystemExit("--symbol required with --download-only")
        smart_download_for_symbol(args.symbol)
        return

    if not SYMBOLS_FILE.exists():
        raise SystemExit(f"symbols.yaml not found")

    # Load symbols
    symbols = yaml.safe_load(SYMBOLS_FILE.read_text())

    if args.consolidate_only:
        # Consolidation-only mode
        print(f"Running consolidation-only mode for {CURRENT_YEAR}")

        if not SRC_BASE.exists():
            print(f"Warning: Source directory {SRC_BASE} not found - no daily files to process")
            return

        DST_BASE.mkdir(parents=True, exist_ok=True)

        print(f"Source: {SRC_BASE}")
        print(f"Destination: {DST_BASE}")
        print(f"Symbols: {list(symbols.keys())}")

        processed = 0
        errors = 0

        for symbol in sorted(symbols.keys()):
            try:
                process_symbol_year(symbol)
                processed += 1
            except Exception as e:
                print(f"❌ Error processing symbol {symbol}: {str(e)}")
                errors += 1
                continue

        print(f"\n=== Consolidation Summary ===")
        print(f"Year: {CURRENT_YEAR}")
        print(f"Symbols processed: {processed}")
        print(f"Errors: {errors}")
        if DST_BASE.exists():
            print(f"Output location: {DST_BASE}")

    else:
        # Legacy mode: do both download and consolidation
        print("Warning: Running in legacy mode. Consider using --download-only and --consolidate-only")

        DST_BASE.mkdir(parents=True, exist_ok=True)

        processed = 0
        errors = 0

        for symbol in sorted(symbols.keys()):
            try:
                print(f"\n[{symbol}] Starting smart download and consolidation")
                smart_download_for_symbol(symbol)
                process_symbol_year(symbol)
                processed += 1
            except Exception as e:
                print(f"❌ Error processing symbol {symbol}: {str(e)}")
                errors += 1
                continue

        print(f"\n=== Summary ===")
        print(f"Year: {CURRENT_YEAR}")
        print(f"Symbols processed: {processed}")
        print(f"Errors: {errors}")
        if DST_BASE.exists():
            print(f"Output location: {DST_BASE}")

if __name__ == "__main__":
    main()
