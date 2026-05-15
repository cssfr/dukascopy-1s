#!/usr/bin/env python3
"""
Historical yearly consolidation script for backfilled data.
Consolidates daily 1s files into yearly files from earliest date until previous year (2024).
Uses EXACTLY the same logic as yearly_consolidation.py but processes historical years.
Designed to be run manually once for backfilled historical data.
"""

import os
import json
import polars as pl
import subprocess
import argparse
import re
from datetime import datetime, date, timedelta
from pathlib import Path
import yaml

# ─── CONFIG ───────────────────────────────────────────────────────
SRC_BASE = Path("ohlcv/1s")      # Downloaded daily files
DST_BASE = Path("ohlcv/1Ys")      # Yearly consolidation output
SYMBOLS_FILE = Path("symbols.yaml")
CURRENT_YEAR = datetime.now().year
PREVIOUS_YEAR = CURRENT_YEAR - 1  # 2024
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


def list_minio_dailies_for_symbol_year(symbol: str, target_year: int) -> set[date]:
    """List all daily parquet dates that exist for `symbol`, `target_year` in MinIO."""
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
                if d.year == target_year:
                    dates.add(d)
        except (ValueError, json.JSONDecodeError):
            continue
    return dates


def smart_download_for_symbol(symbol: str, target_year: int) -> None:
    """Download every daily parquet present in MinIO whose date is missing
    from the existing yearly. Picks up backfilled historical days correctly,
    unlike the old `last_consolidated_date` watermark."""

    dst_dir = DST_BASE / f"symbol={symbol}" / f"year={target_year}"
    dst_file = dst_dir / f"{symbol}_{target_year}.parquet"

    dates_in_yearly  = get_dates_in_yearly(dst_file)
    dailies_in_minio = list_minio_dailies_for_symbol_year(symbol, target_year)

    # Don't try to pull future dates.
    cutoff = min(date.today(), date(target_year, 12, 31))
    missing = sorted(d for d in (dailies_in_minio - dates_in_yearly) if d <= cutoff)

    if not missing:
        print(f"[{symbol}] {target_year}: yearly already covers every daily in MinIO ({len(dates_in_yearly)} days)")
        return

    print(
        f"[{symbol}] {target_year}: yearly has {len(dates_in_yearly)} days, "
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
            print(f"[{symbol}] ⚠ {date_str}: no parquet at source ({copy_result.stderr.strip()})")

    print(f"[{symbol}] ✅ Smart download completed: {files_copied}/{len(missing)} files copied")


def get_daily_files_to_process(symbol: str, target_year: int, skip_dates: set[date]) -> list[Path]:
    """List local daily parquets for `symbol`/`target_year` whose date is NOT in skip_dates."""
    symbol_dir = SRC_BASE / f"symbol={symbol}"
    if not symbol_dir.exists():
        return []

    daily_files: list[Path] = []
    for date_dir in symbol_dir.glob(f"date={target_year}-*"):
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


def process_symbol_year(symbol: str, target_year: int) -> None:
    """Consolidate daily files into yearly file for given symbol"""

    dst_dir = DST_BASE / f"symbol={symbol}" / f"year={target_year}"
    dst_file = dst_dir / f"{symbol}_{target_year}.parquet"
    dst_dir.mkdir(parents=True, exist_ok=True)

    dates_in_yearly = get_dates_in_yearly(dst_file)
    daily_files     = get_daily_files_to_process(symbol, target_year, dates_in_yearly)

    if not daily_files:
        if dates_in_yearly:
            print(f"[{symbol}] {target_year}: no new daily files (yearly already covers {len(dates_in_yearly)} days)")
        else:
            print(f"[{symbol}] {target_year}: no daily files found")
        return

    print(f"[{symbol}] {target_year}: processing {len(daily_files)} daily files")
    if dates_in_yearly:
        print(f"[{symbol}] Merging into yearly that already has {len(dates_in_yearly)} days")
    else:
        print(f"[{symbol}] Creating new yearly file")
    
    try:
        # Read and combine all daily files with schema casting - IDENTICAL TO ORIGINAL
        dfs = []
        for f in daily_files:
            df = pl.scan_parquet(str(f)).with_columns([
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
        
        # If yearly file exists, merge with existing data - IDENTICAL TO ORIGINAL
        if dst_file.exists():
            print(f"[{symbol}] Merging with existing yearly data")
            existing_df = pl.scan_parquet(dst_file).with_columns([
                pl.col('open').cast(pl.Float64),
                pl.col('high').cast(pl.Float64),
                pl.col('low').cast(pl.Float64),
                pl.col('close').cast(pl.Float64),
                pl.col('volume').cast(pl.Float64)
            ])
            combined_df = pl.concat([existing_df, new_data])
        else:
            combined_df = new_data
        
        # Sort, deduplicate and save - IDENTICAL TO ORIGINAL
        print(f"[{symbol}] Sorting and deduplicating data")
        final_df = (combined_df
                   .sort('timestamp')
                   .unique()
                   .collect())
        
        final_df.write_parquet(dst_file)
        
        record_count = len(final_df)
        print(f"✅ [{symbol}] Saved {dst_file.relative_to(DST_BASE)} with {record_count:,} records")
        
    except Exception as e:
        print(f"❌ [{symbol}] Error processing: {str(e)}")

def run_mc_command(cmd: str) -> str:
    """Run MinIO client command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command '{cmd}': {e}")
        return ""

def get_available_years_for_symbol(symbol: str) -> list[int]:
    """Scan MinIO bucket to discover what years are actually available for this symbol"""
    print(f"[{symbol}] Scanning MinIO bucket for available years...")
    
    # Path pattern: ohlcv/1s/symbol=SYMBOL/
    path_pattern = f"myminio/dukascopy-node/ohlcv/1s/symbol={symbol}/"
    
    # Get list of date directories
    dates_output = run_mc_command(f"mc ls {path_pattern}")
    if not dates_output:
        print(f"[{symbol}] No data found in MinIO bucket")
        return []
    
    # Extract years from date directories
    years = set()
    for line in dates_output.split('\n'):
        if 'date=' in line:
            # Extract date from directory name like "date=2023-01-15/"
            date_match = re.search(r'date=(\d{4})-\d{2}-\d{2}/', line)
            if date_match:
                year = int(date_match.group(1))
                years.add(year)
    
    available_years = sorted(list(years))
    
    if available_years:
        print(f"[{symbol}] Found data for years: {available_years[0]} to {available_years[-1]}")
    else:
        print(f"[{symbol}] No valid date directories found")
    
    return available_years

def get_historical_years_for_symbol(symbol: str) -> list[int]:
    """Get the list of historical years to process for this symbol (earliest available until PREVIOUS_YEAR)"""
    available_years = get_available_years_for_symbol(symbol)
    
    if not available_years:
        print(f"[{symbol}] No historical years to process - no data found")
        return []
    
    # Filter to only historical years (up to PREVIOUS_YEAR)
    historical_years = [year for year in available_years if year <= PREVIOUS_YEAR]
    
    if not historical_years:
        print(f"[{symbol}] No historical years to process - all data is current year or newer")
        return []
    
    earliest_year = min(historical_years)
    latest_year = max(historical_years)
    
    print(f"[{symbol}] Will process historical years: {earliest_year} to {latest_year}")
    
    return historical_years

def main():
    parser = argparse.ArgumentParser(description="Historical yearly consolidation for backfilled data")
    parser.add_argument("--download-only", action="store_true", help="Only perform smart download")
    parser.add_argument("--consolidate-only", action="store_true", help="Only perform consolidation")
    parser.add_argument("--symbol", help="Symbol to process")
    parser.add_argument("--year", type=int, help="Specific year to process")
    
    args = parser.parse_args()
    
    if args.download_only:
        if not args.symbol:
            raise SystemExit("--symbol required with --download-only")
        if not args.year:
            raise SystemExit("--year required with --download-only")
        smart_download_for_symbol(args.symbol, args.year)
        return
    
    if not SYMBOLS_FILE.exists():
        raise SystemExit(f"symbols.yaml not found")
    
    # Load symbols
    symbols = yaml.safe_load(SYMBOLS_FILE.read_text())
    
    if args.consolidate_only:
        # Consolidation-only mode
        print(f"Running historical consolidation-only mode (earliest year to {PREVIOUS_YEAR})")
        
        if not SRC_BASE.exists():
            print(f"Warning: Source directory {SRC_BASE} not found - no daily files to process")
            return
        
        DST_BASE.mkdir(parents=True, exist_ok=True)
        
        print(f"Source: {SRC_BASE}")
        print(f"Destination: {DST_BASE}")
        
        # Process specified symbol or all symbols
        symbols_to_process = [args.symbol] if args.symbol else sorted(symbols.keys())
        print(f"Symbols: {symbols_to_process}")
        
        processed = 0
        errors = 0
        
        for symbol in symbols_to_process:
            try:
                # Get historical years for this symbol from MinIO bucket
                historical_years = get_historical_years_for_symbol(symbol)
                if not historical_years:
                    print(f"[{symbol}] Skipping - no historical years to process")
                    continue
                
                # Process each year
                for year in historical_years:
                    try:
                        process_symbol_year(symbol, year)
                        processed += 1
                    except Exception as e:
                        print(f"❌ Error processing {symbol} year {year}: {str(e)}")
                        errors += 1
                        continue
                        
            except Exception as e:
                print(f"❌ Error processing symbol {symbol}: {str(e)}")
                errors += 1
                continue
        
        print(f"\n=== Historical Consolidation Summary ===")
        print(f"Processing: earliest available year to {PREVIOUS_YEAR}")
        print(f"Years processed: {processed}")
        print(f"Errors: {errors}")
        if DST_BASE.exists():
            print(f"Output location: {DST_BASE}")
    
    else:
        # Legacy mode: do both download and consolidation
        print("Warning: Running in legacy mode. Consider using --download-only and --consolidate-only")
        
        DST_BASE.mkdir(parents=True, exist_ok=True)
        
        # Process specified symbol or all symbols
        symbols_to_process = [args.symbol] if args.symbol else sorted(symbols.keys())
        print(f"Symbols: {symbols_to_process}")
        
        processed = 0
        errors = 0
        
        for symbol in symbols_to_process:
            try:
                # Get historical years for this symbol from MinIO bucket
                historical_years = get_historical_years_for_symbol(symbol)
                if not historical_years:
                    print(f"[{symbol}] Skipping - no historical years to process")
                    continue
                
                # Process each year
                for year in historical_years:
                    try:
                        print(f"\n[{symbol}] {year}: Starting smart download and consolidation")
                        smart_download_for_symbol(symbol, year)
                        process_symbol_year(symbol, year)
                        processed += 1
                    except Exception as e:
                        print(f"❌ Error processing {symbol} year {year}: {str(e)}")
                        errors += 1
                        continue
                        
            except Exception as e:
                print(f"❌ Error processing symbol {symbol}: {str(e)}")
                errors += 1
                continue
        
        print(f"\n=== Historical Summary ===")
        print(f"Processing: earliest available year to {PREVIOUS_YEAR}")
        print(f"Years processed: {processed}")
        print(f"Errors: {errors}")
        if DST_BASE.exists():
            print(f"Output location: {DST_BASE}")

if __name__ == "__main__":
    main()
