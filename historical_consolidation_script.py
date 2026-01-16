#!/usr/bin/env python3
"""
Historical yearly consolidation script for backfilled data.
Consolidates daily 1s files into yearly files from earliest date until previous year (2024).
Uses EXACTLY the same logic as yearly_consolidation.py but processes historical years.
Designed to be run manually once for backfilled historical data.
"""

import os
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

def build_date_pattern(start_date: date, end_date: date) -> str:
    """Build MinIO include pattern for date range"""
    if start_date.year != end_date.year:
        # Handle year boundary - for now just use full year pattern
        return f"{start_date.year}-*"
    
    if start_date.month == end_date.month:
        # Same month - can be more specific
        if start_date.day == 1 and end_date.day >= 28:
            # Full month
            return f"{start_date.year}-{start_date.month:02d}-*"
        else:
            # Partial month - use broader pattern to be safe
            return f"{start_date.year}-{start_date.month:02d}-*"
    else:
        # Multiple months - use year pattern
        return f"{start_date.year}-*"

def smart_download_for_symbol(symbol: str, target_year: int) -> None:
    """Smart download: read yearly file, determine needed range, download only required daily files"""
    
    # Setup paths
    dst_dir = DST_BASE / f"symbol={symbol}" / f"year={target_year}"
    dst_file = dst_dir / f"{symbol}_{target_year}.parquet"
    
    # Get last consolidated date
    last_consolidated_date = get_last_consolidated_date(dst_file)
    
    # Calculate date range to download - SAME LOGIC AS ORIGINAL
    if last_consolidated_date:
        start_date = last_consolidated_date + timedelta(days=1)
        print(f"[{symbol}] Last consolidated: {last_consolidated_date}, downloading from {start_date}")
    else:
        start_date = date(target_year, 1, 1)  # Use target_year instead of CURRENT_YEAR
        print(f"[{symbol}] No yearly file found, downloading entire {target_year}")
    
    # Don't download future dates - SAME LOGIC AS ORIGINAL
    end_date = min(date.today(), date(target_year, 12, 31))  # Use target_year instead of CURRENT_YEAR
    
    if start_date > end_date:
        print(f"[{symbol}] Already up-to-date (last: {last_consolidated_date})")
        return
    
    # Build smart date pattern
    date_pattern = build_date_pattern(start_date, end_date)
    include_pattern = f"symbol={symbol}/date={date_pattern}/*"
    
    print(f"[{symbol}] Downloading pattern: {include_pattern}")
    
    # Execute smart download - IDENTICAL LOGIC AS ORIGINAL
    try:
        cmd = [
            "mc", "mirror",
            "--exclude", "*",
            "myminio/dukascopy-node/ohlcv/1s/",
            "ohlcv/1s/"
        ]
        
        # Add specific include pattern using multiple excludes (workaround)
        # Since mc doesn't support --include, we'll use mc cp with specific paths
        
        # Alternative approach: use mc cp for specific date ranges
        print(f"[{symbol}] Using mc cp for specific date range: {date_pattern}")
        
        # Calculate specific dates to copy
        current_date = start_date
        files_copied = 0
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            src_path = f"myminio/dukascopy-node/ohlcv/1s/symbol={symbol}/date={date_str}/{symbol}_{date_str}.parquet"
            dst_dir = f"ohlcv/1s/symbol={symbol}/date={date_str}"
            
            # Check if file exists and copy it
            check_cmd = ["mc", "stat", src_path]
            check_result = subprocess.run(check_cmd, capture_output=True, text=True, check=False)
            
            if check_result.returncode == 0:
                # File exists, copy it
                subprocess.run(["mkdir", "-p", dst_dir], check=False)
                copy_cmd = ["mc", "cp", src_path, f"{dst_dir}/"]
                copy_result = subprocess.run(copy_cmd, capture_output=True, text=True, check=False)
                if copy_result.returncode == 0:
                    files_copied += 1
                    print(f"[{symbol}] ✓ Downloaded {date_str}")
                else:
                    print(f"[{symbol}] ⚠️  Failed to copy {date_str}: {copy_result.stderr}")
            
            current_date += timedelta(days=1)
        
        print(f"[{symbol}] ✅ Smart download completed: {files_copied} files copied")
            
    except Exception as e:
        print(f"[{symbol}] ❌ Download error: {str(e)}")

def get_last_consolidated_date(yearly_file_path: Path) -> date | None:
    """Get the last date from existing yearly file"""
    try:
        if not yearly_file_path.exists():
            return None
        
        # Read just the timestamp column to find the latest date
        df = pl.scan_parquet(yearly_file_path).select("timestamp").collect()
        if df.is_empty():
            return None
            
        # Get the latest timestamp and convert to date
        latest_ts = df["timestamp"].max()
        return latest_ts.date() if latest_ts else None
        
    except Exception as e:
        print(f"Warning: Could not read last date from {yearly_file_path}: {e}")
        return None

def get_daily_files_to_process(symbol: str, target_year: int, start_date: date | None = None) -> list[Path]:
    """Get list of daily files that need to be processed for this symbol"""
    symbol_dir = SRC_BASE / f"symbol={symbol}"
    if not symbol_dir.exists():
        return []
    
    daily_files = []
    
    # Get all date directories for target year - SAME PATTERN AS ORIGINAL
    for date_dir in symbol_dir.glob(f"date={target_year}-*"):  # Use target_year instead of CURRENT_YEAR
        if not date_dir.is_dir():
            continue
            
        try:
            # Extract date from directory name
            date_str = date_dir.name.split("=")[1]
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            # Only include files after start_date - SAME LOGIC AS ORIGINAL
            if start_date is None or file_date > start_date:
                expected_file = date_dir / f"{symbol}_{date_str}.parquet"
                if expected_file.exists():
                    daily_files.append(expected_file)
                    
        except ValueError:
            continue
    
    return sorted(daily_files)

def process_symbol_year(symbol: str, target_year: int) -> None:
    """Consolidate daily files into yearly file for given symbol"""
    
    # Setup paths
    dst_dir = DST_BASE / f"symbol={symbol}" / f"year={target_year}"
    dst_file = dst_dir / f"{symbol}_{target_year}.parquet"
    dst_dir.mkdir(parents=True, exist_ok=True)
    
    # Check existing yearly file
    last_consolidated_date = get_last_consolidated_date(dst_file)
    
    # Get daily files that need processing
    daily_files = get_daily_files_to_process(symbol, target_year, last_consolidated_date)
    
    if not daily_files:
        if last_consolidated_date:
            print(f"[{symbol}] No new daily files since {last_consolidated_date}")
        else:
            print(f"[{symbol}] No daily files found for {target_year}")
        return
    
    print(f"[{symbol}] Processing {len(daily_files)} daily files for {target_year}")
    if last_consolidated_date:
        print(f"[{symbol}] Incremental update from {last_consolidated_date}")
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
