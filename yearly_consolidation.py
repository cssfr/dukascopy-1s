#!/usr/bin/env python3
"""
Yearly consolidation script for GitHub Actions.
Consolidates daily 1m files into yearly files for the current year only.
Designed for incremental updates with smart downloading - only processes new daily data.
"""

import os
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

def smart_download_for_symbol(symbol: str) -> None:
    """Smart download: read yearly file, determine needed range, download only required daily files"""
    
    # Setup paths
    dst_dir = DST_BASE / f"symbol={symbol}" / f"year={CURRENT_YEAR}"
    dst_file = dst_dir / f"{symbol}_{CURRENT_YEAR}.parquet"
    
    # Get last consolidated date
    last_consolidated_date = get_last_consolidated_date(dst_file)
    
    # Calculate date range to download
    if last_consolidated_date:
        start_date = last_consolidated_date + timedelta(days=1)
        print(f"[{symbol}] Last consolidated: {last_consolidated_date}, downloading from {start_date}")
    else:
        start_date = date(CURRENT_YEAR, 1, 1)
        print(f"[{symbol}] No yearly file found, downloading entire {CURRENT_YEAR}")
    
    # Don't download future dates
    end_date = min(date.today(), date(CURRENT_YEAR, 12, 31))
    
    if start_date > end_date:
        print(f"[{symbol}] Already up-to-date (last: {last_consolidated_date})")
        return
    
    # Build smart date pattern
    date_pattern = build_date_pattern(start_date, end_date)
    include_pattern = f"symbol={symbol}/date={date_pattern}/*"
    
    print(f"[{symbol}] Downloading pattern: {include_pattern}")
    
    # Execute smart download
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
            dst_dir = f"ohlcv/1m/symbol={symbol}/date={date_str}"
            
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

def get_daily_files_to_process(symbol: str, start_date: date | None = None) -> list[Path]:
    """Get list of daily files that need to be processed for this symbol"""
    symbol_dir = SRC_BASE / f"symbol={symbol}"
    if not symbol_dir.exists():
        return []
    
    daily_files = []
    
    # Get all date directories for current year
    for date_dir in symbol_dir.glob(f"date={CURRENT_YEAR}-*"):
        if not date_dir.is_dir():
            continue
            
        try:
            # Extract date from directory name
            date_str = date_dir.name.split("=")[1]
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            # Only include files after start_date
            if start_date is None or file_date > start_date:
                expected_file = date_dir / f"{symbol}_{date_str}.parquet"
                if expected_file.exists():
                    daily_files.append(expected_file)
                    
        except ValueError:
            continue
    
    return sorted(daily_files)

def process_symbol_year(symbol: str) -> None:
    """Consolidate daily files into yearly file for given symbol"""
    
    # Setup paths
    dst_dir = DST_BASE / f"symbol={symbol}" / f"year={CURRENT_YEAR}"
    dst_file = dst_dir / f"{symbol}_{CURRENT_YEAR}.parquet"
    dst_dir.mkdir(parents=True, exist_ok=True)
    
    # Check existing yearly file
    last_consolidated_date = get_last_consolidated_date(dst_file)
    
    # Get daily files that need processing
    daily_files = get_daily_files_to_process(symbol, last_consolidated_date)
    
    if not daily_files:
        if last_consolidated_date:
            print(f"[{symbol}] No new daily files since {last_consolidated_date}")
        else:
            print(f"[{symbol}] No daily files found for {CURRENT_YEAR}")
        return
    
    print(f"[{symbol}] Processing {len(daily_files)} daily files for {CURRENT_YEAR}")
    if last_consolidated_date:
        print(f"[{symbol}] Incremental update from {last_consolidated_date}")
    else:
        print(f"[{symbol}] Creating new yearly file")
    
    try:
        # Read and combine all daily files with schema casting
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
        
        # If yearly file exists, merge with existing data
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
        
        # Sort, deduplicate and save
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