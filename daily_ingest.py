#!/usr/bin/env python3
"""Daily gap‑filling Dukascopy ingest utility.

Reads existing parquet files in ./ohlcv/1s (synced from your MinIO bucket),
finds the newest date per symbol, and downloads any missing days up to
yesterday UTC using dukascopy-node. Converts each day to Parquet with a
unix_time column and saves it back into the same folder hierarchy.

Usage (nightly cron):
    python daily_ingest.py

Usage (manual backfill):
    python daily_ingest.py --symbols ES,NQ --from 1990-01-01
"""

import subprocess
import time
from datetime import datetime, timedelta, date
from pathlib import Path
import argparse
import yaml
import pandas as pd
import numpy as np

# -----------------------------------------------------------------------------
#  Paths & config
# -----------------------------------------------------------------------------
OUTPUT_DIR = Path("ohlcv/1s")      # local mirror of your bucket; sync before/after
DOWNLOAD_DIR = Path("download")    # transient CSVs
SYMBOLS_FILE = Path("symbols.yaml")

if not SYMBOLS_FILE.exists():
    raise SystemExit("symbols.yaml not found. Commit it alongside this script.")

SYMBOLS = yaml.safe_load(SYMBOLS_FILE.read_text())

# -----------------------------------------------------------------------------
#  Converter with comprehensive schema definition
# -----------------------------------------------------------------------------
def convert_to_parquet(input_csv_path: Path, output_parquet_path: Path, symbol: str):
    df = pd.read_csv(str(input_csv_path))

    # Define expected schema for comprehensive casting using numpy types
    schema_mapping = {
        'open': np.float64,
        'high': np.float64, 
        'low': np.float64,
        'close': np.float64,
        'volume': np.float64
    }
    
    # Apply schema casting for all expected columns
    for col, dtype in schema_mapping.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    # Ensure UTC, independent of machine timezone
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M', utc=True)

    # Add unix epoch seconds
    df['unix_time'] = df['timestamp'].astype(np.int64) // 10**9

    # Insert symbol column at position 0
    df.insert(0, 'symbol', symbol)

    # Re‑order so unix_time is right after timestamp
    cols = df.columns.tolist()
    cols.insert(cols.index('timestamp') + 1, cols.pop(cols.index('unix_time')))
    df = df[cols]

    df.to_parquet(str(output_parquet_path), index=False)

# -----------------------------------------------------------------------------
#  Downloader (verbatim signature)
# -----------------------------------------------------------------------------
def run_dukascopy(symbol_id: str, date_str: str):
    next_day = (datetime.fromisoformat(date_str) + timedelta(days=1)).strftime("%Y-%m-%d")
    cmd = (
        f"npx dukascopy-node -i {symbol_id} -from {date_str} -to {next_day} "
        f"-t s1 -f csv --date-format \"YYYY-MM-DD HH:mm\" -v -fl"
    )
    print("Running:", cmd)
    subprocess.run(cmd, check=True, shell=True)

# -----------------------------------------------------------------------------
#  Helper – latest ingested day with new structure
# -----------------------------------------------------------------------------
def newest_parquet_date(symbol_key: str) -> date | None:
    folder = OUTPUT_DIR / f"symbol={symbol_key}"
    if not folder.exists():
        return None
    dates: list[date] = []
    
    # Look for date directories in new structure: date=YYYY-MM-DD/
    for date_dir in folder.glob("date=*"):
        if date_dir.is_dir():
            try:
                date_str = date_dir.name.split("=")[1]
                d = datetime.strptime(date_str, "%Y-%m-%d").date()
                # Verify the parquet file actually exists
                expected_file = date_dir / f"{symbol_key}_{date_str}.parquet"
                if expected_file.exists():
                    dates.append(d)
            except ValueError:
                continue
    return max(dates) if dates else None

# -----------------------------------------------------------------------------
def daterange(start: date, end: date):
    while start <= end:
        yield start
        start += timedelta(days=1)

def ingest_symbol(symbol_key: str, start_override: date | None, end_date: date):
    meta = SYMBOLS[symbol_key]
    dukas_id = meta['id']
    earliest_date = datetime.strptime(meta['earliest_date'], "%Y-%m-%d").date()

    # Determine start
    if start_override:
        start_date = start_override
    else:
        latest = newest_parquet_date(symbol_key)
        start_date = (latest + timedelta(days=1)) if latest else earliest_date

    start_date = max(start_date, earliest_date)
    if start_date > end_date:
        print(f"[{symbol_key}] up‑to‑date.")
        return

    DOWNLOAD_DIR.mkdir(exist_ok=True, parents=True)

    for day in daterange(start_date, end_date):
        date_str = day.strftime("%Y-%m-%d")
        next_day_str = (day + timedelta(days=1)).strftime("%Y-%m-%d")
        
        # New path structure: ohlcv/1s/symbol=BTC/date=2017-05-08/BTC_2017-05-08.parquet
        parquet_path = OUTPUT_DIR / f"symbol={symbol_key}" / f"date={date_str}" / f"{symbol_key}_{date_str}.parquet"

        if parquet_path.exists():
            print(f"[{symbol_key}] {date_str} already ingested.")
            continue

        try:
            run_dukascopy(dukas_id, date_str)
            time.sleep(1)  # polite

            csv_name = f"{dukas_id}-s1-bid-{date_str}-{next_day_str}.csv"
            csv_path = DOWNLOAD_DIR / csv_name
            if not csv_path.exists() or csv_path.stat().st_size == 0:
                print(f"[{symbol_key}] CSV {csv_name} not found or empty (weekend/holiday), skipping.")
                continue

            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            convert_to_parquet(csv_path, parquet_path, symbol_key)
            print(f"[{symbol_key}] saved {parquet_path.relative_to(OUTPUT_DIR)}")
            # csv_path.unlink(missing_ok=True)  # uncomment to auto‑delete
        except subprocess.CalledProcessError as e:
            print(f"[{symbol_key}] dukascopy-node failed on {date_str}: {e}")

# -----------------------------------------------------------------------------
#  CLI
# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Gap‑fill Dukascopy data to Parquet.")
    ap.add_argument("--symbols", help="Comma‑separated list (default ALL)")
    ap.add_argument("--from", dest="from_date", help="Start YYYY‑MM‑DD (manual)")
    ap.add_argument("--to", dest="to_date", help="End YYYY‑MM‑DD (manual)")
    opts = ap.parse_args()

    symbols = [s.strip() for s in opts.symbols.split(",")] if opts.symbols else list(SYMBOLS.keys())
    utc_today = datetime.utcnow().date()
    end_date = datetime.strptime(opts.to_date, "%Y-%m-%d").date() if opts.to_date else utc_today - timedelta(days=1)
    start_override = datetime.strptime(opts.from_date, "%Y-%m-%d").date() if opts.from_date else None

    for sym in symbols:
        if sym not in SYMBOLS:
            print(f"Unknown symbol {sym}, skipping.")
            continue
        ingest_symbol(sym, start_override, end_date)

if __name__ == "__main__":
    main()
