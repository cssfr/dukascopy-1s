#!/usr/bin/env python3

import argparse
import subprocess
from datetime import datetime, timedelta, date
from pathlib import Path
import yaml
import pandas as pd
import json
import numpy as np

OUTPUT_DIR = Path("ohlcv/1s")
DOWNLOAD_DIR = Path("download")
SYMBOLS_FILE = Path("symbols.yaml")

SYMBOLS = yaml.safe_load(SYMBOLS_FILE.read_text())

def convert_to_parquet(input_csv_path: Path, output_parquet_path: Path, symbol: str):
    df = pd.read_csv(str(input_csv_path))
    
    schema_mapping = {
        'open': np.float64,
        'high': np.float64, 
        'low': np.float64,
        'close': np.float64,
        'volume': np.float64
    }
    
    for col, dtype in schema_mapping.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', utc=True)
    df['unix_time'] = (df['timestamp'] - pd.Timestamp('1970-01-01', tz='UTC')).dt.total_seconds().astype(np.int64)
    
    df.insert(0, 'symbol', symbol)
    cols = df.columns.tolist()
    cols.insert(cols.index('timestamp') + 1, cols.pop(cols.index('unix_time')))
    df = df[cols]
    df.to_parquet(str(output_parquet_path), index=False)

def run_dukascopy(symbol_id: str, date_str: str):
    next_day_str = (datetime.fromisoformat(date_str) + timedelta(days=1)).strftime("%Y-%m-%d")
    cmd = [
        "npx", "dukascopy-node",
        "-i", symbol_id,
        "-from", date_str,
        "-to", next_day_str,
        "-t", "s1",
        "-f", "csv",
        '--date-format "YYYY-MM-DD HH:mm:ss"',
        "-v",
        "-fl",
    ]
    subprocess.run(" ".join(cmd), check=True, shell=True)

def list_parquet_dates_remote(symbol_key: str):
    proc = subprocess.run(
        ["mc", "ls", "--json", f"myminio/dukascopy-node/ohlcv/1s/symbol={symbol_key}/"],
        capture_output=True, text=True, check=True
    )
    dates = []
    for line in proc.stdout.splitlines():
        obj = json.loads(line)
        key = obj.get("key", "")
        if "date=" in key and key.endswith("/"):
            date_part = key.split("date=")[1].rstrip("/")
            try:
                dates.append(datetime.strptime(date_part, "%Y-%m-%d").date())
            except ValueError:
                pass
    return dates

def _download_one_day(symbol_key: str, dukas_id: str, current: date) -> bool:
    """Download and convert one day. Returns True if parquet was created."""
    date_str = current.strftime("%Y-%m-%d")
    next_day_str = (current + timedelta(days=1)).strftime("%Y-%m-%d")

    parquet_path = OUTPUT_DIR / f"symbol={symbol_key}" / f"date={date_str}" / f"{symbol_key}_{date_str}.parquet"

    if parquet_path.exists():
        print(f"[{symbol_key}] Already exists locally: {date_str}")
        return False

    try:
        run_dukascopy(dukas_id, date_str)
        csv_name = f"{dukas_id}-s1-bid-{date_str}-{next_day_str}.csv"
        csv_path = DOWNLOAD_DIR / csv_name
        if csv_path.exists():
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            convert_to_parquet(csv_path, parquet_path, symbol_key)
            csv_path.unlink(missing_ok=True)
            print(f"[{symbol_key}] ✔ {date_str}")
            return True
        else:
            print(f"[{symbol_key}] ⚠ No data for {date_str} (holiday/weekend/unavailable)")
            return False
    except Exception as e:
        print(f"[{symbol_key}] ❌ Error on {date_str}: {e}")
        return False

def ingest_date_range(symbol_key: str, start_date: date, end_date: date):
    """Fill an arbitrary date range regardless of what already exists in MinIO."""
    meta = SYMBOLS[symbol_key]
    dukas_id = meta["id"]
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    current = start_date
    while current <= end_date:
        _download_one_day(symbol_key, dukas_id, current)
        current += timedelta(days=1)

def ingest_symbol_backfill(symbol_key: str, earliest_required: date, earliest_available: date):
    meta = SYMBOLS[symbol_key]
    dukas_id = meta["id"]
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    current = earliest_required
    while current < earliest_available:
        _download_one_day(symbol_key, dukas_id, current)
        current += timedelta(days=1)

def main():
    parser = argparse.ArgumentParser(description="Backfill missing 1s OHLCV parquet files")
    parser.add_argument("--start-date", help="Start date YYYY-MM-DD (inclusive) for explicit range fill")
    parser.add_argument("--end-date", help="End date YYYY-MM-DD (inclusive) for explicit range fill")
    parser.add_argument("--symbols", help="Comma-separated symbols e.g. NQ,ES (default: all in symbols.yaml)")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",")] if args.symbols else list(SYMBOLS.keys())

    if args.start_date and args.end_date:
        # Explicit date range: fill a specific gap regardless of existing data
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        for symbol in symbols:
            if symbol not in SYMBOLS:
                print(f"Unknown symbol: {symbol}")
                continue
            print(f"[{symbol}] Filling date range {start} to {end}")
            ingest_date_range(symbol, start, end)
    else:
        # Original mode: backfill missing dates before earliest available in MinIO
        for symbol in symbols:
            if symbol not in SYMBOLS:
                print(f"Unknown symbol: {symbol}")
                continue
            earliest_required = datetime.strptime(SYMBOLS[symbol]["earliest_date"], "%Y-%m-%d").date()
            existing_dates = list_parquet_dates_remote(symbol)
            if not existing_dates:
                print(f"[{symbol}] No existing parquet; starting full backfill.")
                earliest_available = date.today()
                ingest_symbol_backfill(symbol, earliest_required, earliest_available)
                continue
            earliest_available = min(existing_dates)
            if earliest_required < earliest_available:
                print(f"[{symbol}] Backfilling {earliest_required} to {earliest_available - timedelta(days=1)}")
                ingest_symbol_backfill(symbol, earliest_required, earliest_available)
            else:
                print(f"[{symbol}] Already has full history; nothing to backfill.")

if __name__ == "__main__":
    main()
