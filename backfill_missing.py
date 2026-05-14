#!/usr/bin/env python3

import argparse
import subprocess
import time
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

MAX_RETRIES = 3
RETRY_DELAY = 30  # seconds between attempts


class NoDataError(Exception):
    """Empty/header-only CSV — weekend, holiday, or genuinely unavailable day."""


class DownloadError(Exception):
    """Dukascopy fetch failed after all retries — likely a real trading day."""


def convert_to_parquet(input_csv_path: Path, output_parquet_path: Path, symbol: str):
    try:
        df = pd.read_csv(str(input_csv_path))
    except (pd.errors.EmptyDataError, ValueError) as e:
        raise NoDataError(str(e))

    if df.empty:
        raise NoDataError("No data rows in CSV")

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


def _exists_in_minio(symbol_key: str, date_str: str) -> bool:
    result = subprocess.run(
        ["mc", "ls", f"myminio/dukascopy-node/ohlcv/1s/symbol={symbol_key}/date={date_str}/"],
        capture_output=True, text=True
    )
    return result.returncode == 0 and bool(result.stdout.strip())


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


def _download_one_day(symbol_key: str, dukas_id: str, current: date):
    """Download and convert one day with retries.

    Returns True if parquet was created.
    Returns False if the day has no data (weekend/holiday) — not a failure.
    Raises DownloadError if all retries exhausted on what looks like a real trading day.
    """
    date_str = current.strftime("%Y-%m-%d")
    next_day_str = (current + timedelta(days=1)).strftime("%Y-%m-%d")

    parquet_path = OUTPUT_DIR / f"symbol={symbol_key}" / f"date={date_str}" / f"{symbol_key}_{date_str}.parquet"

    if parquet_path.exists():
        print(f"[{symbol_key}] Already exists locally: {date_str}")
        return True

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            run_dukascopy(dukas_id, date_str)
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                print(f"[{symbol_key}] ✗ {date_str} attempt {attempt}/{MAX_RETRIES}: {e} — retrying in {RETRY_DELAY}s")
                time.sleep(RETRY_DELAY)
                continue
            else:
                print(f"[{symbol_key}] ✗ {date_str} — all {MAX_RETRIES} attempts failed: {e}")
                raise DownloadError(str(e)) from e

        csv_name = f"{dukas_id}-s1-bid-{date_str}-{next_day_str}.csv"
        csv_path = DOWNLOAD_DIR / csv_name

        if not csv_path.exists() or csv_path.stat().st_size == 0:
            csv_path.unlink(missing_ok=True)
            print(f"[{symbol_key}] ⚠ {date_str} — no data (holiday/weekend/unavailable)")
            return False

        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            convert_to_parquet(csv_path, parquet_path, symbol_key)
        except NoDataError:
            csv_path.unlink(missing_ok=True)
            print(f"[{symbol_key}] ⚠ {date_str} — no data rows (holiday/weekend/unavailable)")
            return False

        csv_path.unlink(missing_ok=True)
        print(f"[{symbol_key}] ✔ {date_str}")
        return True


def _group_into_ranges(dates: list) -> list:
    if not dates:
        return []
    ranges = []
    range_start = dates[0]
    prev = dates[0]
    for d in dates[1:]:
        if (datetime.strptime(d, "%Y-%m-%d") - datetime.strptime(prev, "%Y-%m-%d")).days == 1:
            prev = d
        else:
            ranges.append((range_start, prev))
            range_start = d
            prev = d
    ranges.append((range_start, prev))
    return ranges


def ingest_date_range(symbol_key: str, start_date: date, end_date: date) -> list:
    """Fill an arbitrary date range, skipping dates already in MinIO.
    Returns list of dates that failed after all retries (real trading days, not weekends)."""
    meta = SYMBOLS[symbol_key]
    dukas_id = meta["id"]
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    failed = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        if _exists_in_minio(symbol_key, date_str):
            print(f"[{symbol_key}] Already in MinIO: {date_str}")
            current += timedelta(days=1)
            continue

        try:
            _download_one_day(symbol_key, dukas_id, current)
        except DownloadError:
            failed.append(date_str)

        current += timedelta(days=1)
    return failed


def ingest_symbol_backfill(symbol_key: str, earliest_required: date, earliest_available: date) -> list:
    meta = SYMBOLS[symbol_key]
    dukas_id = meta["id"]
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    failed = []
    current = earliest_required
    while current < earliest_available:
        date_str = current.strftime("%Y-%m-%d")
        try:
            _download_one_day(symbol_key, dukas_id, current)
        except DownloadError:
            failed.append(date_str)
        current += timedelta(days=1)
    return failed


def main():
    parser = argparse.ArgumentParser(description="Backfill missing 1s OHLCV parquet files")
    parser.add_argument("--start-date", help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--symbols", help="Comma-separated symbols e.g. NQ,ES (default: all in symbols.yaml)")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",")] if args.symbols else list(SYMBOLS.keys())

    all_failed: dict = {}

    if args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        for symbol in symbols:
            if symbol not in SYMBOLS:
                print(f"Unknown symbol: {symbol}")
                continue
            print(f"[{symbol}] Scanning and filling {start} to {end} (skipping dates already in MinIO)")
            failed = ingest_date_range(symbol, start, end)
            if failed:
                all_failed[symbol] = failed
    else:
        for symbol in symbols:
            if symbol not in SYMBOLS:
                print(f"Unknown symbol: {symbol}")
                continue
            earliest_required = datetime.strptime(SYMBOLS[symbol]["earliest_date"], "%Y-%m-%d").date()
            existing_dates = list_parquet_dates_remote(symbol)
            if not existing_dates:
                print(f"[{symbol}] No existing parquet; starting full backfill.")
                failed = ingest_symbol_backfill(symbol, earliest_required, date.today())
            else:
                earliest_available = min(existing_dates)
                if earliest_required < earliest_available:
                    print(f"[{symbol}] Backfilling {earliest_required} to {earliest_available - timedelta(days=1)}")
                    failed = ingest_symbol_backfill(symbol, earliest_required, earliest_available)
                else:
                    print(f"[{symbol}] Already has full history; nothing to backfill.")
                    failed = []
            if failed:
                all_failed[symbol] = failed

    if all_failed:
        print("\n" + "=" * 60)
        print("FAILED DATES — re-run with these commands:")
        for symbol, dates in all_failed.items():
            for r_start, r_end in _group_into_ranges(dates):
                print(
                    f"  gh workflow run dukascopy-backfill.yml "
                    f"--repo cssfr/dukascopy-1s --ref main "
                    f"-f start_date={r_start} -f end_date={r_end} -f symbols={symbol}"
                )
        print("=" * 60)
    else:
        print("\n✔ All dates processed successfully.")


if __name__ == "__main__":
    main()
