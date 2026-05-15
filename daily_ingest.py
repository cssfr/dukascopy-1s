#!/usr/bin/env python3
"""Daily gap-filling Dukascopy ingest utility.

Reads existing parquet files in ./ohlcv/1s (synced from your MinIO bucket),
finds the newest *attempted* date per symbol, and downloads any missing days
up to yesterday UTC using dukascopy-node. Converts each day to Parquet with a
unix_time column and saves it back into the same folder hierarchy.

Each attempt writes a `_status.json` next to where the parquet lives, so
failures (transient or permanent) are visible and retryable. A startup
retry-sweep re-attempts recently-FAILED days before the forward sweep.

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

from ingest_status import (
    STATUS_INGESTED, STATUS_EMPTY, STATUS_FAILED,
    write_status, read_status, parquet_path, has_attempt,
)

# -----------------------------------------------------------------------------
#  Paths & config
# -----------------------------------------------------------------------------
OUTPUT_DIR   = Path("ohlcv/1s")
DOWNLOAD_DIR = Path("download")
SYMBOLS_FILE = Path("symbols.yaml")

# Per-day download timeout in seconds.  1s data requires downloading 24 hourly
# tick files from Dukascopy's CDN and aggregating — allow up to 5 minutes.
DUKASCOPY_TIMEOUT   = 300

# Retry recently-FAILED days at the start of each run.
RETRY_LOOKBACK_DAYS = 30
MAX_ATTEMPTS        = 5

if not SYMBOLS_FILE.exists():
    raise SystemExit("symbols.yaml not found. Commit it alongside this script.")

SYMBOLS = yaml.safe_load(SYMBOLS_FILE.read_text())


# -----------------------------------------------------------------------------
#  Converter with comprehensive schema definition
# -----------------------------------------------------------------------------
def convert_to_parquet(input_csv_path: Path, output_parquet_path: Path, symbol: str) -> int:
    df = pd.read_csv(str(input_csv_path))

    schema_mapping = {
        'open':   np.float64,
        'high':   np.float64,
        'low':    np.float64,
        'close':  np.float64,
        'volume': np.float64,
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
    return len(df)


# -----------------------------------------------------------------------------
#  Downloader
# -----------------------------------------------------------------------------
def run_dukascopy(symbol_id: str, date_str: str):
    """Run dukascopy-node for one day of 1s data.

    Raises subprocess.CalledProcessError on non-zero exit.
    Raises subprocess.TimeoutExpired if the download stalls past DUKASCOPY_TIMEOUT.
    """
    next_day = (datetime.fromisoformat(date_str) + timedelta(days=1)).strftime("%Y-%m-%d")
    cmd = (
        f"npx dukascopy-node -i {symbol_id} -from {date_str} -to {next_day} "
        f"-t s1 -f csv --date-format \"YYYY-MM-DD HH:mm:ss\" -v -fl"
    )
    print("Running:", cmd)
    subprocess.run(cmd, check=True, shell=True, timeout=DUKASCOPY_TIMEOUT)


# -----------------------------------------------------------------------------
#  Single-day attempt with status tracking
# -----------------------------------------------------------------------------
def attempt_day(symbol_key: str, dukas_id: str, day: date, *, attempt: int = 1) -> str:
    """Attempt to download and convert one day. Always writes a status JSON.

    Returns the resulting status string (INGESTED | EMPTY | FAILED).
    """
    date_str     = day.strftime("%Y-%m-%d")
    next_day_str = (day + timedelta(days=1)).strftime("%Y-%m-%d")
    p_path       = parquet_path(OUTPUT_DIR, symbol_key, date_str)

    # Legacy parquet without status JSON — record it and skip re-attempt.
    if p_path.exists() and read_status(OUTPUT_DIR, symbol_key, date_str) is None:
        write_status(OUTPUT_DIR, symbol_key, date_str, STATUS_INGESTED,
                     row_count=-1, attempt=attempt)
        return STATUS_INGESTED

    start = time.monotonic()
    try:
        run_dukascopy(dukas_id, date_str)
        time.sleep(1)  # polite

        csv_name    = f"{dukas_id}-s1-bid-{date_str}-{next_day_str}.csv"
        csv_path    = DOWNLOAD_DIR / csv_name
        duration_ms = int((time.monotonic() - start) * 1000)

        if not csv_path.exists() or csv_path.stat().st_size == 0:
            print(f"[{symbol_key}] {date_str}: dukascopy returned no data (closure/weekend/holiday).")
            csv_path.unlink(missing_ok=True)
            write_status(OUTPUT_DIR, symbol_key, date_str, STATUS_EMPTY,
                         duration_ms=duration_ms, attempt=attempt)
            return STATUS_EMPTY

        p_path.parent.mkdir(parents=True, exist_ok=True)
        rows = convert_to_parquet(csv_path, p_path, symbol_key)
        csv_path.unlink(missing_ok=True)

        write_status(OUTPUT_DIR, symbol_key, date_str, STATUS_INGESTED,
                     row_count=rows, duration_ms=duration_ms, attempt=attempt)
        print(f"[{symbol_key}] saved {p_path.relative_to(OUTPUT_DIR)} ({rows:,} rows)")
        return STATUS_INGESTED

    except subprocess.TimeoutExpired:
        duration_ms = int((time.monotonic() - start) * 1000)
        print(f"[{symbol_key}] {date_str}: dukascopy-node timed out after {DUKASCOPY_TIMEOUT}s.")
        write_status(OUTPUT_DIR, symbol_key, date_str, STATUS_FAILED,
                     error="TimeoutExpired", duration_ms=duration_ms, attempt=attempt)
        return STATUS_FAILED

    except subprocess.CalledProcessError as e:
        duration_ms = int((time.monotonic() - start) * 1000)
        print(f"[{symbol_key}] {date_str}: dukascopy-node exited with error: {e}")
        write_status(OUTPUT_DIR, symbol_key, date_str, STATUS_FAILED,
                     error=f"CalledProcessError(returncode={e.returncode})",
                     duration_ms=duration_ms, attempt=attempt)
        return STATUS_FAILED


# -----------------------------------------------------------------------------
#  Helpers — find latest attempted day (replaces newest_parquet_date)
# -----------------------------------------------------------------------------
def latest_attempted_date(symbol_key: str) -> date | None:
    """Max date for which a status JSON or a parquet file exists.

    Includes EMPTY days so we don't waste API calls re-attempting confirmed
    weekends and holidays forever. Falls back to parquet presence for legacy
    data that predates the status JSON convention.
    """
    folder = OUTPUT_DIR / f"symbol={symbol_key}"
    if not folder.exists():
        return None
    dates: list[date] = []
    for date_dir in folder.glob("date=*"):
        if not date_dir.is_dir():
            continue
        try:
            date_str = date_dir.name.split("=")[1]
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if has_attempt(OUTPUT_DIR, symbol_key, date_str):
            dates.append(d)
    return max(dates) if dates else None


# -----------------------------------------------------------------------------
#  Retry sweep — re-attempt recent FAILED days
# -----------------------------------------------------------------------------
def retry_failed_days(symbol_key: str, dukas_id: str,
                      lookback_days: int = RETRY_LOOKBACK_DAYS,
                      max_attempts: int = MAX_ATTEMPTS) -> None:
    folder = OUTPUT_DIR / f"symbol={symbol_key}"
    if not folder.exists():
        return
    cutoff = datetime.utcnow().date() - timedelta(days=lookback_days)

    failed: list[tuple[date, int]] = []
    for date_dir in folder.glob("date=*"):
        if not date_dir.is_dir():
            continue
        try:
            date_str = date_dir.name.split("=")[1]
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < cutoff:
            continue
        status = read_status(OUTPUT_DIR, symbol_key, date_str)
        if status and status.get("status") == STATUS_FAILED and status.get("attempt", 0) < max_attempts:
            failed.append((d, int(status.get("attempt", 0))))

    if not failed:
        return

    print(f"[{symbol_key}] retrying {len(failed)} FAILED days from last {lookback_days} days")
    for d, prev_attempts in sorted(failed):
        attempt_day(symbol_key, dukas_id, d, attempt=prev_attempts + 1)


# -----------------------------------------------------------------------------
def daterange(start: date, end: date):
    while start <= end:
        yield start
        start += timedelta(days=1)


def ingest_symbol(symbol_key: str, start_override: date | None, end_date: date):
    meta = SYMBOLS[symbol_key]
    dukas_id = meta['id']
    earliest_date = datetime.strptime(meta['earliest_date'], "%Y-%m-%d").date()

    DOWNLOAD_DIR.mkdir(exist_ok=True, parents=True)

    # 1) Retry recently-FAILED days first. Runs regardless of start_override —
    # the retry sweep is independent of the forward sweep and the lookback
    # window keeps it bounded.
    retry_failed_days(symbol_key, dukas_id)

    # 2) Determine the forward-sweep start.
    if start_override:
        start_date = start_override
    else:
        latest = latest_attempted_date(symbol_key)
        start_date = (latest + timedelta(days=1)) if latest else earliest_date

    start_date = max(start_date, earliest_date)
    if start_date > end_date:
        print(f"[{symbol_key}] up‑to‑date.")
        return

    # 3) Forward sweep — attempt each day, recording status.
    for day in daterange(start_date, end_date):
        date_str = day.strftime("%Y-%m-%d")
        existing = read_status(OUTPUT_DIR, symbol_key, date_str)
        if existing and existing.get("status") in (STATUS_INGESTED, STATUS_EMPTY):
            continue  # already settled
        attempt = (int(existing.get("attempt", 0)) + 1) if existing else 1
        attempt_day(symbol_key, dukas_id, day, attempt=attempt)


# -----------------------------------------------------------------------------
#  CLI
# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Gap-fill Dukascopy data to Parquet.")
    ap.add_argument("--symbols", help="Comma-separated list (default ALL)")
    ap.add_argument("--from", dest="from_date", help="Start YYYY-MM-DD (manual)")
    ap.add_argument("--to",   dest="to_date",   help="End YYYY-MM-DD (manual)")
    opts = ap.parse_args()

    symbols   = [s.strip() for s in opts.symbols.split(",")] if opts.symbols else list(SYMBOLS.keys())
    utc_today = datetime.utcnow().date()
    end_date  = datetime.strptime(opts.to_date,   "%Y-%m-%d").date() if opts.to_date   else utc_today - timedelta(days=1)
    start_override = datetime.strptime(opts.from_date, "%Y-%m-%d").date() if opts.from_date else None

    for sym in symbols:
        if sym not in SYMBOLS:
            print(f"Unknown symbol {sym}, skipping.")
            continue
        ingest_symbol(sym, start_override, end_date)


if __name__ == "__main__":
    main()
