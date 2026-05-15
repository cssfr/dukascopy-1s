#!/usr/bin/env python3
"""Bulk year backfill for 1s OHLCV data.

Alternative to backfill_missing.py for large historical loads. Invokes
dukascopy-node once per MONTH (12 calls/year) instead of once per DAY
(~250 calls/year), amortizing the ~3-5s per-invocation overhead and
turning ~9 hours of per-day downloads into ~1 hour of per-month
downloads.

Output is identical in shape to backfill_missing.py: per-day parquets
at ohlcv/1s/symbol=X/date=YYYY-MM-DD/X_YYYY-MM-DD.parquet, plus
per-day _status.json. Downstream (historical_consolidation_script.py,
Dagster, etc.) is unchanged.

When to use this vs. backfill_missing.py:
  - This script: >30 days of historical data, full-year extensions,
    new symbol onboarding.
  - backfill_missing.py: small recent gaps, per-day retries after
    transient failures.

Usage:
  python bulk_year_backfill.py --symbols NQ --from-year 2018 --to-year 2020
  python bulk_year_backfill.py --symbols NQ,ES --from-year 2018 --to-year 2020 --force
"""

import argparse
import calendar
import subprocess
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import yaml

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

# Per-month dukascopy-node timeout. Aggressive: a month of NQ 1s downloads
# in ~5-8 minutes locally; allow 30 min in case CI network is slow.
MONTH_TIMEOUT_SEC = 1800

if not SYMBOLS_FILE.exists():
    raise SystemExit("symbols.yaml not found.")
SYMBOLS = yaml.safe_load(SYMBOLS_FILE.read_text())


# -----------------------------------------------------------------------------
#  Date helpers
# -----------------------------------------------------------------------------
def days_in_month(year: int, month: int) -> list[date]:
    _, last_day = calendar.monthrange(year, month)
    return [date(year, month, d) for d in range(1, last_day + 1)]


def month_bounds(year: int, month: int) -> tuple[str, str]:
    """[from, to) bounds for dukascopy-node, formatted YYYY-MM-DD."""
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def month_is_settled(symbol_key: str, year: int, month: int) -> bool:
    """True iff every day in the month already has INGESTED or EMPTY status
    (or a legacy parquet without a status JSON)."""
    for day in days_in_month(year, month):
        date_str = day.strftime("%Y-%m-%d")
        status = read_status(OUTPUT_DIR, symbol_key, date_str)
        if status and status.get("status") in (STATUS_INGESTED, STATUS_EMPTY):
            continue
        # Legacy parquet (predates status JSON convention) counts as INGESTED.
        if parquet_path(OUTPUT_DIR, symbol_key, date_str).exists():
            continue
        return False
    return True


def max_attempt_in_month(symbol_key: str, year: int, month: int) -> int:
    """Max `attempt` value across the month's status JSONs (0 if none)."""
    best = 0
    for day in days_in_month(year, month):
        date_str = day.strftime("%Y-%m-%d")
        status = read_status(OUTPUT_DIR, symbol_key, date_str) or {}
        best = max(best, int(status.get("attempt", 0)))
    return best


# -----------------------------------------------------------------------------
#  Dukascopy month download
# -----------------------------------------------------------------------------
def run_dukascopy_month(symbol_id: str, year: int, month: int) -> Path:
    """Download one full month of 1s data as a single CSV. Returns the CSV path."""
    from_str, to_str = month_bounds(year, month)
    cmd = (
        f"npx dukascopy-node -i {symbol_id} -from {from_str} -to {to_str} "
        f'-t s1 -f csv --date-format "YYYY-MM-DD HH:mm:ss" -v -fl'
    )
    print(f"Running: {cmd}")
    subprocess.run(cmd, check=True, shell=True, timeout=MONTH_TIMEOUT_SEC)
    csv_name = f"{symbol_id}-s1-bid-{from_str}-{to_str}.csv"
    return DOWNLOAD_DIR / csv_name


# -----------------------------------------------------------------------------
#  CSV → per-day parquet split (polars)
# -----------------------------------------------------------------------------
SCHEMA_OVERRIDES = {
    "open":   pl.Float64,
    "high":   pl.Float64,
    "low":    pl.Float64,
    "close":  pl.Float64,
    "volume": pl.Float64,
}

OUTPUT_COLS = ["symbol", "timestamp", "unix_time", "open", "high", "low", "close", "volume"]


def split_month_csv_to_dailies(
    csv_path: Path,
    symbol_key: str,
    year: int,
    month: int,
    attempt: int,
) -> None:
    """Read the month CSV with polars, partition by date, write per-day parquets
    and status JSONs (INGESTED for dates with data, EMPTY for in-range dates
    that had none)."""
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        # dukascopy returned nothing for the whole month — likely a market-closed
        # period; mark every day EMPTY.
        print(f"[{symbol_key}] {year}-{month:02d}: no data returned for any day in month")
        for day in days_in_month(year, month):
            write_status(OUTPUT_DIR, symbol_key, day.strftime("%Y-%m-%d"),
                         STATUS_EMPTY, attempt=attempt)
        return

    size_mib = csv_path.stat().st_size / (1024 * 1024)
    print(f"[{symbol_key}] {year}-{month:02d}: reading CSV ({size_mib:.1f} MiB)")

    df = (
        pl.read_csv(csv_path, schema_overrides=SCHEMA_OVERRIDES)
          .with_columns(
              pl.col("timestamp").str.strptime(pl.Datetime("ns", "UTC"),
                                                format="%Y-%m-%d %H:%M:%S"),
          )
          .with_columns([
              pl.col("timestamp").dt.epoch(time_unit="s").alias("unix_time"),
              pl.lit(symbol_key).alias("symbol"),
              pl.col("timestamp").dt.date().alias("_date"),
          ])
    )

    dates_with_data: set[date] = set()
    for sub in df.partition_by("_date", maintain_order=False):
        d = sub["_date"][0]
        date_str = d.strftime("%Y-%m-%d")
        dates_with_data.add(d)

        out = parquet_path(OUTPUT_DIR, symbol_key, date_str)
        out.parent.mkdir(parents=True, exist_ok=True)
        sub.select(OUTPUT_COLS).write_parquet(str(out))

        write_status(OUTPUT_DIR, symbol_key, date_str, STATUS_INGESTED,
                     row_count=len(sub), attempt=attempt)
        print(f"[{symbol_key}] ✓ {date_str} ({len(sub):,} rows)")

    # Dates in the month that the CSV had no rows for → confirmed EMPTY by dukascopy.
    for day in days_in_month(year, month):
        if day not in dates_with_data:
            date_str = day.strftime("%Y-%m-%d")
            write_status(OUTPUT_DIR, symbol_key, date_str, STATUS_EMPTY,
                         attempt=attempt)


# -----------------------------------------------------------------------------
#  Per-month processing with status tracking on failure
# -----------------------------------------------------------------------------
def process_month(symbol_key: str, year: int, month: int, force: bool) -> str:
    """Process one (symbol, year, month). Returns 'settled', 'done', or 'failed'."""
    if not force and month_is_settled(symbol_key, year, month):
        print(f"[{symbol_key}] {year}-{month:02d}: already settled, skipping (use --force to re-run)")
        return "settled"

    attempt = max_attempt_in_month(symbol_key, year, month) + 1
    meta = SYMBOLS[symbol_key]
    symbol_id = meta["id"]

    start = time.monotonic()
    try:
        csv_path = run_dukascopy_month(symbol_id, year, month)
        try:
            split_month_csv_to_dailies(csv_path, symbol_key, year, month, attempt)
        finally:
            csv_path.unlink(missing_ok=True)
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - start
        print(f"[{symbol_key}] {year}-{month:02d}: dukascopy-node TIMEOUT after {elapsed:.0f}s")
        _mark_month_failed(symbol_key, year, month, attempt,
                           f"TimeoutExpired after {MONTH_TIMEOUT_SEC}s (bulk month)")
        return "failed"
    except subprocess.CalledProcessError as e:
        print(f"[{symbol_key}] {year}-{month:02d}: dukascopy-node EXIT {e.returncode}")
        _mark_month_failed(symbol_key, year, month, attempt,
                           f"CalledProcessError(returncode={e.returncode}) (bulk month)")
        return "failed"
    except Exception as e:
        print(f"[{symbol_key}] {year}-{month:02d}: unexpected error: {e!r}")
        _mark_month_failed(symbol_key, year, month, attempt,
                           f"Unexpected {type(e).__name__}: {str(e)[:200]} (bulk month)")
        return "failed"

    elapsed = time.monotonic() - start
    print(f"[{symbol_key}] {year}-{month:02d}: completed in {elapsed:.0f}s")
    return "done"


def _mark_month_failed(symbol_key: str, year: int, month: int, attempt: int, error: str):
    """Write FAILED status for every day of the month so the retry sweep / user
    can see what happened."""
    for day in days_in_month(year, month):
        date_str = day.strftime("%Y-%m-%d")
        write_status(OUTPUT_DIR, symbol_key, date_str, STATUS_FAILED,
                     error=error, attempt=attempt)


# -----------------------------------------------------------------------------
#  Inline yearly consolidation — scoped to one (symbol, year)
# -----------------------------------------------------------------------------
YEARLY_BASE = Path("ohlcv/1Ys")

YEARLY_SCHEMA_CASTS = [
    pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
    pl.col("open").cast(pl.Float64),
    pl.col("high").cast(pl.Float64),
    pl.col("low").cast(pl.Float64),
    pl.col("close").cast(pl.Float64),
    pl.col("volume").cast(pl.Float64),
]


def _list_local_dailies_for_year(symbol_key: str, year: int) -> list[Path]:
    folder = OUTPUT_DIR / f"symbol={symbol_key}"
    if not folder.exists():
        return []
    out = []
    for date_dir in folder.glob(f"date={year}-*"):
        if not date_dir.is_dir():
            continue
        try:
            date_str = date_dir.name.split("=")[1]
            datetime.strptime(date_str, "%Y-%m-%d")  # validate
        except (ValueError, IndexError):
            continue
        parquet = date_dir / f"{symbol_key}_{date_str}.parquet"
        if parquet.exists():
            out.append(parquet)
    return sorted(out)


def consolidate_year(symbol_key: str, year: int) -> str:
    """Merge new local dailies for (symbol, year) into the yearly parquet.

    1. Find local dailies for the year
    2. Download the SPECIFIC existing yearly (one file, ~250 MiB) from MinIO if any
    3. Determine which dailies aren't already in the yearly
    4. If new ones exist, merge and write back; safety-check; upload that one file

    No mc mirror, no full-history scan. Only this year's yearly is touched.

    Returns: 'updated' | 'unchanged' | 'no-dailies' | 'failed'.
    """
    dst_dir = YEARLY_BASE / f"symbol={symbol_key}" / f"year={year}"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst_file = dst_dir / f"{symbol_key}_{year}.parquet"
    remote_path = f"myminio/dukascopy-node/ohlcv/1Ys/symbol={symbol_key}/year={year}/{symbol_key}_{year}.parquet"

    daily_files = _list_local_dailies_for_year(symbol_key, year)
    if not daily_files:
        print(f"[{symbol_key}] {year}: consolidation skipped (no local dailies)")
        return "no-dailies"

    # Download the existing yearly (one specific file).
    print(f"[{symbol_key}] {year}: fetching existing yearly from MinIO (if any)")
    fetch = subprocess.run(
        ["mc", "cp", remote_path, str(dst_file)],
        capture_output=True, text=True,
    )
    has_existing = dst_file.exists() and dst_file.stat().st_size > 0
    if fetch.returncode != 0 and "Object does not exist" not in (fetch.stderr or ""):
        print(f"[{symbol_key}] {year}: WARN mc cp existing yearly failed: {fetch.stderr.strip()}")
        # If the download outright failed (network) and a yearly truly exists remotely,
        # writing a new one could destroy data. Abort this year's consolidation.
        return "failed"
    if not has_existing:
        print(f"[{symbol_key}] {year}: no existing yearly — will create new")

    # Determine existing dates so we can:
    #   (a) filter daily files we still need to merge
    #   (b) run the safety check against losing any
    existing_dates: set = set()
    if has_existing:
        try:
            df = (
                pl.scan_parquet(dst_file)
                  .select(pl.col("timestamp").cast(pl.Datetime("us", "UTC")).dt.date().alias("d"))
                  .unique()
                  .collect()
            )
            existing_dates = set(df["d"].to_list())
        except Exception as e:
            print(f"[{symbol_key}] {year}: WARN couldn't read existing yearly dates: {e}")
            existing_dates = set()

    # Filter dailies that aren't already in the yearly.
    new_dailies = []
    for f in daily_files:
        date_str = f.parent.name.split("=")[1]
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        if d not in existing_dates:
            new_dailies.append(f)

    if not new_dailies:
        print(f"[{symbol_key}] {year}: yearly already covers all {len(daily_files)} local dailies — nothing to update")
        # Tidy up: remove the locally-downloaded yearly to avoid disk bloat across years.
        try:
            dst_file.unlink()
        except OSError:
            pass
        return "unchanged"

    print(f"[{symbol_key}] {year}: merging {len(new_dailies)} new dailies "
          f"into yearly with {len(existing_dates)} existing day(s)")

    new_data = pl.concat([
        pl.scan_parquet(str(f)).with_columns(YEARLY_SCHEMA_CASTS) for f in new_dailies
    ])
    if has_existing:
        existing_lazy = pl.scan_parquet(dst_file).with_columns(YEARLY_SCHEMA_CASTS)
        combined = pl.concat([existing_lazy, new_data])
    else:
        combined = new_data

    final = combined.sort("timestamp").unique().collect()

    # Safety check — refuse to write if any existing date is missing from result.
    if existing_dates:
        final_dates = set(
            final.select(
                pl.col("timestamp").cast(pl.Datetime("us", "UTC")).dt.date().alias("d")
            )["d"].to_list()
        )
        lost = existing_dates - final_dates
        if lost:
            raise RuntimeError(
                f"[{symbol_key}] {year}: REFUSING to write yearly — would drop "
                f"{len(lost)} existing date(s) (e.g. {sorted(lost)[:5]})."
            )

    final.write_parquet(dst_file)
    print(f"[{symbol_key}] {year}: wrote yearly with {len(final):,} records, uploading...")

    upload = subprocess.run(
        ["mc", "cp", "--preserve", str(dst_file), remote_path],
        capture_output=True, text=True,
    )
    if upload.returncode != 0:
        print(f"[{symbol_key}] {year}: UPLOAD FAILED: {upload.stderr.strip()}")
        return "failed"
    print(f"[{symbol_key}] {year}: yearly uploaded ({dst_file.stat().st_size / (1024*1024):.0f} MiB)")
    return "updated"


# -----------------------------------------------------------------------------
#  Year-level orchestration
# -----------------------------------------------------------------------------
def process_year(symbol_key: str, year: int, force: bool) -> dict:
    """Process all 12 months of `year` for `symbol_key`. Then inline-consolidate
    the (symbol, year)'s yearly. Returns counters."""
    print(f"\n=== {symbol_key} {year} ===")
    counters = {"settled": 0, "done": 0, "failed": 0, "consolidation": "skipped"}
    for month in range(1, 13):
        result = process_month(symbol_key, year, month, force)
        counters[result] += 1

    # Only attempt consolidation if at least one month produced new data this run,
    # OR if force was used (where the user explicitly wants a re-merge regardless).
    if counters["done"] > 0 or force:
        try:
            counters["consolidation"] = consolidate_year(symbol_key, year)
        except Exception as e:
            print(f"[{symbol_key}] {year}: consolidation FAILED: {e!r}")
            counters["consolidation"] = "failed"
    else:
        counters["consolidation"] = "skipped-no-changes"
    return counters


# -----------------------------------------------------------------------------
#  CLI
# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Bulk year backfill for 1s OHLCV data (per-month dukascopy calls)"
    )
    ap.add_argument("--symbols", help="Comma-separated symbols (default: all in symbols.yaml)")
    ap.add_argument("--from-year", type=int, required=True,
                    help="Start year (inclusive)")
    ap.add_argument("--to-year", type=int, required=True,
                    help="End year (inclusive)")
    ap.add_argument("--force", action="store_true",
                    help="Re-download months that are already fully settled")
    opts = ap.parse_args()

    if opts.from_year > opts.to_year:
        raise SystemExit("--from-year must be <= --to-year")

    if opts.symbols:
        symbols = [s.strip() for s in opts.symbols.split(",")]
    else:
        symbols = list(SYMBOLS.keys())

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    summary: dict = {}
    for symbol_key in symbols:
        if symbol_key not in SYMBOLS:
            print(f"Unknown symbol: {symbol_key}, skipping")
            continue
        summary[symbol_key] = {}
        for year in range(opts.from_year, opts.to_year + 1):
            summary[symbol_key][year] = process_year(symbol_key, year, opts.force)

    # Final summary
    print("\n" + "=" * 60)
    print("BULK BACKFILL SUMMARY")
    print("=" * 60)
    any_failed = False
    for sym, years in summary.items():
        for yr, counts in years.items():
            print(f"  {sym} {yr}: settled={counts['settled']} done={counts['done']} "
                  f"failed={counts['failed']} consolidation={counts['consolidation']}")
            if counts["failed"] or counts["consolidation"] == "failed":
                any_failed = True
    print("=" * 60)
    if any_failed:
        raise SystemExit("Some months or consolidations failed — see logs above and re-run "
                         "with the same args, or retry specific dates with backfill_missing.py")


if __name__ == "__main__":
    main()
