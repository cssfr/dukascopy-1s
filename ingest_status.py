"""Per-day ingest status tracking.

Each (symbol, date) attempted by daily_ingest.py or backfill_missing.py
writes a `_status.json` next to where the parquet lives. This makes failures
(transient or permanent) visible and retryable, and lets the daily sweep
advance past confirmed-empty days without re-attempting them forever.

Status values:
    INGESTED — parquet successfully written
    EMPTY    — dukascopy returned 0 bytes (confirmed market closure)
    FAILED   — timeout or non-zero exit (transient failure; retry candidate)
"""

import json
from datetime import datetime, timezone
from pathlib import Path

STATUS_INGESTED = "INGESTED"
STATUS_EMPTY    = "EMPTY"
STATUS_FAILED   = "FAILED"

STATUS_FILE = "_status.json"


def day_dir(output_dir: Path, symbol_key: str, date_str: str) -> Path:
    return output_dir / f"symbol={symbol_key}" / f"date={date_str}"


def status_path(output_dir: Path, symbol_key: str, date_str: str) -> Path:
    return day_dir(output_dir, symbol_key, date_str) / STATUS_FILE


def write_status(output_dir: Path, symbol_key: str, date_str: str, status: str, *,
                 row_count: int = 0, error: str | None = None,
                 duration_ms: int | None = None, attempt: int = 1) -> None:
    path = status_path(output_dir, symbol_key, date_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol":       symbol_key,
        "date":         date_str,
        "status":       status,
        "row_count":    row_count,
        "attempt":      attempt,
        "attempted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_ms":  duration_ms,
        "error":        error,
    }
    path.write_text(json.dumps(payload, indent=2))


def read_status(output_dir: Path, symbol_key: str, date_str: str) -> dict | None:
    path = status_path(output_dir, symbol_key, date_str)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def parquet_path(output_dir: Path, symbol_key: str, date_str: str) -> Path:
    return day_dir(output_dir, symbol_key, date_str) / f"{symbol_key}_{date_str}.parquet"


def has_attempt(output_dir: Path, symbol_key: str, date_str: str) -> bool:
    """True if either the status JSON or the parquet exists (legacy data without status)."""
    d = day_dir(output_dir, symbol_key, date_str)
    return (d / STATUS_FILE).exists() or (d / f"{symbol_key}_{date_str}.parquet").exists()
