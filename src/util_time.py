"""Utility functions for timestamp generation and formatting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

# Excel's day 0 (Windows) is 1899-12-30, including the 1900 leap year bug.
EXCEL_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)

@dataclass(frozen=True)
class RunTimestamps:
    """Container for various timestamp formats used throughout the application.

    Attributes:
        iso_stamp: Filename-safe timestamp (e.g., "20250912T143012Z")
        unix: Unix timestamp in seconds since epoch (UTC)
        iso_utc: ISO 8601 UTC timestamp (e.g., "YYYY-MM-DDTHH:MM:SSZ")
        iso_local: ISO 8601 local timestamp with timezone (e.g., "YYYY-MM-DDTHH:MM:SS±HH:MM")
        excel_serial: Excel serial date number (float)
    """
    iso_stamp: str          # e.g. "20250912T143012Z" used in filenames
    unix: float             # seconds since epoch (UTC)
    iso_utc: str            # "YYYY-MM-DDTHH:MM:SSZ"
    iso_local: str          # "YYYY-MM-DDTHH:MM:SS±HH:MM"
    excel_serial: float     # Excel serial date (float)

def _to_excel_serial(datetime_utc: datetime) -> float:
    """Convert UTC datetime to Excel serial date number.

    Excel's date system starts at 1899-12-30 (with the 1900 leap year bug).
    This function calculates the number of days since that epoch.

    Args:
        datetime_utc: UTC datetime object to convert

    Returns:
        Excel serial date as float (days since Excel epoch)
    """
    if datetime_utc.tzinfo is None:
        datetime_utc = datetime_utc.replace(tzinfo=timezone.utc)

    time_delta: timedelta = datetime_utc - EXCEL_EPOCH
    return time_delta.total_seconds() / 86400.0

def make_run_timestamps() -> RunTimestamps:
    """Generate timestamps for current time in various formats.

    Creates a RunTimestamps object containing multiple timestamp representations
    of the current time, useful for file naming, metadata, and Excel compatibility.

    Returns:
        RunTimestamps object with current time in multiple formats
    """
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone()

    unix_timestamp = now_utc.timestamp()
    iso_utc = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    iso_local = now_local.replace(microsecond=0).isoformat()
    excel_serial = _to_excel_serial(now_utc)
    iso_stamp = now_utc.strftime("%Y%m%dT%H%M%SZ")

    return RunTimestamps(
        iso_stamp=iso_stamp,
        unix=unix_timestamp,
        iso_utc=iso_utc,
        iso_local=iso_local,
        excel_serial=excel_serial,
    )
