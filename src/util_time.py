from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

# Excel's day 0 (Windows) is 1899-12-30, including the 1900 leap year bug.
EXCEL_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)


@dataclass(frozen=True)
class RunTimestamps:
    iso_stamp: str          # e.g. "20250912T143012Z" used in filenames
    unix: float             # seconds since epoch (UTC)
    iso_utc: str            # "YYYY-MM-DDTHH:MM:SSZ"
    iso_local: str          # "YYYY-MM-DDTHH:MM:SS±HH:MM"
    excel_serial: float     # Excel serial date (float)


def _to_excel_serial(dt_utc: datetime) -> float:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    delta: timedelta = dt_utc - EXCEL_EPOCH
    return delta.total_seconds() / 86400.0


def make_run_timestamps() -> RunTimestamps:
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone()

    unix = now_utc.timestamp()
    iso_utc = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    iso_local = now_local.replace(microsecond=0).isoformat()
    excel_serial = _to_excel_serial(now_utc)
    iso_stamp = now_utc.strftime("%Y%m%dT%H%M%SZ")

    return RunTimestamps(
        iso_stamp=iso_stamp,
        unix=unix,
        iso_utc=iso_utc,
        iso_local=iso_local,
        excel_serial=excel_serial,
    )
