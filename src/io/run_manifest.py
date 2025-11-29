# src/io/run_manifest.py
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from src.util_time import RunTimestamps
from src.config.env import LeagueExportPaths


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def build_manifest_dict(
    module_name: str,
    league_key: str,
    league_root: Path,
    run_ts: RunTimestamps,
    cli_args: dict[str, Any],
    produced_paths: Iterable[Path],
) -> dict[str, Any]:
    files: dict[str, dict[str, Any]] = {}

    for abs_path in produced_paths:
        rel = abs_path.relative_to(league_root).as_posix()
        stat = abs_path.stat()
        files[rel] = {
            "size_bytes": stat.st_size,
            "sha256": _sha256_file(abs_path),
        }

    return {
        "module": module_name,
        "league_key": league_key,
        "_generated_unix": run_ts.unix,
        "_generated_iso_utc": run_ts.iso_utc,
        "_generated_iso_local": run_ts.iso_local,
        "files": files,
        "cli_args": cli_args,
    }


def write_manifest(
    paths: LeagueExportPaths,
    run_ts: RunTimestamps,
    manifest_data: dict[str, Any],
) -> Path:
    out_path = paths.manifest_dir / f"manifest.{run_ts.iso_stamp}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2, sort_keys=True)
    return out_path
