"""Run manifest utilities for tracking export file integrity."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable

from src.util_time import RunTimestamps
from src.config.env import LeagueExportPaths

def _sha256_file(path: Path) -> str:
    """Calculate SHA256 hash of a file.

    Args:
        path: Path to file to hash

    Returns:
        Hexadecimal SHA256 hash string
    """
    hash_obj = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

def build_manifest_dict(
    module_name: str,
    league_key: str,
    league_root: Path,
    run_timestamps: RunTimestamps,
    cli_args: Dict[str, Any],
    produced_paths: Iterable[Path],
) -> Dict[str, Any]:
    """Build manifest dictionary for export run.

    Creates a manifest containing metadata about all files produced
    during an export run, including file sizes and SHA256 hashes.

    Args:
        module_name: Name of the module that produced the files
        league_key: Yahoo league key
        league_root: Root directory for the league
        run_timestamps: RunTimestamps with current timestamps
        cli_args: Dictionary of CLI arguments used
        produced_paths: Iterable of paths to files produced

    Returns:
        Manifest dictionary ready for serialization
    """
    files: Dict[str, Dict[str, Any]] = {}

    for absolute_path in produced_paths:
        relative_path = absolute_path.relative_to(league_root).as_posix()
        file_stat = absolute_path.stat()
        files[relative_path] = {
            "size_bytes": file_stat.st_size,
            "sha256": _sha256_file(absolute_path),
        }

    return {
        "module": module_name,
        "league_key": league_key,
        "_generated_unix": run_timestamps.unix,
        "_generated_iso_utc": run_timestamps.iso_utc,
        "_generated_iso_local": run_timestamps.iso_local,
        "files": files,
        "cli_args": cli_args,
    }

def write_manifest(
    paths: LeagueExportPaths,
    run_timestamps: RunTimestamps,
    manifest_data: Dict[str, Any],
) -> Path:
    """Write manifest dictionary to file.

    Args:
        paths: LeagueExportPaths containing directory paths
        run_timestamps: RunTimestamps with current timestamps
        manifest_data: Manifest dictionary to write

    Returns:
        Path to the written manifest file
    """
    output_path = paths.manifest_dir / f"manifest.{run_timestamps.iso_stamp}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(manifest_data, file, indent=2, sort_keys=True)

    return output_path
