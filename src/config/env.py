"""Configuration and environment utilities for Yahoo Fantasy NHL Utility."""

import os
from dataclasses import dataclass
from pathlib import Path

def get_export_dir() -> Path:
    """Get the base export directory path.

    Returns:
        Path object pointing to the export directory.
        Defaults to './exports' if EXPORT_DIR environment variable not set.
    """
    return Path(os.getenv("EXPORT_DIR", "./exports")).expanduser().resolve()

@dataclass(frozen=True)
class LeagueExportPaths:
    """Data structure containing paths for league export directories.

    Attributes:
        league_key: Yahoo league key
        league_root: Base directory for league exports
        meta_dir: Directory for metadata files
        raw_dir: Directory for raw API response files
        processed_dir: Directory for processed data files
        excel_dir: Directory for Excel output files
        manifest_dir: Directory for manifest files
    """
    league_key: str
    league_root: Path   # exports/<league_key>
    meta_dir: Path      # exports/<league_key>/_meta
    raw_dir: Path       # exports/<league_key>/league_dump/raw
    processed_dir: Path # exports/<league_key>/league_dump/processed
    excel_dir: Path     # exports/<league_key>/league_dump/excel
    manifest_dir: Path  # exports/<league_key>/league_dump/manifest

def get_league_export_paths(league_key: str, base: Path | None = None) -> LeagueExportPaths:
    """Get standardized export paths for a league.

    Creates directory structure if it doesn't exist.

    Args:
        league_key: Yahoo league key (e.g., '465.l.22607')
        base: Optional base directory. If None, uses get_export_dir()

    Returns:
        LeagueExportPaths dataclass with all path components
    """
    root_base = base if base is not None else get_export_dir()
    league_root = root_base / league_key

    # Define all directory paths
    meta_dir = league_root / "_meta"
    raw_dir = league_root / "league_dump" / "raw"
    processed_dir = league_root / "league_dump" / "processed"
    excel_dir = league_root / "league_dump" / "excel"
    manifest_dir = league_root / "league_dump" / "manifest"

    # Create directories if they don't exist
    for directory in (meta_dir, raw_dir, processed_dir, excel_dir, manifest_dir):
        directory.mkdir(parents=True, exist_ok=True)

    return LeagueExportPaths(
        league_key=league_key,
        league_root=league_root,
        meta_dir=meta_dir,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        excel_dir=excel_dir,
        manifest_dir=manifest_dir,
    )
