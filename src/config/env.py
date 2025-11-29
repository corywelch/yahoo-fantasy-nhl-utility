import os
from dataclasses import dataclass
from pathlib import Path

def get_export_dir() -> Path:
    return Path(os.getenv("EXPORT_DIR", "./exports")).expanduser().resolve()

@dataclass(frozen=True)
class LeagueExportPaths:
    league_key: str
    league_root: Path   # exports/<league_key>
    meta_dir: Path      # exports/<league_key>/_meta
    raw_dir: Path       # exports/<league_key>/league_dump/raw
    processed_dir: Path # exports/<league_key>/league_dump/processed
    excel_dir: Path     # exports/<league_key>/league_dump/excel
    manifest_dir: Path  # exports/<league_key>/league_dump/manifest


def get_league_export_paths(league_key: str, base: Path | None = None) -> LeagueExportPaths:
    root_base = base if base is not None else get_export_dir()
    league_root = root_base / league_key

    meta_dir = league_root / "_meta"
    raw_dir = league_root / "league_dump" / "raw"
    processed_dir = league_root / "league_dump" / "processed"
    excel_dir = league_root / "league_dump" / "excel"
    manifest_dir = league_root / "league_dump" / "manifest"

    for d in (meta_dir, raw_dir, processed_dir, excel_dir, manifest_dir):
        d.mkdir(parents=True, exist_ok=True)

    return LeagueExportPaths(
        league_key=league_key,
        league_root=league_root,
        meta_dir=meta_dir,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        excel_dir=excel_dir,
        manifest_dir=manifest_dir,
    )
