"""League metadata utilities for managing profile and latest.json files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from src.util_time import RunTimestamps
from src.config.env import LeagueExportPaths

def update_league_profile(
    paths: LeagueExportPaths,
    run_timestamps: RunTimestamps,
    league_name: str,
    teams: Iterable[Dict[str, Any]],
) -> Path:
    """Update league profile with team information.

    Maintains a canonical mapping of team keys to team metadata including
    names, abbreviations, logos, and URLs.

    Args:
        paths: LeagueExportPaths containing directory paths
        run_timestamps: RunTimestamps with current timestamps
        league_name: Name of the league
        teams: Iterable of team dictionaries

    Returns:
        Path to the updated league profile JSON file
    """
    profile_path = paths.meta_dir / "league_profile.json"

    # Load existing profile or create new one
    if profile_path.exists():
        with profile_path.open("r", encoding="utf-8") as file:
            profile = json.load(file)
    else:
        profile = {
            "league_key": paths.league_key,
            "league_name": league_name,
            "teams": {},
        }

    teams_map = profile.setdefault("teams", {})

    # Update team information
    for team in teams:
        team_key = team["team_key"]
        abbrev = team.get("abbrev") or team.get("team_abbr")
        logo_url = (
            team.get("logo_url")
            or (team.get("team_logo") or {}).get("url")
        )
        team_url = team.get("team_url") or team.get("url")

        teams_map[team_key] = {
            "name": team["name"],
            "abbrev": abbrev,
            "logo_url": logo_url,
            "team_url": team_url,
        }

    # Update metadata timestamps
    profile["_last_updated_unix"] = run_timestamps.unix
    profile["_last_updated_iso_utc"] = run_timestamps.iso_utc

    # Write updated profile
    with profile_path.open("w", encoding="utf-8") as file:
        json.dump(profile, file, indent=2, sort_keys=True)

    return profile_path

def update_latest(
    paths: LeagueExportPaths,
    run_timestamps: RunTimestamps,
    processed_rel: str,
    excel_rel: Optional[str],
) -> Path:
    """Update latest.json with current export information.

    Maintains a record of the latest processed files for each module,
    preserving other modules' entries.

    Args:
        paths: LeagueExportPaths containing directory paths
        run_timestamps: RunTimestamps with current timestamps
        processed_rel: Relative path to processed data file
        excel_rel: Optional relative path to Excel file

    Returns:
        Path to the updated latest.json file
    """
    latest_path = paths.meta_dir / "latest.json"

    # Load existing latest.json or create new one
    if latest_path.exists():
        with latest_path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    else:
        data = {"league_key": paths.league_key}

    # Update league_dump section
    league_dump = data.get("league_dump", {})
    league_dump["processed"] = processed_rel
    if excel_rel is not None:
        league_dump["excel"] = excel_rel

    data["league_dump"] = league_dump
    data["_updated_unix"] = run_timestamps.unix
    data["_updated_iso_utc"] = run_timestamps.iso_utc

    # Write updated latest.json
    with latest_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, sort_keys=True)

    return latest_path
