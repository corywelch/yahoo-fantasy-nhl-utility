# src/io/league_meta.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from src.util_time import RunTimestamps
from src.config.env import LeagueExportPaths


def update_league_profile(
    paths: LeagueExportPaths,
    run_ts: RunTimestamps,
    league_name: str,
    teams: Iterable[dict[str, Any]],
) -> Path:
    profile_path = paths.meta_dir / "league_profile.json"

    if profile_path.exists():
        with profile_path.open("r", encoding="utf-8") as f:
            profile = json.load(f)
    else:
        profile = {
            "league_key": paths.league_key,
            "league_name": league_name,
            "teams": {},
        }

    teams_map = profile.setdefault("teams", {})

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

    profile["_last_updated_unix"] = run_ts.unix
    profile["_last_updated_iso_utc"] = run_ts.iso_utc

    with profile_path.open("w", encoding="utf-8") as f:
        json.dump(profile, f, indent=2, sort_keys=True)

    return profile_path


def update_latest(
    paths: LeagueExportPaths,
    run_ts: RunTimestamps,
    processed_rel: str,
    excel_rel: str | None,
) -> Path:
    latest_path = paths.meta_dir / "latest.json"

    if latest_path.exists():
        with latest_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"league_key": paths.league_key}

    league_dump = data.get("league_dump", {})
    league_dump["processed"] = processed_rel
    if excel_rel is not None:
        league_dump["excel"] = excel_rel

    data["league_dump"] = league_dump
    data["_updated_unix"] = run_ts.unix
    data["_updated_iso_utc"] = run_ts.iso_utc

    with latest_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)

    return latest_path
