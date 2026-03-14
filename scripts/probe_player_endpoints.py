#!/usr/bin/env python3
from __future__ import annotations

"""Probe Yahoo endpoints for a small set of players and save responses.

Usage: python -m scripts.probe_player_endpoints --season 2025
"""

import json
import argparse
from pathlib import Path
from typing import List

from src.auth.oauth import get_session

API_BASE = "https://fantasysports.yahooapis.com/fantasy/v2"


def _save(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True)
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()

    base = Path("exports/playerdata") / str(args.season)
    if not base.exists():
        print("No playerdata for season", args.season)
        return

    dirs = [p for p in base.iterdir() if p.is_dir()]
    dirs = sorted(dirs)[: args.limit]
    session = get_session()

    out_base = Path("exports/_debug/probe") / str(args.season)

    for d in dirs:
        player_file = d / f"{d.name}.json"
        if not player_file.exists():
            continue
        try:
            p = json.loads(player_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        player_key = p.get("player_key") or d.name
        player_id = p.get("player_id")

        endpoints = [
            ("player_stats_basic", f"{API_BASE}/player/{player_key}/stats?format=json"),
            ("player_stats_game", f"{API_BASE}/player/{player_key}/stats;type=game;season={args.season}?format=json"),
            ("players_batch_game", f"{API_BASE}/players;player_keys={player_key};out=stats;type=game;season={args.season}"),
            ("player_profile", f"{API_BASE}/player/{player_key}/profile?format=json"),
            ("game_nhl_players", f"{API_BASE}/game/nhl/players?format=json"),
        ]

        for name, url in endpoints:
            try:
                r = session.get(url, headers={"Accept": "application/json"}, timeout=30)
                try:
                    data = r.json()
                except Exception:
                    data = {"status": r.status_code, "text": r.text}
            except Exception as e:
                data = {"error": str(e)}

            fname = out_base / player_key / f"{name}.json"
            _save(fname, data)
            print(f"Wrote probe: {fname}")


if __name__ == "__main__":
    main()
