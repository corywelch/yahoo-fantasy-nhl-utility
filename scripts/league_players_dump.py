#!/usr/bin/env python3
from __future__ import annotations

"""
league_players_dump: league-scoped player stats export (wrapper).

This script now acts as a wrapper around season_player_data_dump.py, which
handles all the fetching and writing of player data directly into exports/<season>/playerdata/.
We delegate execution to that script, passing along the --league-key argument.
"""

import argparse
import sys
import subprocess
import json
from src.config.env import get_export_dir

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch season-level player stats for all rostered players (wrapper)."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--league-key", help="Full league key, e.g. 465.l.22607")
    group.add_argument("--league-id", help="League ID, e.g. 22607 (paired with --game)")

    parser.add_argument("--game", default="nhl", help="Game code (default: nhl)")
    parser.add_argument(
        "--season",
        help="Fantasy season (e.g. 2025).",
        required=True
    )
    # Ignored arguments kept for backwards compatibility
    parser.add_argument("--stale-hours", type=float, default=24.0, help=argparse.SUPPRESS)
    parser.add_argument("--force-refresh", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON outputs.")
    parser.add_argument("--to-excel", action="store_true", help="Also generate an Excel summary.")

    return parser.parse_args()

def main() -> None:
    args = _parse_args()
    league_key = args.league_key if args.league_key else f"{args.game}.l.{args.league_id}"

    cmd = [
        sys.executable,
        "-m", "scripts.season_player_data_dump",
        "--league-key", league_key,
        "--season", str(args.season)
    ]
    if args.pretty:
        cmd.append("--pretty")
    if args.to_excel:
        cmd.append("--to-excel")

    print(f"Delegating league players dump to season_player_data_dump.py for league {league_key}...")
    try:
        subprocess.run(cmd, check=True)
        print(f"Successfully ran season_player_data_dump for league {league_key}.")
        print(f"Note: All player data is now exported to exports/{args.season}/playerdata/.")
        
        # Add pointer in league metadata
        league_meta = get_export_dir() / league_key / "_meta" / "latest.json"
        if league_meta.exists():
            try:
                latest = json.loads(league_meta.read_text(encoding="utf-8"))
                latest["league_players_dump"] = {
                    "note": f"Data has been migrated to exports/{args.season}/playerdata/"
                }
                league_meta.write_text(json.dumps(latest, indent=2), encoding="utf-8")
            except Exception as meta_ex:
                print(f"Warning: Failed to add migration note to league metadata: {meta_ex}")

    except subprocess.CalledProcessError as e:
        print(f"ERROR: Delegate script failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)

if __name__ == "__main__":
    main()
