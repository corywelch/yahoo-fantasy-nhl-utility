#!/usr/bin/env python3
"""
League dump orchestrator: runs multiple dump scripts in sequence.

This script simplifies executing all dump scripts for a league. It runs:
1. league_details_dump (metadata + teams + scoring)
2. draft_dump (only if data doesn't already exist)
3. standings_dump
4. transactions_dump
5. rostered_players_list

Usage:
    python -m scripts.league_dump --league-key 453.l.33099 --pretty --to-excel
"""

import argparse
import subprocess
import sys
import os
import json
from pathlib import Path
from typing import List, Optional, Dict

def run_script(script_name: str, args: List[str], check_exists: bool = False) -> bool:
    """
    Run a script with the given arguments.

    Args:
        script_name: Name of the script to run (without .py extension)
        args: List of arguments to pass to the script
        check_exists: If True, check if output data already exists before running

    Returns:
        True if script was executed successfully, False otherwise
    """
    # Check if we should skip this script due to existing data
    if check_exists and script_name == "draft_dump":
        # For draft_dump, check if the data already exists
        # This is a simple check - could be enhanced to be more specific
        if _draft_data_exists(args):
            print(f"Skipping {script_name} - data already exists")
            return True

    # Use module import syntax (without .py extension)
    module_path = f"scripts.{script_name}"
    command = [sys.executable, "-m", module_path] + args
    print(f"Running: {' '.join(command)}")

    try:
        result = subprocess.run(command, check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error running {script_name}: {e}")
        return False

def _draft_data_exists(args: List[str]) -> bool:
    """
    Check if draft data already exists for the given league.

    Args:
        args: Command line arguments containing league-key

    Returns:
        True if draft data exists, False otherwise
    """
    # Parse league-key from args
    league_key = None
    i = 0
    while i < len(args):
        if args[i] == "--league-key" and i + 1 < len(args):
            league_key = args[i + 1]
            break
        elif args[i].startswith("--league-key="):
            league_key = args[i].split("=", 1)[1]
            break
        i += 1

    if not league_key:
        return False

    # Check if draft dump directory exists and has files
    exports_dir = Path("exports") / league_key / "draft_dump"
    if exports_dir.exists():
        # Check for processed or raw files
        processed_dir = exports_dir / "processed"
        raw_dir = exports_dir / "raw"

        if (processed_dir.exists() and any(processed_dir.glob("*.json"))) or \
           (raw_dir.exists() and any(raw_dir.glob("*.json"))):
            return True

    return False

def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Orchestrate league dump scripts: details, draft, standings, transactions, and rostered players."
    )

    # League identification (mutually exclusive)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--league-key", help="Full league key, e.g., 453.l.33099")
    group.add_argument("--league-id", type=int, help="League ID (use with --game)")

    # Common options
    parser.add_argument("--game", default="nhl", help="Game key (default: nhl)")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    parser.add_argument("--to-excel", action="store_true", help="Generate Excel workbooks")

    # Additional options that could be passed through
    parser.add_argument("--season", help="Season for player stats (not used in this orchestrator)")

    return parser.parse_args()

def _build_script_args(args: argparse.Namespace) -> List[str]:
    """Build argument list for individual scripts."""
    script_args = []

    if args.league_key:
        script_args.extend(["--league-key", args.league_key])
    else:
        script_args.extend(["--league-id", str(args.league_id)])

    if args.game and args.game != "nhl":
        script_args.extend(["--game", args.game])

    if args.pretty:
        script_args.append("--pretty")

    if args.to_excel:
        script_args.append("--to-excel")

    return script_args

def main() -> None:
    """Main orchestrator function."""
    args = _parse_args()
    script_args = _build_script_args(args)

    print(f"Starting league dump orchestration for league: {args.league_key or f'{args.game}.l.{args.league_id}'}")
    print("This will run the following scripts in order:")
    print("1. league_details_dump (metadata + teams + scoring)")
    print("2. draft_dump (only if data doesn't already exist)")
    print("3. standings_dump")
    print("4. transactions_dump")
    print("5. league_rostered_players_list")
    print("Note: league_players_dump will NOT be run by this orchestrator")
    print()

    # Run scripts in sequence
    scripts_to_run = [
        ("league_details_dump", False),  # Always run league details first
        ("draft_dump", True),           # Only run draft dump if data doesn't exist
        ("standings_dump", False),      # Always run standings dump
        ("transactions_dump", False),   # Always run transactions dump
        ("league_rostered_players_list", False), # Always run rostered players list
    ]

    all_success = True
    league_info = None

    for script_name, check_exists in scripts_to_run:
        print(f"\n{'='*60}")
        print(f"Running {script_name}...")
        print(f"{'='*60}")

        success = run_script(script_name, script_args, check_exists=check_exists)
        if not success:
            all_success = False
            print(f"Warning: {script_name} failed. Continuing with next script...")

        # Extract league info after running league_details_dump
        if script_name == "league_details_dump" and success:
            league_info = _extract_league_info_for_output(args.league_key)

    print(f"\n{'='*60}")
    if all_success:
        print("✅ All scripts completed successfully!")
        if league_info:
            print(f"Season: {league_info['season']}, Name: {league_info['name']}, Key: {league_info['league_key']}")
    else:
        print("⚠️  Some scripts failed. Check output above for details.")

    print(f"{'='*60}")

    # Exit with appropriate code
    sys.exit(0 if all_success else 1)

def _extract_league_info_for_output(league_key: str) -> Optional[Dict[str, str]]:
    """Extract league information from the processed league dump for display."""
    try:
        # Look for the most recent processed league file
        league_root = Path("exports") / league_key
        processed_dir = league_root / "league_dump" / "processed"

        if not processed_dir.exists():
            return None

        # Find the most recent JSON file
        json_files = list(processed_dir.glob("league.*.json"))
        if not json_files:
            return None

        # Get the most recent file by modification time
        most_recent_file = max(json_files, key=lambda f: f.stat().st_mtime)

        # Read and extract league info
        with most_recent_file.open("r", encoding="utf-8") as f:
            data = json.load(f)

        league_info = data.get("league_info", {})
        if not league_info:
            return None

        return {
            "season": league_info.get("season", "Unknown"),
            "name": league_info.get("name", "Unknown"),
            "league_key": league_info.get("league_key", league_key)
        }

    except Exception as e:
        print(f"Warning: Could not extract league info for output: {e}")
        return None

if __name__ == "__main__":
    main()
