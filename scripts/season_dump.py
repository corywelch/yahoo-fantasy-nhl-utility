"""
Season dump orchestrator: runs multiple season-related scripts in sequence.

This script simplifies executing all season dump scripts. It runs:
1. season_details_dump
2. season_player_data_dump

Usage:
    python -m scripts.season_dump --season 2024 --league-key 453.l.33099 --pretty
    python -m scripts.season_dump --season 2024 --pretty
"""

import argparse
import subprocess
import sys
from typing import List

def run_script(script_name: str, args: List[str]) -> bool:
    """
    Run a script with the given arguments.

    Args:
        script_name: Name of the script to run (without .py extension)
        args: List of arguments to pass to the script

    Returns:
        True if script was executed successfully, False otherwise
    """
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

def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Orchestrate season dump scripts: details, player data."
    )

    parser.add_argument("--season", required=True, help="Season year, e.g. 2024")
    parser.add_argument("--league-key", help="Optional full league key (e.g., 453.l.33099). If provided, season_player_data_dump uses rostered players; otherwise it fetches the global game player universe.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    parser.add_argument("--to-excel", action="store_true", help="Also generate Excel workbooks for all season dumps")

    return parser.parse_args()

def _build_script_args(args: argparse.Namespace, script_name: str) -> List[str]:
    """Build argument list for individual scripts."""
    script_args = ["--season", args.season]

    # Only pass league-key to season_player_data_dump if it is provided
    if script_name == "season_player_data_dump" and args.league_key:
        script_args.extend(["--league-key", args.league_key])

    if args.pretty:
        script_args.append("--pretty")
        
    if args.to_excel:
        script_args.append("--to-excel")

    return script_args

def main() -> None:
    """Main orchestrator function."""
    args = _parse_args()

    print(f"Starting season dump orchestration for season: {args.season}")
    if args.league_key:
        print(f"Using league key: {args.league_key} for player data scoping.")
    else:
        print("No league key provided; player data will use global game universe.")
    print("This will run the following scripts in order:")
    print("1. season_details_dump")
    print("2. season_player_data_dump")
    print()

    # Run scripts in sequence
    scripts_to_run = [
        "season_details_dump",
        "season_player_data_dump"
    ]

    all_success = True

    for script_name in scripts_to_run:
        print(f"\n{'='*60}")
        print(f"Running {script_name}...")
        print(f"{'='*60}")

        script_args = _build_script_args(args, script_name)
        success = run_script(script_name, script_args)
        
        if not success:
            all_success = False
            if script_name == "season_details_dump":
                print(f"CRITICAL ERROR: {script_name} failed. Aborting orchestration.")
                break
            else:
                print(f"Warning: {script_name} failed. Continuing with next script...")

    print(f"\n{'='*60}")
    if all_success:
        print("✅ All season scripts completed successfully!")
    else:
        print("⚠️  Some scripts failed. Check output above for details.")
    print(f"{'='*60}")

    # Exit with appropriate code
    sys.exit(0 if all_success else 1)

if __name__ == "__main__":
    main()
