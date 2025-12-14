#!/usr/bin/env python3
from __future__ import annotations

"""
league_players_dump: league-scoped player stats export (season totals, JSON-only).

This script depends on prior runs of ``league_dump`` and ``league_rostered_players_list``
for the same league. It *reads* their latest processed JSON outputs from:

    exports/<league_key>/_meta/latest.json
      → league_dump.processed
      → league_rostered_players_list.processed

Then, for the league's rostered player universe, it fetches season-level
player stats from Yahoo's ``league/<league_key>/players;out=stats`` endpoint,
with optional local caching per (player_key, season) to avoid repeated API calls.

Outputs under:

  exports/<league_key>/league_players_dump/
    raw/
      players.stats.season<YYYY>.<ISO>.json
    processed/
      player_stats.season<YYYY>.<ISO>.json
    manifest/
      manifest.season<YYYY>.<ISO>.json
    cache/
      season-<YYYY>/<player_key>.json    (per-player cache, internal use)

Where:
  - <league_key> is the full Yahoo league key (e.g. "465.l.22607")
  - <YYYY> is the fantasy season (e.g. "2025")
  - <ISO> is a run identifier like "20251129T014755Z" (UTC timestamp)

The processed JSON contains one record per rostered player with identity
fields (player_key, editorial_player_key, name, NHL team, positions) and a
flat map of season stat_id → numeric value, plus the season and timestamps.
"""

import argparse
import json
import sys
import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.auth.oauth import get_session
from src.config.env import get_export_dir
from src.util_time import RunTimestamps, make_run_timestamps

# Constants
API_BASE = "https://fantasysports.yahooapis.com/fantasy/v2"
BATCH_SIZE = 25  # max 25 player_keys per players call is a safe default

# =================
# DATA MODELS
# =================

@dataclass
class Paths:
    """Container for all paths used in players dump processing."""
    league_root: Path
    meta_dir: Path
    module_root: Path
    raw_dir: Path
    processed_dir: Path
    manifest_dir: Path
    cache_root: Path  # players_dump/cache

# =================
# PATH + IO HELPERS
# =================

def _paths_for_league(league_key: str) -> Paths:
    """Get standardized paths for a league's players dump.

    Args:
        league_key: Yahoo league key (e.g., '465.l.22607')

    Returns:
        Paths dataclass containing all directory paths
    """
    root = get_export_dir() / league_key
    module_root = root / "league_players_dump"
    return Paths(
        league_root=root,
        meta_dir=root / "_meta",
        module_root=module_root,
        raw_dir=module_root / "raw",
        processed_dir=module_root / "processed",
        manifest_dir=module_root / "manifest",
        cache_root=module_root / "cache",
    )

def _ensure_dirs(paths: Paths) -> None:
    """Ensure all required directories exist.

    Args:
        paths: Paths dataclass containing directory paths
    """
    paths.meta_dir.mkdir(parents=True, exist_ok=True)
    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)
    paths.manifest_dir.mkdir(parents=True, exist_ok=True)
    paths.cache_root.mkdir(parents=True, exist_ok=True)

def _sha256_of_file(path: Path) -> str:
    """Calculate SHA256 hash of a file.

    Args:
        path: Path to file to hash

    Returns:
        Hexadecimal SHA256 hash string
    """
    hash_obj = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

def _dump_json(data: Any, path: Path, pretty: bool) -> None:
    """Write JSON data to file.

    Args:
        data: Data to serialize as JSON
        path: Destination file path
        pretty: Whether to use pretty formatting
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        if pretty:
            json.dump(data, f, indent=2, sort_keys=False)
        else:
            json.dump(data, f, separators=(",", ":"), sort_keys=False)

# =================
# META + INPUT LOADING
# =================

def _load_latest_meta(league_key: str, meta_dir: Path) -> Dict[str, Any]:
    """Load latest metadata from _meta/latest.json.

    Args:
        league_key: Yahoo league key
        meta_dir: Path to _meta directory

    Returns:
        Parsed latest metadata dictionary

    Raises:
        SystemExit: If latest.json doesn't exist or can't be parsed
    """
    latest_path = meta_dir / "latest.json"
    if not latest_path.exists():
        print(
            f"ERROR: _meta/latest.json not found for league {league_key}. "
            "Run league_dump and league_rostered_players_list first.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        with latest_path.open("r", encoding="utf-8") as f:
            latest = json.load(f)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"ERROR: Failed to parse _meta/latest.json ({exc}).", file=sys.stderr)
        sys.exit(1)

    return latest

def _require_block(latest: Dict[str, Any], key: str, league_key: str) -> Dict[str, Any]:
    """Validate and return a required block from latest metadata.

    Args:
        latest: Latest metadata dictionary
        key: Block key to validate (e.g., 'league_dump')
        league_key: Yahoo league key for error messages

    Returns:
        Validated block dictionary

    Raises:
        SystemExit: If required block is missing or invalid
    """
    block = latest.get(key)
    if not isinstance(block, dict) or "processed" not in block:
        print(
            f"ERROR: _meta/latest.json is missing '{key}.processed' for league {league_key}.\n"
            f"       Run {key} for this league before running league_rostered_players_list.",
            file=sys.stderr,
        )
        sys.exit(1)
    return block

def _load_inputs_for_league(paths: Paths, league_key: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Load input data from previous export runs.

    Args:
        paths: Paths dataclass
        league_key: Yahoo league key

    Returns:
        Tuple of (latest_metadata, league_dump_data, rostered_players_data)

    Raises:
        SystemExit: If input files can't be loaded or parsed
    """
    latest = _load_latest_meta(league_key, paths.meta_dir)

    league_block = _require_block(latest, "league_dump", league_key)
    rostered_block = _require_block(latest, "league_rostered_players_list", league_key)

    league_path = paths.league_root / league_block["processed"]
    rostered_path = paths.league_root / rostered_block["processed"]

    try:
        with league_path.open("r", encoding="utf-8") as f:
            league_dump = json.load(f)
    except Exception as exc:
        print(f"ERROR: Failed to parse league_dump processed JSON at '{league_path}' ({exc}).", file=sys.stderr)
        sys.exit(1)

    try:
        with rostered_path.open("r", encoding="utf-8") as f:
            rostered_players = json.load(f)
    except Exception as exc:
        print(
            f"ERROR: Failed to parse rostered_players_list processed JSON at '{rostered_path}' ({exc}).",
            file=sys.stderr,
        )
        sys.exit(1)

    return latest, league_dump, rostered_players

# =================
# YAHOO FETCH HELPERS
# =================

def _fetch_players_stats_batch(
    session,
    league_key: str,
    player_keys: List[str],
    season: str,
) -> Dict[str, Any]:
    """Fetch player stats for a batch of players from Yahoo API.

    Args:
        session: Authenticated requests session
        league_key: Yahoo league key
        player_keys: List of player keys to fetch
        season: Fantasy season (e.g., '2025')

    Returns:
        Raw JSON response from Yahoo API
    """
    joined = ",".join(player_keys)
    endpoint = f"league/{league_key}/players;player_keys={joined};out=stats;type=season;season={season}"
    url = f"{API_BASE}/{endpoint}"
    response = session.get(url, params={"format": "json"}, headers={"Accept": "application/json"})
    response.raise_for_status()
    return response.json()

# =================
# CACHE HELPERS
# =================

def _cache_dir_for_season(paths: Paths, season: str) -> Path:
    """Get cache directory for a specific season.

    Args:
        paths: Paths dataclass
        season: Fantasy season (e.g., '2025')

    Returns:
        Path to season-specific cache directory
    """
    cache_dir = paths.cache_root / f"season-{season}"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir

def _load_cache_entry(cache_path: Path) -> Optional[Dict[str, Any]]:
    """Load cached player data from file.

    Args:
        cache_path: Path to cache file

    Returns:
        Parsed cache data, or None if file doesn't exist or can't be parsed
    """
    if not cache_path.exists():
        return None
    try:
        with cache_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _is_cache_fresh(entry: Dict[str, Any], now_unix: float, stale_hours: float) -> bool:
    """Check if cache entry is still fresh.

    Args:
        entry: Cache entry dictionary
        now_unix: Current Unix timestamp
        stale_hours: Number of hours after which cache becomes stale

    Returns:
        True if cache is fresh, False otherwise
    """
    if stale_hours <= 0:
        return True
    fetched_unix = entry.get("fetched_unix")
    if not isinstance(fetched_unix, (int, float)):
        return False
    age_hours = (now_unix - float(fetched_unix)) / 3600.0
    return age_hours <= stale_hours

def _split_and_cache_players(
    raw_payload: Dict[str, Any],
    league_key: str,
    season: str,
    cache_dir: Path,
    run_ts: RunTimestamps,
    cache: Dict[str, Dict[str, Any]],
) -> None:
    """Split raw payload into individual player cache entries.

    Args:
        raw_payload: Raw JSON payload from Yahoo API
        league_key: Yahoo league key
        season: Fantasy season
        cache_dir: Path to cache directory
        run_ts: Run timestamps
        cache: Cache dictionary to update
    """
    fantasy_content = raw_payload.get("fantasy_content") or {}
    league_node = fantasy_content.get("league")

    if not isinstance(league_node, list) or len(league_node) < 2:
        print("WARNING: Unexpected players stats payload shape (missing league list).", file=sys.stderr)
        return

    players_container = league_node[1].get("players") or {}
    if not isinstance(players_container, dict):
        print("WARNING: Unexpected players stats payload shape (missing players dict).", file=sys.stderr)
        return

    for key, value in players_container.items():
        if key == "count":
            continue
        player = value.get("player")
        if not isinstance(player, list) or len(player) < 2:
            continue

        meta_list = player[0]
        if not isinstance(meta_list, list):
            continue

        meta_flat: Dict[str, Any] = {}
        for item in meta_list:
            if isinstance(item, dict):
                meta_flat.update(item)

        player_key = meta_flat.get("player_key")
        if not player_key:
            continue

        cache_obj = {
            "league_key": league_key,
            "season": season,
            "player_key": player_key,
            "fetched_unix": run_ts.unix,
            "fetched_iso_utc": run_ts.iso_utc,
            "fetched_iso_local": run_ts.iso_local,
            "player": player,
        }

        cache_path = cache_dir / f"{player_key}.json"
        _dump_json(cache_obj, cache_path, pretty=False)
        cache[player_key] = cache_obj

# =================
# NORMALIZATION HELPERS
# =================

def _flatten_meta_list(meta_list: List[Any]) -> Dict[str, Any]:
    """Flatten list of metadata dictionaries into single dictionary.

    Args:
        meta_list: List of metadata dictionaries

    Returns:
        Flattened dictionary containing all metadata fields
    """
    flattened: Dict[str, Any] = {}
    for item in meta_list:
        if isinstance(item, dict):
            flattened.update(item)
    return flattened

def _maybe_number(value: Any) -> Any:
    """Convert string values to numbers if possible.

    Args:
        value: Value to convert

    Returns:
        Integer or float if conversion successful, original value otherwise
    """
    if isinstance(value, (int, float)):
        return value
    if not isinstance(value, str):
        return value
    stripped_value = value.strip()
    if stripped_value == "":
        return value
    try:
        if "." in stripped_value:
            return float(stripped_value)
        return int(stripped_value)
    except ValueError:
        try:
            return float(stripped_value)
        except ValueError:
            return value

def _extract_stats_block(block: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Extract stats from Yahoo API stats block.

    Args:
        block: Stats block from Yahoo API

    Returns:
        Dictionary mapping stat_id to numeric values
    """
    if not isinstance(block, dict):
        return {}

    stats_list = block.get("stats") or []
    result: Dict[str, Any] = {}

    for item in stats_list:
        if not isinstance(item, dict):
            continue
        stat = item.get("stat") or {}
        stat_id = stat.get("stat_id")
        if stat_id is None:
            continue
        value = stat.get("value")
        result[str(stat_id)] = _maybe_number(value)

    return result

def _build_processed_players(
    league_dump: Dict[str, Any],
    rostered_players: Dict[str, Any],
    season: str,
    cache: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build processed player data from cache.

    Args:
        league_dump: League dump data
        rostered_players: Rostered players data
        season: Fantasy season
        cache: Player cache dictionary

    Returns:
        List of processed player dictionaries
    """
    players_in_roster = rostered_players.get("players") or []
    player_keys: List[str] = []

    for player in players_in_roster:
        if isinstance(player, dict):
            player_key = player.get("player_key")
            if player_key:
                player_keys.append(player_key)

    # Ensure uniqueness and stable ordering
    player_keys = sorted(set(player_keys))

    processed: List[Dict[str, Any]] = []

    for player_key in player_keys:
        entry = cache.get(player_key)
        if not entry:
            # If cache is missing for some reason, skip gracefully.
            continue

        player = entry.get("player")
        if not isinstance(player, list) or len(player) < 2:
            continue

        meta_list = player[0]
        extra = player[1]

        if not isinstance(meta_list, list) or not isinstance(extra, dict):
            continue

        meta_flat = _flatten_meta_list(meta_list)

        name_info = meta_flat.get("name") or {}
        full_name = name_info.get("full")
        if not full_name:
            first = name_info.get("first") or ""
            last = name_info.get("last") or ""
            full_name = f"{first} {last}".strip() or None

        eligible_positions = meta_flat.get("eligible_positions") or []
        positions: List[str] = []
        if isinstance(eligible_positions, list):
            for item in eligible_positions:
                if isinstance(item, dict):
                    position = item.get("position")
                    if isinstance(position, str):
                        positions.append(position)
        positions = sorted(set(positions))

        player_stats_block = extra.get("player_stats")
        advanced_stats_block = extra.get("player_advanced_stats")

        stats = _extract_stats_block(player_stats_block)
        advanced_stats = _extract_stats_block(advanced_stats_block)

        processed.append(
            {
                "player_key": meta_flat.get("player_key"),
                "player_id": meta_flat.get("player_id"),
                "editorial_player_key": meta_flat.get("editorial_player_key"),
                "name_full": full_name,
                "name_first": name_info.get("first"),
                "name_last": name_info.get("last"),
                "nhl_team_key": meta_flat.get("editorial_team_key"),
                "nhl_team_abbr": meta_flat.get("editorial_team_abbr"),
                "nhl_team_full_name": meta_flat.get("editorial_team_full_name"),
                "positions": positions,
                "is_keeper": meta_flat.get("is_keeper"),
                "season": season,
                "stats": stats,
                "advanced_stats": advanced_stats,
            }
        )

    processed.sort(key=lambda p: (p.get("name_full") or "", p.get("player_key") or ""))
    return processed

# =================
# MANIFEST + LATEST.JSON
# =================

def _write_manifest(
    paths: Paths,
    league_key: str,
    season: str,
    run_ts: RunTimestamps,
    cli_args: Dict[str, Any],
    raw_path: Path,
    processed_path: Path,
) -> Path:
    """Write manifest file for this run.

    Args:
        paths: Paths dataclass
        league_key: Yahoo league key
        season: Fantasy season
        run_ts: Run timestamps
        cli_args: CLI arguments dictionary
        raw_path: Path to raw data file
        processed_path: Path to processed data file

    Returns:
        Path to written manifest file
    """
    manifest_path = paths.manifest_dir / f"manifest.season{season}.{run_ts.iso_stamp}.json"

    def rel(path: Path) -> str:
        return path.relative_to(paths.league_root).as_posix()

    files: Dict[str, Dict[str, Any]] = {}

    if raw_path.exists():
        files[rel(raw_path)] = {
            "size_bytes": raw_path.stat().st_size,
            "sha256": _sha256_of_file(raw_path),
        }

    if processed_path.exists():
        files[rel(processed_path)] = {
            "size_bytes": processed_path.stat().st_size,
            "sha256": _sha256_of_file(processed_path),
        }

    manifest = {
        "module": "league_players_dump",
        "league_key": league_key,
        "season": season,
        "_generated_unix": run_ts.unix,
        "_generated_iso_utc": run_ts.iso_utc,
        "_generated_iso_local": run_ts.iso_local,
        "cli_args": cli_args,
        "files": files,
    }

    _dump_json(manifest, manifest_path, pretty=True)
    return manifest_path

def _update_latest_meta(
    paths: Paths,
    league_key: str,
    season: str,
    raw_path: Path,
    processed_path: Path,
    manifest_path: Path,
    run_ts: RunTimestamps,
) -> None:
    """Update latest metadata with players dump information.

    Args:
        paths: Paths dataclass
        league_key: Yahoo league key
        season: Fantasy season
        raw_path: Path to raw data file
        processed_path: Path to processed data file
        manifest_path: Path to manifest file
        run_ts: Run timestamps
    """
    latest_path = paths.meta_dir / "latest.json"

    if latest_path.exists():
        try:
            with latest_path.open("r", encoding="utf-8") as f:
                latest = json.load(f)
        except Exception:
            latest = {}
    else:
        latest = {}

    latest["league_key"] = league_key
    processed_rel = processed_path.relative_to(paths.league_root).as_posix()
    raw_rel = raw_path.relative_to(paths.league_root).as_posix()
    manifest_rel = manifest_path.relative_to(paths.league_root).as_posix()

    latest["league_players_dump"] = {
        "season": season,
        "processed": processed_rel,
        "raw": raw_rel,
        "manifest": manifest_rel,
    }
    latest["_updated_unix"] = run_ts.unix
    latest["_updated_iso_utc"] = run_ts.iso_utc

    _dump_json(latest, latest_path, pretty=True)

# =================
# CLI + MAIN
# =================

def _parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Fetch and cache season-level player stats for all rostered players."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--league-key", help="Full league key, e.g. 465.l.22607")
    group.add_argument("--league-id", help="League ID, e.g. 22607 (paired with --game)")

    parser.add_argument("--game", default="nhl", help="Game code (default: nhl)")
    parser.add_argument(
        "--season",
        help="Fantasy season (e.g. 2025). Default: league season from league_dump.",
    )
    parser.add_argument(
        "--stale-hours",
        type=float,
        default=24.0,
        help="Cache freshness window in hours (default: 24). "
             "Set to 0 to always treat cache as fresh.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore existing cache and re-fetch all players.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON outputs.",
    )

    return parser.parse_args()

def _resolve_league_key(args: argparse.Namespace) -> str:
    """Resolve league key from arguments.

    Args:
        args: Parsed arguments

    Returns:
        Resolved league key string
    """
    if args.league_key:
        return args.league_key
    return f"{args.game}.l.{args.league_id}"

def main() -> None:
    """Main entry point for players dump script."""
    args = _parse_args()
    league_key = _resolve_league_key(args)

    paths = _paths_for_league(league_key)
    _ensure_dirs(paths)

    run_ts = make_run_timestamps()

    latest, league_dump, rostered_players = _load_inputs_for_league(paths, league_key)

    league_info = league_dump.get("league_info") or {}
    season = args.season or league_info.get("season")
    if not season:
        print("ERROR: Could not determine season (no --season and league_dump.league_info.season missing).", file=sys.stderr)
        sys.exit(1)

    season = str(season)

    # Gather rostered player_keys
    roster_players = rostered_players.get("players") or []
    player_keys: List[str] = []
    for player in roster_players:
        if isinstance(player, dict):
            player_key = player.get("player_key")
            if player_key:
                player_keys.append(player_key)
    player_keys = sorted(set(player_keys))

    if not player_keys:
        print("No rostered players found; nothing to do.", file=sys.stderr)
        sys.exit(0)

    cache_dir = _cache_dir_for_season(paths, season)
    cache: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []

    # Load existing cache where fresh
    for player_key in player_keys:
        cache_path = cache_dir / f"{player_key}.json"
        entry = _load_cache_entry(cache_path)
        if entry and not args.force_refresh and _is_cache_fresh(entry, run_ts.unix, args.stale_hours):
            cache[player_key] = entry
        else:
            missing.append(player_key)

    raw_responses: List[Dict[str, Any]] = []

    if missing:
        session = get_session()
        total = len(missing)
        batches = math.ceil(total / BATCH_SIZE)

        for i in range(batches):
            batch = missing[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
            if not batch:
                continue

            raw = _fetch_players_stats_batch(
                session,
                league_key=league_key,
                player_keys=batch,
                season=season
            )
            raw_responses.append(raw)
            _split_and_cache_players(
                raw_payload=raw,
                league_key=league_key,
                season=season,
                cache_dir=cache_dir,
                run_ts=run_ts,
                cache=cache,
            )

    # Build processed player stats from cache
    processed_players = _build_processed_players(
        league_dump=league_dump,
        rostered_players=rostered_players,
        season=season,
        cache=cache,
    )

    processed = {
        "league_key": league_info.get("league_key", league_key),
        "season": season,
        "generated_unix": run_ts.unix,
        "generated_iso_utc": run_ts.iso_utc,
        "generated_iso_local": run_ts.iso_local,
        "player_count": len(processed_players),
        "players": processed_players,
    }

    # Write raw + processed
    raw_path = paths.raw_dir / f"players.stats.season{season}.{run_ts.iso_stamp}.json"
    raw_wrapper = {
        "league_key": league_key,
        "season": season,
        "generated_unix": run_ts.unix,
        "generated_iso_utc": run_ts.iso_utc,
        "generated_iso_local": run_ts.iso_local,
        "response_count": len(raw_responses),
        "responses": raw_responses,
    }
    _dump_json(raw_wrapper, raw_path, pretty=args.pretty)
    print(f"Wrote raw players stats wrapper: {raw_path}")

    processed_path = paths.processed_dir / f"player_stats.season{season}.{run_ts.iso_stamp}.json"
    _dump_json(processed, processed_path, pretty=args.pretty)
    print(f"Wrote processed player stats JSON: {processed_path}")

    cli_args: Dict[str, Any] = {
        "league_key": league_key,
        "league_id": getattr(args, "league_id", None),
        "game": getattr(args, "game", None),
        "season": season,
        "stale_hours": args.stale_hours,
        "force_refresh": bool(args.force_refresh),
        "pretty": bool(args.pretty),
    }

    manifest_path = _write_manifest(
        paths=paths,
        league_key=league_key,
        season=season,
        run_ts=run_ts,
        cli_args=cli_args,
        raw_path=raw_path,
        processed_path=processed_path,
    )
    print(f"Wrote manifest: {manifest_path}")

    _update_latest_meta(
        paths=paths,
        league_key=league_key,
        season=season,
        raw_path=raw_path,
        processed_path=processed_path,
        manifest_path=manifest_path,
        run_ts=run_ts,
    )
    print(f"Updated latest metadata: {paths.meta_dir / 'latest.json'}")

if __name__ == "__main__":  # pragma: no cover
    main()
