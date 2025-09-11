# src/fantasy.py
#!/usr/bin/env python3
"""Simple Yahoo Fantasy Sports API client.

Usage examples:
  python -m src.fantasy --whoami
  python -m src.fantasy --games
  python -m src.fantasy --leagues --game-key nhl        # or numeric game key like 458
  python -m src.fantasy --league-meta --league-key 458.l.12345
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

import requests

# Reuse helpers from oauth.py
from .oauth import load_config, read_token, token_is_valid, refresh_token, write_token

API_BASE = "https://fantasysports.yahooapis.com/fantasy/v2"

def _ensure_access_token() -> Dict:
    cfg = load_config()
    tok_path = Path(cfg["token_file"])
    tok = read_token(tok_path)
    if not tok:
        raise SystemExit("No token found. Run: python -m src.oauth")
    if not token_is_valid(tok):
        tok = refresh_token(cfg, tok)
        write_token(tok_path, tok)
    return {"cfg": cfg, "tok": tok}

def _get(url_path: str, params: Optional[Dict[str, str]] = None) -> Dict:
    """Make an authenticated GET and return JSON."""
    ctx = _ensure_access_token()
    cfg, tok = ctx["cfg"], ctx["tok"]
    params = params or {}
    params.setdefault("format", "json")
    url = f"{API_BASE}{url_path}"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {tok['access_token']}"},
        params=params,
        timeout=cfg["http_timeout"],
    )
    if resp.status_code != 200:
        raise SystemExit(f"GET {url} failed: {resp.status_code} {resp.text[:300]}")
    return resp.json()

def whoami() -> Dict:
    """Fetch OpenID UserInfo (useful sanity check)."""
    ctx = _ensure_access_token()
    cfg, tok = ctx["cfg"], ctx["tok"]
    r = requests.get(
        "https://api.login.yahoo.com/openid/v1/userinfo",
        headers={"Authorization": f"Bearer {tok['access_token']}"},
        timeout=cfg["http_timeout"],
    )
    if r.status_code != 200:
        raise SystemExit(f"userinfo failed: {r.status_code} {r.text[:200]}")
    return r.json()

def list_games_for_logged_in_user() -> Dict:
    """GET /users;use_login=1/games"""
    return _get("/users;use_login=1/games")

def list_leagues_for_game(game_key: str) -> Dict:
    """GET /users;use_login=1/games;game_keys={game_key}/leagues"""
    return _get(f"/users;use_login=1/games;game_keys={game_key}/leagues")

def league_metadata(league_key: str) -> Dict:
    """GET /league/{league_key}  (league_key: <game_id>.l.<league_id>)"""
    return _get(f"/league/{league_key}")

def main() -> int:
    ap = argparse.ArgumentParser(description="Yahoo Fantasy API client")
    ap.add_argument("--whoami", action="store_true", help="Fetch OpenID user info")
    ap.add_argument("--games", action="store_true", help="List games for the logged-in user")
    ap.add_argument("--leagues", action="store_true", help="List leagues for a given game_key")
    ap.add_argument("--league-meta", action="store_true", help="Fetch league metadata for a given league_key")
    ap.add_argument("--game-key", type=str, help="Game key (e.g., nhl or a numeric like 458)")
    ap.add_argument("--league-key", type=str, help="League key (e.g., 458.l.12345)")
    args = ap.parse_args()

    if args.whoami:
        print(json.dumps(whoami(), indent=2)); return 0
    if args.games:
        print(json.dumps(list_games_for_logged_in_user(), indent=2)); return 0
    if args.leagues:
        if not args.game_key:
            raise SystemExit("--leagues requires --game-key (e.g., nhl)")
        print(json.dumps(list_leagues_for_game(args.game_key), indent=2)); return 0
    if args.league_meta:
        if not args.league_key:
            raise SystemExit("--league-meta requires --league-key (e.g., 458.l.12345)")
        print(json.dumps(league_metadata(args.league_key), indent=2)); return 0

    ap.print_help(); return 0

if __name__ == "__main__":
    raise SystemExit(main())
