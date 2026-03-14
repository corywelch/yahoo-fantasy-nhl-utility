# scripts/raw_fetch.py
from __future__ import annotations
import argparse, json
from pathlib import Path
from src.auth.oauth import get_session
from src.config.env import get_export_dir
from src.yahoo.api_error import handle_api_error

API_BASE = "https://fantasysports.yahooapis.com/fantasy/v2"

def main():
    ap = argparse.ArgumentParser(description="Raw Yahoo Fantasy fetcher (no parsing). Supports league-scoped and global endpoints.")
    ap.add_argument("--league-key", required=False, help="e.g. 453.l.33099 (optional for global endpoints)")
    ap.add_argument("--path", required=True, help="endpoint after /league/<key>/ e.g. 'standings' or global 'game/nhl/players' or 'player/{player_key}/stats;type=game;season=2025'")
    args = ap.parse_args()

    # Support both league-scoped and global endpoints.
    # If the path already starts with a global prefix (game/, player/, players), call API_BASE/{path}
    p = args.path.lstrip('/')
    if p.startswith(('game/', 'player/', 'players', 'players;')) and not args.league_key:
        url = f"{API_BASE}/{p}?format=json"
    elif args.league_key:
        url = f"{API_BASE}/league/{args.league_key}/{p}?format=json"
    else:
        raise SystemExit("ERROR: --league-key is required for league-scoped paths. For global endpoints, omit --league-key and use a path starting with 'game/' or 'player/'.")
    sess = get_session()
    r = sess.get(url, headers={"Accept": "application/json"})
    handle_api_error(r, f"raw path {p}")
    data = r.json()  # if your app sees XML, switch to text/xml handling here

    out = get_export_dir() / "_debug" / f"{p.replace('/', '_').replace('?', '_')}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved: {out}")

if __name__ == "__main__":
    main()
