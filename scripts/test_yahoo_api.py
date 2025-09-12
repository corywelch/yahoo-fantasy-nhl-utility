# scripts/test_yahoo_api.py
"""
Minimal raw tester to confirm API shape.
It reads the Bearer token via src/auth/oauth.get_session and pulls three endpoints:
  /metadata, /settings, /teams
Saves raw JSON/XML to ./exports/_debug and prints top keys.
"""
from __future__ import annotations
import json
from pathlib import Path
import argparse

from src.auth.oauth import get_session
from src.config.env import get_export_dir

API_BASE = "https://fantasysports.yahooapis.com/fantasy/v2"

def save(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def fetch(session, url: str):
    r = session.get(url, headers={"Accept": "application/json"})
    ctype = r.headers.get("Content-Type","")
    if "json" in ctype:
        return r.json()
    # fallback XML to dict if available
    try:
        import xmltodict
        return xmltodict.parse(r.text)
    except Exception:
        return {"_raw": r.text, "_content_type": ctype}

def keys_of(obj):
    if isinstance(obj, dict):
        return list(obj.keys())
    if isinstance(obj, list):
        return f"list[{len(obj)}]"
    return type(obj).__name__

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--league-key", required=True, help="Full league key, e.g., 465.l.22607")
    args = ap.parse_args()

    outdir = get_export_dir() / "_debug"
    sess = get_session()

    for suffix in ["metadata", "settings", "teams"]:
        url = f"{API_BASE}/league/{args.league_key}/{suffix}?format=json"
        data = fetch(sess, url)
        save(outdir / f"{suffix}.json", data)
        print(f"\n[{suffix}] top-level keys:", keys_of(data))
        if isinstance(data, dict) and "fantasy_content" in data:
            fc = data.get("fantasy_content", {})
            print("  fantasy_content keys:", keys_of(fc))
            lg = fc.get("league")
            print("  league type:", type(lg).__name__)

if __name__ == "__main__":
    main()
