# scripts/raw_fetch.py
from __future__ import annotations
import argparse, json
from pathlib import Path
from src.auth.oauth import get_session
from src.config.env import get_export_dir

API_BASE = "https://fantasysports.yahooapis.com/fantasy/v2"

def main():
    ap = argparse.ArgumentParser(description="Raw Yahoo Fantasy fetcher (no parsing).")
    ap.add_argument("--league-key", required=True, help="e.g. 453.l.33099")
    ap.add_argument("--path", required=True, help="endpoint after /league/<key>/ e.g. 'standings' or 'transactions;type=trade'")
    args = ap.parse_args()

    url = f"{API_BASE}/league/{args.league_key}/{args.path}?format=json"
    sess = get_session()
    r = sess.get(url, headers={"Accept": "application/json"})
    r.raise_for_status()
    data = r.json()  # if your app sees XML, switch to text/xml handling here

    out = get_export_dir() / "_debug" / f"{args.path.replace('/', '_')}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved: {out}")

if __name__ == "__main__":
    main()
