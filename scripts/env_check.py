#!/usr/bin/env python3
"""Environment validator for Yahoo OAuth/OIDC (localhost HTTPS).

Usage:
  python scripts/env_check.py
"""
from __future__ import annotations
import os, sys, urllib.parse
from pathlib import Path
from typing import List, Dict
from dotenv import load_dotenv

REQUIRED_KEYS = [
    "YAHOO_CLIENT_ID",
    "YAHOO_CLIENT_SECRET",
    "YAHOO_REDIRECT_URI",
]

DEFAULTS = {
    "YAHOO_SCOPE": "openid email profile fspt-r",
    "CACHE_DIR": "./data",
    "TOKEN_FILE": "./data/yahoo_token.json",
    "LOG_LEVEL": "INFO",
    "TZ": "America/Toronto",
    "HTTP_TIMEOUT": "30",
    "OAUTH_MANUAL": "0",
    "TLS_CERT_FILE": "./certs/localhost.pem",
    "TLS_KEY_FILE": "./certs/localhost-key.pem",
}

AUTH_ENDPOINT = "https://api.login.yahoo.com/oauth2/request_auth"
TOKEN_ENDPOINT = "https://api.login.yahoo.com/oauth2/get_token"
USERINFO_ENDPOINT = "https://api.login.yahoo.com/openid/v1/userinfo"

def main() -> int:
    load_dotenv()
    root = Path.cwd()
    dotenv_path = root / ".env"
    print("=== env-check (Yahoo OAuth/OIDC) ===")
    print(f"Project root: {root}")
    print(f".env present: {'yes' if dotenv_path.exists() else 'no'}\n")

    missing: List[str] = []
    for k in REQUIRED_KEYS:
        if not os.getenv(k):
            missing.append(k)
    if missing:
        print("Missing required keys:")
        for k in missing: print(f"  - {k}")
        print("\nAdd them to your .env and re-run this check.")
        return 2

    cfg: Dict[str, str] = {}
    for k in REQUIRED_KEYS: cfg[k] = os.getenv(k, "")
    for k, v in DEFAULTS.items(): cfg[k] = os.getenv(k, v)

    rid = cfg["YAHOO_REDIRECT_URI"]
    try:
        parsed = urllib.parse.urlparse(rid)
    except Exception:
        print("Invalid YAHOO_REDIRECT_URI; could not parse.")
        return 2

    if not parsed.scheme or not parsed.netloc:
        print("YAHOO_REDIRECT_URI must include scheme and host, e.g., https://127.0.0.1:8910/callback")
        return 2

    if parsed.scheme != "https":
        host = parsed.hostname or ""
        if host not in {"localhost", "127.0.0.1"}:
            print("WARNING: Redirect is not HTTPS and not localhost. Yahoo may reject this. Prefer HTTPS.")
        else:
            print("NOTICE: Using http on localhost/127.0.0.1; if Yahoo rejects it, switch to https and provide TLS_CERT_FILE/TLS_KEY_FILE.")

    # Print resolved config (redact secret)
    print("Configuration OK. Resolved values:")
    redacted = dict(cfg)
    redacted["YAHOO_CLIENT_SECRET"] = "***redacted***"
    for k, v in redacted.items():
        print(f"  {k} = {v}")

    # Ensure directories
    Path(cfg["CACHE_DIR"]).mkdir(parents=True, exist_ok=True)
    tok = Path(cfg["TOKEN_FILE"]); tok.parent.mkdir(parents=True, exist_ok=True)
    print("\nDirectories ensured:")
    print(f"  CACHE_DIR: {Path(cfg['CACHE_DIR']).resolve()}")
    print(f"  TOKEN_FILE dir: {tok.parent.resolve()}")

    print("\nYahoo endpoints:")
    print(f"  authorize: {AUTH_ENDPOINT}")
    print(f"  token:     {TOKEN_ENDPOINT}")
    print(f"  userinfo:  {USERINFO_ENDPOINT}")

    print("\nNext: run `python -m src.oauth` to perform OAuth (auto HTTPS localhost).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
