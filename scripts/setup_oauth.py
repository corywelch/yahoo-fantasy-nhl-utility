# scripts/setup_oauth.py
"""Interactive one-time OAuth bootstrap for Yahoo.
- Starts a local HTTP server at http://127.0.0.1:8765/callback
- Opens the Yahoo consent page
- Exchanges the auth code for tokens
- Saves tokens to TOKEN_FILE (default ./data/tokens/yahoo.json)

ENV you must set:
  YAHOO_CLIENT_ID
  YAHOO_CLIENT_SECRET
Optional:
  YAHOO_REDIRECT_URI (default http://127.0.0.1:8765/callback)
  YAHOO_SCOPE (default fspt-r)
  TOKEN_FILE (default ./data/yahoo_token.json)
"""
from __future__ import annotations
import base64, json, os, socket, threading, urllib.parse, webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import requests
from src.auth.oauth import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, SCOPE, TOKEN_FILE, _basic_auth_header, TokenBundle, _save_token, YAHOO_TOKEN_URL

AUTH_URL = "https://api.login.yahoo.com/oauth2/request_auth"

def _build_auth_url(state: str) -> str:
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "language": "en-us",
        "scope": SCOPE,
        "state": state,
    }
    return f"{AUTH_URL}?{urllib.parse.urlencode(params)}"

class CaptureHandler(BaseHTTPRequestHandler):
    code: str | None = None
    error: str | None = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != urllib.parse.urlparse(REDIRECT_URI).path:
            self.send_response(404); self.end_headers(); return
        q = urllib.parse.parse_qs(parsed.query)
        if "error" in q:
            self.send_response(400); self.end_headers()
            self.wfile.write(b"OAuth error: " + q["error"][0].encode())
            CaptureHandler.error = q["error"][0]
            return
        code = q.get("code", [None])[0]
        CaptureHandler.code = code
        self.send_response(200); self.end_headers()
        self.wfile.write(b"Authorization received. You may close this window.")
    def log_message(self, format, *args):  # silence
        return

def _run_server_once() -> tuple[str | None, str | None]:
    url = urllib.parse.urlparse(REDIRECT_URI)
    host, port = url.hostname, url.port or 8765
    httpd = HTTPServer((host, port), CaptureHandler)
    # serve one request then stop
    httpd.handle_request()
    return CaptureHandler.code, CaptureHandler.error

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit("Set YAHOO_CLIENT_ID and YAHOO_CLIENT_SECRET env vars first.")
    # Open browser
    state = "yahoo-oauth"
    auth_url = _build_auth_url(state)
    print("Opening browser to:", auth_url)
    webbrowser.open(auth_url)

    code, err = _run_server_once()
    if err:
        raise SystemExit(f"OAuth error: {err}")
    if not code:
        raise SystemExit("No code captured; ensure redirect URI matches and try again.")

    # Exchange code for tokens
    headers = {
        "Authorization": _basic_auth_header(CLIENT_ID, CLIENT_SECRET),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI,
        "code": code,
    }
    resp = requests.post(YAHOO_TOKEN_URL, headers=headers, data=data, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    tb = TokenBundle.from_dict({
        "access_token": payload["access_token"],
        "refresh_token": payload.get("refresh_token", ""),
        "expires_in": payload.get("expires_in", 3600),
    })
    _save_token(tb)
    print(f"Saved token to: {TOKEN_FILE}")
    print("Done. You can now run league_dump.")

if __name__ == "__main__":
    main()
