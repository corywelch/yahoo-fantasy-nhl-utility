
#!/usr/bin/env python3
from __future__ import annotations

import argparse, base64, http.server, json, logging, os, random, ssl, string, threading, time, urllib.parse, webbrowser
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import requests
from dotenv import load_dotenv

AUTH_URL = "https://api.login.yahoo.com/oauth2/request_auth"
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"
USERINFO_URL = "https://api.login.yahoo.com/openid/v1/userinfo"

def now_epoch() -> int: return int(time.time())

def human_time(ts: int, tz_name: str = "America/Toronto") -> str:
    try:
        dt = datetime.fromtimestamp(ts); return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} ({tz_name})"
    except Exception: return str(ts)

def build_basic_auth_header(client_id: str, client_secret: str) -> str:
    return base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")

def load_config() -> Dict[str, str]:
    load_dotenv()
    cfg = {
        "client_id": os.getenv("YAHOO_CLIENT_ID"),
        "client_secret": os.getenv("YAHOO_CLIENT_SECRET"),
        "redirect_uri": os.getenv("YAHOO_REDIRECT_URI"),
        "scope": os.getenv("YAHOO_SCOPE", "openid email profile fspt-r"),
        "cache_dir": os.getenv("CACHE_DIR", "./data"),
        "token_file": os.getenv("TOKEN_FILE", "./data/yahoo_token.json"),
        "log_level": os.getenv("LOG_LEVEL", "INFO").upper(),
        "http_timeout": int(os.getenv("HTTP_TIMEOUT", "30")),
        "tz": os.getenv("TZ", "America/Toronto"),
        "tls_cert": os.getenv("TLS_CERT_FILE", "./certs/localhost.pem"),
        "tls_key": os.getenv("TLS_KEY_FILE", "./certs/localhost-key.pem"),
        "manual_env": os.getenv("OAUTH_MANUAL", "0").strip() in {"1","true","TRUE","yes","on"},
        "prompt": os.getenv("OAUTH_PROMPT", "").strip(),
    }
    missing = [k for k, v in {"YAHOO_CLIENT_ID": cfg["client_id"], "YAHOO_CLIENT_SECRET": cfg["client_secret"], "YAHOO_REDIRECT_URI": cfg["redirect_uri"]}.items() if not v]
    if missing: raise SystemExit(f"Missing required env keys: {', '.join(missing)}. Run scripts/env_check.py first.")
    Path(cfg["cache_dir"]).mkdir(parents=True, exist_ok=True); Path(cfg["token_file"]).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=getattr(logging, cfg["log_level"], logging.INFO), format="[%(levelname)s] %(message)s"); return cfg

def read_token(path: Path) -> Optional[Dict]:
    return json.load(path.open("r", encoding="utf-8")) if path.exists() else None

def write_token(path: Path, data: Dict) -> None:
    tmp = path.with_suffix(".tmp"); json.dump(data, tmp.open("w", encoding="utf-8"), indent=2); tmp.replace(path)

def token_is_valid(tok: Dict) -> bool: return int(tok.get("expires_at", 0)) - now_epoch() > 60

def refresh_token(cfg: Dict[str, str], tok: Dict) -> Dict:
    logging.info("Refreshing access token...")
    headers = {"Authorization": f"Basic {build_basic_auth_header(cfg['client_id'], cfg['client_secret'])}", "Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "refresh_token", "redirect_uri": cfg["redirect_uri"], "refresh_token": tok["refresh_token"]}
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=cfg["http_timeout"])
    if resp.status_code != 200: raise SystemExit(f"Refresh failed: {resp.status_code} {resp.text}")
    p = resp.json(); new_tok = {**tok, "access_token": p.get("access_token", tok.get("access_token")), "token_type": p.get("token_type", tok.get("token_type", "bearer")), "expires_in": int(p.get("expires_in", tok.get("expires_in", 3600))), "scope": p.get("scope", tok.get("scope"))}
    new_tok["expires_at"] = now_epoch() + int(new_tok["expires_in"]) - 60; logging.info("Token refreshed. Expires at %s", human_time(new_tok["expires_at"], cfg["tz"])); return new_tok

class OAuthHandler(http.server.BaseHTTPRequestHandler):
    server_version = "YahooOAuth/2.0"; _state: str = ""; _code: Optional[str] = None; _error: Optional[str] = None
    def log_message(self, format, *args): logging.debug("HTTP: " + format % args)
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path); qs = urllib.parse.parse_qs(parsed.query)
        state, code, error = qs.get("state", [None])[0], qs.get("code", [None])[0], qs.get("error", [None])[0]
        if error: self._error = error
        if code: self._code = code
        if state != self._state: self.send_response(400); self.end_headers(); self.wfile.write(b"State mismatch. You can close this window."); return
        self.send_response(200); self.end_headers(); self.wfile.write(b"Authorization received. You can close this window." if not error else b"Authorization failed. You can close this window.")

def start_local_server(redirect_uri: str, state: str, tls_cert: str, tls_key: str):
    url = urllib.parse.urlparse(redirect_uri); host, port, scheme = url.hostname, url.port, (url.scheme or "http")
    if not host or not port: raise SystemExit("YAHOO_REDIRECT_URI must include host and port, e.g., https://127.0.0.1:8910/callback")
    handler_cls = OAuthHandler; handler_cls._state = state
    httpd = http.server.HTTPServer((host, port), handler_cls)
    if scheme.lower() == "https":
        if not (tls_cert and tls_key): raise SystemExit("Redirect is HTTPS but TLS_CERT_FILE/TLS_KEY_FILE are not set in .env")
        if not (Path(tls_cert).exists() and Path(tls_key).exists()): raise SystemExit("TLS cert/key files not found. Check TLS_CERT_FILE and TLS_KEY_FILE paths.")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(certfile=tls_cert, keyfile=tls_key); httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    t = threading.Thread(target=httpd.serve_forever, daemon=True); t.start(); return httpd, t, handler_cls

def build_auth_url(client_id: str, redirect_uri: str, scope: str, state: str, prompt: str = "") -> str:
    params = {'response_type':'code','client_id':client_id,'redirect_uri':redirect_uri,'scope':scope,'state':state}
    if not scope:
        params.pop('scope')
    if prompt:
        params['prompt'] = prompt
    return f"{AUTH_URL}?{urllib.parse.urlencode(params)}"

def parse_code_from_url(url: str) -> Optional[str]:
    try: parsed = urllib.parse.urlparse(url); qs = urllib.parse.parse_qs(parsed.query); return qs.get("code", [None])[0]
    except Exception: return None

def exchange_code_for_token(cfg: Dict[str, str], code: str) -> Dict:
    headers = {"Authorization": f"Basic {build_basic_auth_header(cfg['client_id'], cfg['client_secret'])}", "Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "authorization_code", "redirect_uri": cfg["redirect_uri"], "code": code}
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=cfg["http_timeout"])
    if resp.status_code != 200: raise SystemExit(f"Token exchange failed: {resp.status_code} {resp.text}")
    p = resp.json(); tok = {"access_token": p["access_token"], "refresh_token": p.get("refresh_token"), "token_type": p.get("token_type", "bearer"), "scope": p.get("scope"), "expires_in": int(p.get("expires_in", 3600))}
    tok["expires_at"] = now_epoch() + tok["expires_in"] - 60; return tok

def fetch_userinfo(cfg: Dict[str, str], access_token: str):
    try: r = requests.get(USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"}, timeout=cfg["http_timeout"]); return r.json() if r.status_code==200 else None
    except Exception: return None

def random_state(n: int = 24) -> str: return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(n))

def main() -> int:
    parser = argparse.ArgumentParser(description="Yahoo OAuth/OIDC helper")
    parser.add_argument("--force-consent", action="store_true", help="Add prompt=consent to force re-consent")
    parser.add_argument("--manual", action="store_true", help="Manual paste mode; copy ?code= from the redirected URL")
    args = parser.parse_args()
    if args.force_consent:
        # override env prompt
        os.environ['OAUTH_PROMPT'] = 'consent'
    cfg = load_config(); manual = args.manual or cfg["manual_env"]; token_path = Path(cfg["token_file"])
    tok = read_token(token_path)
    if tok:
        if token_is_valid(tok): print(f"Token OK (expires: {human_time(tok['expires_at'], cfg['tz'])})."); return 0
        new_tok = refresh_token(cfg, tok); write_token(token_path, new_tok); print(f"Token refreshed (expires: {human_time(new_tok['expires_at'], cfg['tz'])})."); return 0
    state = random_state(); url = build_auth_url(cfg["client_id"], cfg["redirect_uri"], cfg["scope"], state, cfg.get("prompt",""))
    print("Open this URL to authorize:"); print(url)
    try: webbrowser.open(url)
    except Exception: pass
    if manual:
        print("\\nManual mode: after authorizing, copy the FULL redirected URL and paste it here.")
        pasted = input("Paste redirected URL: ").strip(); code = parse_code_from_url(pasted)
        if not code: raise SystemExit("Could not find ?code= in the pasted URL.")
        tok = exchange_code_for_token(cfg, code); write_token(token_path, tok); print(f"Token saved: {token_path}. Expires: {human_time(tok['expires_at'], cfg['tz'])}")
        ui = fetch_userinfo(cfg, tok["access_token"]); 
        if ui: print(f"User: {ui.get('sub')}  email: {ui.get('email')}"); 
        return 0
    else:
        httpd, thread, handler_cls = start_local_server(cfg["redirect_uri"], state, cfg["tls_cert"], cfg["tls_key"])
        try:
            print("Waiting for browser authorization...")
            for _ in range(600):
                if handler_cls._error: raise SystemExit(f"Authorization error: {handler_cls._error}")
                if handler_cls._code: code = handler_cls._code; break
                time.sleep(0.5)
            else: raise SystemExit("Timed out waiting for authorization callback.")
            tok = exchange_code_for_token(cfg, code); write_token(token_path, tok); print(f"Token saved: {token_path}. Expires: {human_time(tok['expires_at'], cfg['tz'])}")
            ui = fetch_userinfo(cfg, tok["access_token"]); 
            if ui: print(f"User: {ui.get('sub')}  email: {ui.get('email')}"); 
            return 0
        finally:
            httpd.shutdown(); httpd.server_close()

if __name__ == "__main__": raise SystemExit(main())
