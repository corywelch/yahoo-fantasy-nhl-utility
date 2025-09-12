# src/auth/oauth.py
from __future__ import annotations

import json
import os
import sys
import time
import threading
import ssl
from contextlib import contextmanager
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Optional
import webbrowser

from requests import Session
from requests_oauthlib import OAuth2Session

# ==========
# ENV LOADER
# ==========
def load_env() -> Dict[str, str]:
    """
    Lightweight .env reader to avoid new dependencies.
    Rules:
      - Lines like KEY=VALUE (no quotes needed)
      - Ignores blanks and comments that start with '#'
      - Strips inline comments after '#' (e.g., VALUE  # note)
    """
    env = dict(os.environ)
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip()
                # strip inline comments
                if "#" in v:
                    v = v.split("#", 1)[0].rstrip()
                # remove optional surrounding quotes
                if (len(v) >= 2) and ((v[0] == v[-1]) and v[0] in ("'", '"')):
                    v = v[1:-1]
                env.setdefault(k, v)
    # defaults
    env.setdefault("TLS_CERT_FILE", "./certs/localhost.pem")
    env.setdefault("TLS_KEY_FILE", "./certs/localhost-key.pem")
    env.setdefault("OAUTH_PROMPT", "")
    env.setdefault("OAUTH_MANUAL", env.get("OAUTH_MANUAL", "0"))
    env.setdefault("OAUTH_DEBUG", env.get("OAUTH_DEBUG", "0"))
    return env


# ==========
# CONSTANTS
# ==========
YAHOO_AUTH_BASE = "https://api.login.yahoo.com/oauth2"
YAHOO_TOKEN_URL = f"{YAHOO_AUTH_BASE}/get_token"
YAHOO_AUTH_URL = f"{YAHOO_AUTH_BASE}/request_auth"

DEFAULT_SCOPE = "openid fspt-r"
DEFAULT_TOKEN_FILE = "./data/yahoo_token.json"

# ================
# FILE IO HELPERS
# ================
def _ensure_parent(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

def _atomic_write_json(path: str, data: dict) -> None:
    _ensure_parent(path)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)

def _read_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ===================
# LOCAL HTTPS SERVER
# ===================
class _CallbackHandler(BaseHTTPRequestHandler):
    code: Optional[str] = None

    def do_GET(self):
        # Expect /callback?code=...&state=...
        if self.path.startswith("/callback"):
            # Parse code minimally
            from urllib.parse import urlparse, parse_qs
            query = parse_qs(urlparse(self.path).query)
            _CallbackHandler.code = query.get("code", [None])[0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Auth complete. You may close this tab.")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        # keep quiet unless debugging
        return

@contextmanager
def _run_local_server(host: str, port: int, scheme: str, cert_file: str | None, key_file: str | None):
    server = HTTPServer((host, port), _CallbackHandler)
    if scheme.lower() == "https":
        if not (cert_file and key_file):
            raise RuntimeError("HTTPS redirect_uri requires TLS_CERT_FILE and TLS_KEY_FILE in .env")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile=cert_file, keyfile=key_file)
        server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield
    finally:
        server.shutdown()
        thread.join()

# ====================
# PUBLIC ENTRY POINTS
# ====================
def get_session() -> Session:
    """
    Return a requests.Session with Bearer auth that auto-refreshes tokens.
    - Reads existing token from TOKEN_FILE; if missing/expired, triggers flow.
    - On refresh, writes updated token back to TOKEN_FILE.
    """
    env = load_env()
    client_id = env.get("YAHOO_CLIENT_ID", "").strip()
    client_secret = env.get("YAHOO_CLIENT_SECRET", "").strip()
    redirect_uri = env.get("YAHOO_REDIRECT_URI", "").strip()
    scope = (env.get("YAHOO_SCOPE") or DEFAULT_SCOPE).strip()
    token_file = env.get("TOKEN_FILE", DEFAULT_TOKEN_FILE).strip()
    manual = (env.get("OAUTH_MANUAL", "0").strip() == "1")
    prompt = env.get("OAUTH_PROMPT", "").strip()
    tls_cert = env.get("TLS_CERT_FILE", "").strip()
    tls_key = env.get("TLS_KEY_FILE", "").strip()
    debug = env.get("OAUTH_DEBUG", "0").strip() == "1"

    if not client_id or not client_secret or not redirect_uri:
        raise RuntimeError("Missing YAHOO_CLIENT_ID / YAHOO_CLIENT_SECRET / YAHOO_REDIRECT_URI in environment or .env")

    def token_updater(t: dict):
        _atomic_write_json(token_file, t)

    token = _read_json(token_file)

    # NOTE: Do NOT set redirect_uri/scope on the session.
    # We'll pass them explicitly (mirrors working standalone flow).
    oauth = OAuth2Session(
        client_id=client_id,
        redirect_uri=redirect_uri,          # bind here
        scope=scope.split(),                # bind here
        token=token,
        auto_refresh_url=YAHOO_TOKEN_URL,
        auto_refresh_kwargs={"client_id": client_id, "client_secret": client_secret},
        token_updater=token_updater if token else None,
    )


    if not token or _is_expired(token):
        token = _obtain_token(
            oauth, client_id, client_secret, redirect_uri, scope, manual, prompt, tls_cert, tls_key, debug
        )
        token_updater(token)
        oauth.token = token

    sess = Session()
    sess.headers.update({"Authorization": f"Bearer {oauth.token['access_token']}"})

    def _response_hook(r, *args, **kwargs):
        if r.status_code == 401:
            refreshed = oauth.refresh_token(
                YAHOO_TOKEN_URL,
                client_id=client_id,
                client_secret=client_secret,
            )
            token_updater(refreshed)
            sess.headers.update({"Authorization": f"Bearer {refreshed['access_token']}"})
        return r

    sess.hooks["response"].append(_response_hook)
    return sess

# =================
# INTERNAL HELPERS
# =================
def _is_expired(token: dict, skew: int = 60) -> bool:
    now = time.time()
    if "expires_at" in token:
        return (token["expires_at"] - skew) <= now
    if "expires_in" in token:
        return (token.get("_issued_at", now) + token["expires_in"] - skew) <= now
    return True

def _obtain_token(
    oauth: OAuth2Session,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    scope: str,
    manual: bool,
    prompt: str,
    tls_cert: str | None,
    tls_key: str | None,
    debug: bool,
) -> dict:
    """
    Acquire initial tokens (automatic localhost HTTP/HTTPS if manual=False, else manual paste).
    NOTE:
      - redirect_uri and scope are already bound on the OAuth2Session.
      - DO NOT pass them again to authorization_url(...) or fetch_token(...),
        or oauthlib will see duplicate values and throw.
    """
    # Only optional extras (e.g., prompt) go in kwargs
    kwargs = {}
    if prompt:
        kwargs["prompt"] = prompt

    # Build authorize URL; NO redirect_uri/scope here (already bound on session)
    auth_url, state = oauth.authorization_url(YAHOO_AUTH_URL, **kwargs)
    if debug:
        print(f"[oauth] authorize URL: {auth_url}")

    if manual:
        print("Open this URL, authorize, and paste the full redirected URL:")
        print(auth_url)
        redirected = input("Redirected URL: ").strip()
        from urllib.parse import urlparse, parse_qs
        code = parse_qs(urlparse(redirected).query).get("code", [None])[0]
        if not code:
            raise RuntimeError("No 'code' in redirected URL")
        # Exchange code; DO NOT pass redirect_uri here (session already has it)
        token = oauth.fetch_token(
            token_url=YAHOO_TOKEN_URL,
            code=code,
            client_secret=client_secret,
            include_client_id=True,
        )
        _stamp_issue_time(token)
        return token

    # Automatic localhost callback (HTTP or HTTPS)
    host, port, scheme = _host_port_scheme_from_uri(redirect_uri)
    _CallbackHandler.code = None
    with _run_local_server(host, port, scheme, tls_cert, tls_key):
        webbrowser.open(auth_url, new=1, autoraise=True)
        deadline = time.time() + 300
        while time.time() < deadline and _CallbackHandler.code is None:
            time.sleep(0.2)
    if not _CallbackHandler.code:
        raise RuntimeError("Did not receive authorization code on local callback")

    # Token exchange; DO NOT pass redirect_uri here
    token = oauth.fetch_token(
        token_url=YAHOO_TOKEN_URL,
        code=_CallbackHandler.code,
        client_secret=client_secret,
        include_client_id=True,
    )
    _stamp_issue_time(token)
    return token



def _host_port_scheme_from_uri(uri: str) -> tuple[str, int, str]:
    from urllib.parse import urlparse
    u = urlparse(uri)
    port = u.port or (443 if u.scheme == "https" else 80)
    host = u.hostname or "127.0.0.1"
    scheme = u.scheme or "http"
    return host, port, scheme

def _stamp_issue_time(token: dict) -> None:
    token["_issued_at"] = time.time()

# ===============
# CLI ENTRYPOINT
# ===============
def main(argv=None) -> int:
    try:
        env = load_env()
        sess = get_session()
        # Probe a cheap endpoint to confirm the token (users;use_login=1 is stable)
        resp = sess.get("https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1?format=json", timeout=30)
        ok = resp.status_code == 200
        print(f"Token OK: {ok} (status={resp.status_code})")
        if env.get("OAUTH_DEBUG","0") == "1":
            print(f"Redirect URI (env): {env.get('YAHOO_REDIRECT_URI')}")
        return 0 if ok else 2
    except Exception as e:
        print(f"Auth error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
