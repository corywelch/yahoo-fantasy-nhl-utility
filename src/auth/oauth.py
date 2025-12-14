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
    """Load environment variables from .env file and system environment.

    Lightweight .env reader to avoid new dependencies.
    Rules:
      - Lines like KEY=VALUE (no quotes needed)
      - Ignores blanks and comments that start with '#'
      - Strips inline comments after '#' (e.g., VALUE  # note)
      - Removes optional surrounding quotes from values

    Returns:
        Dictionary containing combined environment variables
    """
    env = dict(os.environ)
    env_path = os.path.join(os.getcwd(), ".env")

    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key, value = key.strip(), value.strip()

                # Strip inline comments
                if "#" in value:
                    value = value.split("#", 1)[0].rstrip()

                # Remove optional surrounding quotes
                if (len(value) >= 2) and ((value[0] == value[-1]) and value[0] in ("'", '"')):
                    value = value[1:-1]

                env.setdefault(key, value)

    # Set defaults for missing environment variables
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
    """Ensure parent directory exists for the given path.

    Args:
        path: File path to check/create parent directory for
    """
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

def _atomic_write_json(path: str, data: dict) -> None:
    """Atomically write JSON data to file.

    Writes to temporary file first, then replaces original to avoid
    partial writes during crashes.

    Args:
        path: Destination file path
        data: JSON-serializable data to write
    """
    _ensure_parent(path)
    temp_path = f"{path}.tmp"

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)

    os.replace(temp_path, path)

def _read_json(path: str) -> Optional[dict]:
    """Read JSON data from file.

    Args:
        path: File path to read from

    Returns:
        Parsed JSON data as dictionary, or None if file doesn't exist
    """
    if not os.path.exists(path):
        return None

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ===================
# LOCAL HTTPS SERVER
# ===================
class _CallbackHandler(BaseHTTPRequestHandler):
    """OAuth callback handler for local HTTP server.

    Handles OAuth redirect callbacks and extracts authorization codes.
    """
    code: Optional[str] = None

    def do_GET(self) -> None:
        """Handle GET requests to the callback endpoint."""
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

    def log_message(self, fmt: str, *args) -> None:
        """Suppress logging unless debugging is enabled."""
        # Keep quiet unless debugging
        return

@contextmanager
def _run_local_server(
    host: str,
    port: int,
    scheme: str,
    cert_file: Optional[str],
    key_file: Optional[str]
) -> None:
    """Run local HTTP/HTTPS server for OAuth callback handling.

    Args:
        host: Hostname to bind to
        port: Port to listen on
        scheme: HTTP or HTTPS
        cert_file: Path to TLS certificate file (required for HTTPS)
        key_file: Path to TLS key file (required for HTTPS)
    """
    server = HTTPServer((host, port), _CallbackHandler)

    if scheme.lower() == "https":
        if not (cert_file and key_file):
            raise RuntimeError(
                "HTTPS redirect_uri requires TLS_CERT_FILE and TLS_KEY_FILE in .env"
            )
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
    """Get authenticated requests session with auto-refreshing OAuth2 tokens.

    Returns a requests.Session configured with Bearer authentication that
    automatically refreshes expired tokens. Handles both existing valid tokens
    and new authentication flows.

    Returns:
        Authenticated requests.Session with auto-refresh capability

    Raises:
        RuntimeError: If required OAuth credentials are missing
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
        raise RuntimeError(
            "Missing YAHOO_CLIENT_ID / YAHOO_CLIENT_SECRET / YAHOO_REDIRECT_URI "
            "in environment or .env"
        )

    def token_updater(token_data: dict) -> None:
        """Update token file with new token data."""
        _atomic_write_json(token_file, token_data)

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

    # Trigger authentication flow if no token or token is expired
    if not token or _is_expired(token):
        token = _obtain_token(
            oauth, client_id, client_secret, redirect_uri, scope,
            manual, prompt, tls_cert, tls_key, debug
        )
        token_updater(token)
        oauth.token = token

    # Create authenticated session
    session = Session()
    session.headers.update({"Authorization": f"Bearer {oauth.token['access_token']}"})

    def _response_hook(response, *args, **kwargs):
        """Handle 401 responses by refreshing token."""
        if response.status_code == 401:
            refreshed = oauth.refresh_token(
                YAHOO_TOKEN_URL,
                client_id=client_id,
                client_secret=client_secret,
            )
            token_updater(refreshed)
            session.headers.update({"Authorization": f"Bearer {refreshed['access_token']}"})
        return response

    session.hooks["response"].append(_response_hook)
    return session

# =================
# INTERNAL HELPERS
# =================
def _is_expired(token: dict, skew: int = 60) -> bool:
    """Check if OAuth token is expired.

    Args:
        token: OAuth token dictionary
        skew: Time skew in seconds to account for clock differences (default: 60)

    Returns:
        True if token is expired, False otherwise
    """
    now = time.time()

    if "expires_at" in token:
        return (token["expires_at"] - skew) <= now

    if "expires_in" in token:
        issued_at = token.get("_issued_at", now)
        return (issued_at + token["expires_in"] - skew) <= now

    # If no expiration info, consider expired
    return True

def _obtain_token(
    oauth: OAuth2Session,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    scope: str,
    manual: bool,
    prompt: str,
    tls_cert: Optional[str],
    tls_key: Optional[str],
    debug: bool,
) -> dict:
    """Obtain OAuth2 tokens using either manual or automatic flow.

    Handles both manual token entry (for headless environments) and
    automatic localhost callback flow.

    Args:
        oauth: Configured OAuth2Session instance
        client_id: Yahoo OAuth client ID
        client_secret: Yahoo OAuth client secret
        redirect_uri: OAuth redirect URI
        scope: Requested OAuth scopes
        manual: Whether to use manual token entry
        prompt: OAuth prompt parameter
        tls_cert: Path to TLS certificate for HTTPS
        tls_key: Path to TLS key for HTTPS
        debug: Whether to enable debug output

    Returns:
        Dictionary containing OAuth tokens

    Note:
        redirect_uri and scope are already bound on the OAuth2Session.
        Do NOT pass them again to authorization_url() or fetch_token(),
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
    """Parse URI to extract host, port, and scheme.

    Args:
        uri: URI string to parse

    Returns:
        Tuple of (host, port, scheme)
    """
    from urllib.parse import urlparse
    parsed_uri = urlparse(uri)
    port = parsed_uri.port or (443 if parsed_uri.scheme == "https" else 80)
    host = parsed_uri.hostname or "127.0.0.1"
    scheme = parsed_uri.scheme or "http"
    return host, port, scheme

def _stamp_issue_time(token: dict) -> None:
    """Stamp token with issue time.

    Args:
        token: Token dictionary to stamp
    """
    token["_issued_at"] = time.time()

# ===============
# CLI ENTRYPOINT
# ===============
def main(argv=None) -> int:
    """Main CLI entry point for OAuth testing.

    Args:
        argv: Command line arguments (optional)

    Returns:
        0 if successful, 1 if error, 2 if token validation failed
    """
    try:
        env = load_env()
        session = get_session()

        # Probe a cheap endpoint to confirm the token (users;use_login=1 is stable)
        response = session.get(
            "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1?format=json",
            timeout=30
        )

        ok = response.status_code == 200
        print(f"Token OK: {ok} (status={response.status_code})")

        if env.get("OAUTH_DEBUG", "0") == "1":
            print(f"Redirect URI (env): {env.get('YAHOO_REDIRECT_URI')}")

        return 0 if ok else 2

    except Exception as e:
        print(f"Auth error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
