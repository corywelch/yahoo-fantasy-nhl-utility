# src/auth/oauth.py
from __future__ import annotations
import base64, json, os, time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import requests

# Yahoo endpoints
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"

# Env config – same keys you already use
CLIENT_ID = os.getenv("YAHOO_CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("YAHOO_CLIENT_SECRET", "").strip()
REDIRECT_URI = os.getenv("YAHOO_REDIRECT_URI", "").strip()
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

# IMPORTANT: keep the same token path you already use (no subdirectory)
TOKEN_FILE = Path(os.getenv("TOKEN_FILE", "./data/yahoo_token.json")).expanduser()

class OAuthConfigError(RuntimeError): ...
class NotWiredOAuth(RuntimeError): ...

@dataclass
class TokenBundle:
    access_token: str
    refresh_token: Optional[str]
    expires_at: int  # epoch seconds

    @classmethod
    def from_dict(cls, d: Dict) -> "TokenBundle":
        # Your file stores expires_at; keep honoring that
        if "expires_at" in d:
            return cls(
                access_token=d["access_token"],
                refresh_token=d.get("refresh_token"),
                expires_at=int(d["expires_at"]),
            )
        # Fallback if only expires_in is present
        expires_in = int(d.get("expires_in", 3600))
        return cls(
            access_token=d["access_token"],
            refresh_token=d.get("refresh_token"),
            expires_at=int(time.time()) + expires_in - 60,
        )

    def to_dict(self) -> Dict:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at,
        }

def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("utf-8")

def _load_token() -> Optional[TokenBundle]:
    if TOKEN_FILE.exists():
        with TOKEN_FILE.open("r", encoding="utf-8") as f:
            return TokenBundle.from_dict(json.load(f))
    return None

def _save_token(tb: TokenBundle) -> None:
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = TOKEN_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(tb.to_dict(), f, ensure_ascii=False, indent=2)
    tmp.replace(TOKEN_FILE)

def _is_valid(tb: TokenBundle) -> bool:
    # match your “expires_at - now > 60” behavior
    return (tb.expires_at - int(time.time())) > 60

def _refresh(session: requests.Session, tb: TokenBundle) -> TokenBundle:
    if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
        raise OAuthConfigError(
            "Missing YAHOO_CLIENT_ID, YAHOO_CLIENT_SECRET, or YAHOO_REDIRECT_URI for refresh."
        )
    if not tb.refresh_token:
        raise OAuthConfigError("No refresh_token present; re-run your OAuth bootstrap.")

    headers = {
        "Authorization": _basic_auth_header(CLIENT_ID, CLIENT_SECRET),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "refresh_token",
        "redirect_uri": REDIRECT_URI,
        "refresh_token": tb.refresh_token,
    }
    resp = session.post(TOKEN_URL, headers=headers, data=data, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    p = resp.json()
    # Keep same fields as your file and recompute expires_at like you do
    expires_in = int(p.get("expires_in", 3600))
    new_tb = TokenBundle(
        access_token=p.get("access_token", tb.access_token),
        refresh_token=p.get("refresh_token", tb.refresh_token),
        expires_at=int(time.time()) + expires_in - 60,
    )
    _save_token(new_tb)
    return new_tb

def get_session() -> requests.Session:
    """
    Return a requests.Session with a valid Bearer token loaded from ./data/yahoo_token.json.
    If the file is missing, tell the user to run their existing OAuth helper once.
    """
    tb = _load_token()
    if tb is None:
        raise NotWiredOAuth(
            f"No token file at {TOKEN_FILE}. Run your existing OAuth bootstrap to create it "
            "(the same one you used before)."
        )

    sess = requests.Session()
    if not _is_valid(tb):
        tb = _refresh(sess, tb)

    sess.headers.update({"Authorization": f"Bearer {tb.access_token}"})
    return sess
