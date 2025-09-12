# src/yahoo/client.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Union
import requests
try:
    import xmltodict  # optional; only used if response is XML
except Exception:
    xmltodict = None

from src.auth.oauth import get_session

API_BASE = "https://fantasysports.yahooapis.com/fantasy/v2"

Json = Union[Dict[str, Any], List[Any], str, int, float, bool, None]

def _fetch(url: str, session: requests.Session) -> Json:
    # Try JSON first
    resp = session.get(url, headers={"Accept": "application/json"})
    if resp.status_code == 406 or resp.headers.get("Content-Type", "").lower().startswith("application/xml"):
        # Fallback to XML if server insists
        if xmltodict is None:
            resp.raise_for_status()
            raise RuntimeError("Server returned XML but xmltodict is not installed. pip install xmltodict")
        xml_text = resp.text
        return xmltodict.parse(xml_text)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        # Try XML parse if JSON failed
        if xmltodict is None:
            raise
        return xmltodict.parse(resp.text)

def _dig(obj: Json, *path) -> Any:
    """Safely walk nested dict/list by keys/indices; returns None if missing."""
    cur = obj
    for key in path:
        if isinstance(cur, dict):
            cur = cur.get(key)
        elif isinstance(cur, list):
            # Yahoo often stores arrays as [ {key: value}, {key: value} ]
            try:
                idx = int(key)  # allow numeric path parts
                cur = cur[idx]
            except Exception:
                # if key is a string and each item is a {key:...}
                found = None
                for item in cur:
                    if isinstance(item, dict) and key in item:
                        found = item[key]
                        break
                cur = found
        else:
            return None
        if cur is None:
            return None
    return cur

def _extract_first(d: Dict[str, Any], key: str) -> Any:
    v = d.get(key)
    if isinstance(v, list) and v:
        return v[0]
    return v

def _normalize_league_dict(ld: Dict[str, Any]) -> Dict[str, Any]:
    # Try common fields across Yahoo payloads
    # Many payloads look like: {"league_key": "465.l.22607", "name": "...", "season": "2015", ...}
    out = {}
    for k in ["league_key","league_id","name","season","start_date","end_date","scoring_type","draft_status",
              "num_teams","current_week","start_week","end_week","is_private"]:
        if k in ld:
            out[k] = ld[k]
    return out

def _flatten_team_list(team_entry: Any) -> Dict[str, Any]:
    """team entry is often a list of single-field dicts; flatten them."""
    flat: Dict[str, Any] = {}
    if isinstance(team_entry, list):
        for item in team_entry:
            if isinstance(item, dict):
                flat.update(item)
    elif isinstance(team_entry, dict):
        flat.update(team_entry)
    return flat

def _extract_from_json(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    """Return (meta, settings, teams) from Yahoo's JSON."""
    fc = payload.get("fantasy_content")
    if not isinstance(fc, dict):
        return {}, {}, []

    league = fc.get("league")
    meta: Dict[str, Any] = {}
    settings: Dict[str, Any] = {}
    teams_list: List[Dict[str, Any]] = []

    # Case A: league is a dict (rare in my experience)
    if isinstance(league, dict):
        # sometimes meta fields live right here
        for k in ("league_key","league_id","name","season","start_date","end_date","scoring_type",
                  "draft_status","num_teams","current_week","start_week","end_week","is_private"):
            if k in league: meta[k] = league[k]
        if isinstance(league.get("settings"), dict):
            settings = league["settings"]
        t = league.get("teams")
        if isinstance(t, dict):
            for v in t.values():
                if isinstance(v, dict) and "team" in v:
                    teams_list.append(_flatten_team_list(v["team"]))
        return meta, settings, teams_list

    # Case B: league is a list of one-key dicts (common)
    if isinstance(league, list):
        for entry in league:
            if not isinstance(entry, dict):
                continue
            # meta candidates: dicts that contain common meta fields
            if not meta and any(k in entry for k in ("league_key","name","season","num_teams")):
                for k in ("league_key","league_id","name","season","start_date","end_date","scoring_type",
                          "draft_status","num_teams","current_week","start_week","end_week","is_private"):
                    if k in entry: meta[k] = entry[k]
            # settings node
            if "settings" in entry and isinstance(entry["settings"], dict):
                settings = entry["settings"]
            # teams node
            if "teams" in entry:
                t = entry["teams"]
                if isinstance(t, dict):
                    for v in t.values():
                        if isinstance(v, dict) and "team" in v:
                            teams_list.append(_flatten_team_list(v["team"]))
                elif isinstance(t, list):
                    for item in t:
                        if isinstance(item, dict) and "team" in item:
                            teams_list.append(_flatten_team_list(item["team"]))
        return meta, settings, teams_list

    # Fallback
    return {}, {}, []
class YahooLeagueClient:
    """Client with best-effort extraction for Yahoo Fantasy JSON/XML."""
    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or get_session()

    def league_meta(self, league_key: str) -> Dict[str, Any]:
        url = f"{API_BASE}/league/{league_key}/metadata?format=json"
        payload = _fetch(url, self.session)
        if isinstance(payload, dict):
            meta, _, _ = _extract_from_json(payload)
            return meta or payload
        return {}

    def league_settings(self, league_key: str) -> Dict[str, Any]:
        url = f"{API_BASE}/league/{league_key}/settings?format=json"
        payload = _fetch(url, self.session)
        if isinstance(payload, dict):
            _, settings, _ = _extract_from_json(payload)
            return settings or payload
        return {}

    def league_teams(self, league_key: str) -> List[Dict[str, Any]]:
        url = f"{API_BASE}/league/{league_key}/teams?format=json"
        payload = _fetch(url, self.session)
        if isinstance(payload, dict):
            _, _, teams = _extract_from_json(payload)
            return teams or []
        if isinstance(payload, list):
            return payload
        return []
