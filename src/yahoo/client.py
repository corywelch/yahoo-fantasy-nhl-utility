# src/yahoo/client.py
from __future__ import annotations

import requests
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import xmltodict  # optional; only used if response is XML
except ImportError:
    xmltodict = None

from src.auth.oauth import get_session

# Constants
API_BASE = "https://fantasysports.yahooapis.com/fantasy/v2"

# Type aliases
Json = Union[Dict[str, Any], List[Any], str, int, float, bool, None]

def _fetch(url: str, session: requests.Session) -> Json:
    """Fetch data from Yahoo API with JSON/XML fallback handling.

    Args:
        url: URL to fetch data from
        session: Authenticated requests session

    Returns:
        Parsed JSON data or XML-to-dict converted data

    Raises:
        RuntimeError: If XML response received but xmltodict not available
        requests.exceptions.HTTPError: For HTTP errors
    """
    # Try JSON first
    response = session.get(url, headers={"Accept": "application/json"})

    # Check if server insists on XML response
    if (response.status_code == 406 or
        response.headers.get("Content-Type", "").lower().startswith("application/xml")):
        # Fallback to XML parsing if server insists
        if xmltodict is None:
            response.raise_for_status()
            raise RuntimeError(
                "Server returned XML but xmltodict is not installed. "
                "Install with: pip install xmltodict"
            )
        xml_text = response.text
        return xmltodict.parse(xml_text)

    response.raise_for_status()

    try:
        return response.json()
    except Exception:
        # Try XML parse if JSON parsing failed
        if xmltodict is None:
            raise
        return xmltodict.parse(response.text)

def _dig(obj: Json, *path) -> Any:
    """Safely navigate nested dictionary/list structure by keys/indices.

    Returns None if any path element is missing. Handles Yahoo's common patterns
    where arrays are stored as [{key: value}, {key: value}].

    Args:
        obj: JSON object to navigate (dict or list)
        *path: Sequence of keys/indices to traverse

    Returns:
        Value at the specified path, or None if path doesn't exist
    """
    current = obj
    for key in path:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list):
            # Yahoo often stores arrays as [{key: value}, {key: value}]
            try:
                index = int(key)  # allow numeric path parts for list indexing
                current = current[index]
            except (ValueError, IndexError):
                # If key is a string and each item is a {key:...} dict
                found = None
                for item in current:
                    if isinstance(item, dict) and key in item:
                        found = item[key]
                        break
                current = found
        else:
            return None

        if current is None:
            return None

    return current

def _extract_first(dictionary: Dict[str, Any], key: str) -> Any:
    """Extract first element from a list value, or the value itself.

    Args:
        dictionary: Dictionary to extract from
        key: Key to look up

    Returns:
        First element if value is a non-empty list, otherwise the value itself
    """
    value = dictionary.get(key)
    if isinstance(value, list) and value:
        return value[0]
    return value

def _normalize_league_dict(league_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize league dictionary to standard field set.

    Extracts common fields from Yahoo league payloads and returns
    a standardized dictionary with consistent field names.

    Args:
        league_dict: Raw league dictionary from Yahoo API

    Returns:
        Normalized dictionary with standard league metadata fields
    """
    # Common fields across Yahoo payloads
    # Many payloads look like: {"league_key": "465.l.22607", "name": "...", "season": "2015", ...}
    normalized = {}
    common_fields = [
        "league_key", "league_id", "name", "season",
        "start_date", "end_date", "scoring_type", "draft_status",
        "num_teams", "current_week", "start_week", "end_week", "is_private"
    ]

    for field in common_fields:
        if field in league_dict:
            normalized[field] = league_dict[field]

    return normalized

def _flatten_team_list(team_entry: Any) -> Dict[str, Any]:
    """Flatten team entry structure.

    Yahoo often returns team entries as lists of single-field dicts.
    This function flattens them into a single dictionary.

    Args:
        team_entry: Team entry data (list or dict)

    Returns:
        Flattened dictionary containing all team fields
    """
    flattened: Dict[str, Any] = {}

    if isinstance(team_entry, list):
        for item in team_entry:
            if isinstance(item, dict):
                flattened.update(item)
    elif isinstance(team_entry, dict):
        flattened.update(team_entry)

    return flattened

def _extract_from_json(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    """Extract meta, settings, and teams from Yahoo JSON payload.

    Handles both common Yahoo payload formats:
    - Case A: league as dict (rare)
    - Case B: league as list of one-key dicts (common)

    Args:
        payload: Raw JSON payload from Yahoo Fantasy API

    Returns:
        Tuple of (meta_dict, settings_dict, teams_list)
    """
    fantasy_content = payload.get("fantasy_content")
    if not isinstance(fantasy_content, dict):
        return {}, {}, []

    league = fantasy_content.get("league")
    meta: Dict[str, Any] = {}
    settings: Dict[str, Any] = {}
    teams_list: List[Dict[str, Any]] = []

    # Common metadata fields to extract
    meta_fields = [
        "league_key", "league_id", "name", "season",
        "start_date", "end_date", "scoring_type",
        "draft_status", "num_teams", "current_week",
        "start_week", "end_week", "is_private"
    ]

    # Case A: league is a dict (rare in my experience)
    if isinstance(league, dict):
        # Sometimes meta fields live right in the league dict
        for field in meta_fields:
            if field in league:
                meta[field] = league[field]

        # Extract settings if present
        league_settings = league.get("settings")
        if isinstance(league_settings, dict):
            settings = league_settings

        # Extract teams if present
        teams_container = league.get("teams")
        if isinstance(teams_container, dict):
            for value in teams_container.values():
                if isinstance(value, dict) and "team" in value:
                    teams_list.append(_flatten_team_list(value["team"]))

        return meta, settings, teams_list

    # Case B: league is a list of one-key dicts (common)
    if isinstance(league, list):
        for entry in league:
            if not isinstance(entry, dict):
                continue

            # Meta candidates: dicts that contain common meta fields
            if not meta and any(field in entry for field in ("league_key", "name", "season", "num_teams")):
                for field in meta_fields:
                    if field in entry:
                        meta[field] = entry[field]

            # Settings node
            if "settings" in entry and isinstance(entry["settings"], dict):
                settings = entry["settings"]

            # Teams node
            if "teams" in entry:
                teams_container = entry["teams"]
                if isinstance(teams_container, dict):
                    for value in teams_container.values():
                        if isinstance(value, dict) and "team" in value:
                            teams_list.append(_flatten_team_list(value["team"]))
                elif isinstance(teams_container, list):
                    for item in teams_container:
                        if isinstance(item, dict) and "team" in item:
                            teams_list.append(_flatten_team_list(item["team"]))

        return meta, settings, teams_list

    # Fallback for unrecognized formats
    return {}, {}, []
class YahooLeagueClient:
    """Client with best-effort extraction for Yahoo Fantasy JSON/XML."""

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        """Initialize Yahoo League Client.

        Args:
            session: Optional authenticated requests session.
                    If None, creates a new session using OAuth.
        """
        self.session = session or get_session()

    def league_meta(self, league_key: str) -> Dict[str, Any]:
        """Fetch league metadata from Yahoo API.

        Args:
            league_key: Yahoo league key in format 'game.l.id'

        Returns:
            Dictionary containing league metadata, or empty dict if failed
        """
        url = f"{API_BASE}/league/{league_key}/metadata?format=json"
        payload = _fetch(url, self.session)

        if isinstance(payload, dict):
            meta, _, _ = _extract_from_json(payload)
            return meta or payload

        return {}

    def league_settings(self, league_key: str) -> Dict[str, Any]:
        """Fetch league settings from Yahoo API.

        Args:
            league_key: Yahoo league key in format 'game.l.id'

        Returns:
            Dictionary containing league settings, or empty dict if failed
        """
        url = f"{API_BASE}/league/{league_key}/settings?format=json"
        payload = _fetch(url, self.session)

        if isinstance(payload, dict):
            _, settings, _ = _extract_from_json(payload)
            return settings or payload

        return {}

    def league_teams(self, league_key: str) -> List[Dict[str, Any]]:
        """Fetch league teams from Yahoo API.

        Args:
            league_key: Yahoo league key in format 'game.l.id'

        Returns:
            List of team dictionaries, or empty list if failed
        """
        url = f"{API_BASE}/league/{league_key}/teams?format=json"
        payload = _fetch(url, self.session)

        if isinstance(payload, dict):
            _, _, teams = _extract_from_json(payload)
            return teams or []

        if isinstance(payload, list):
            return payload

        return []
