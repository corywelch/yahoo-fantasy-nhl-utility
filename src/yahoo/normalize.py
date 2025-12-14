from typing import Any, Dict, List, Optional

def _safe_convert_to_int(value: Any) -> Optional[int]:
    """Safely convert value to integer.

    Args:
        value: Value to convert to integer

    Returns:
        Integer value if conversion successful, None otherwise
    """
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None

def _safe_convert_to_bool(value: Any) -> Optional[bool]:
    """Safely convert value to boolean.

    Args:
        value: Value to convert to boolean

    Returns:
        Boolean value if conversion successful, None otherwise
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    try:
        return bool(value)
    except (ValueError, TypeError):
        return None

def normalize_league_info(meta: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize league information from Yahoo API metadata and settings.

    Extracts and standardizes league information from raw Yahoo API
    metadata and settings dictionaries.

    Args:
        meta: Raw league metadata dictionary from Yahoo API
        settings: Raw league settings dictionary from Yahoo API

    Returns:
        Normalized dictionary containing standardized league information
    """
    # Get tiebreakers from either field (Yahoo uses different field names)
    tiebreakers = (
        settings.get("tiebreakers") or
        settings.get("tiebreaker_rules") or
        []
    )

    # Get roster positions with fallback to empty list
    roster_positions = settings.get("roster_positions") or []

    return {
        "league_key": meta.get("league_key"),
        "league_id": _safe_convert_to_int(meta.get("league_id")),
        "name": meta.get("name"),
        "season": _safe_convert_to_int(meta.get("season")),
        "start_date": meta.get("start_date"),
        "end_date": meta.get("end_date"),
        "scoring_type": meta.get("scoring_type"),
        "draft_status": meta.get("draft_status"),
        "num_teams": _safe_convert_to_int(meta.get("num_teams")),
        "current_week": meta.get("current_week"),
        "start_week": meta.get("start_week"),
        "end_week": meta.get("end_week"),
        "is_private": meta.get("is_private"),
        "allow_draft_trades": settings.get("allow_draft_trades"),
        "waiver_type": settings.get("waiver_type"),
        "waiver_budget": settings.get("waiver_budget"),
        "trade_end_date": settings.get("trade_end_date"),
        "tiebreakers": tiebreakers,
        "roster_positions": roster_positions,
    }

def normalize_teams(raw_teams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize raw team data from Yahoo API.

    Processes raw team dictionaries from Yahoo API into a standardized format
    with consistent field names and data types.

    Args:
        raw_teams: List of raw team dictionaries from Yahoo API

    Returns:
        List of normalized team dictionaries with standardized structure
    """
    normalized_teams: List[Dict[str, Any]] = []

    for team_data in raw_teams or []:
        # Extract manager information
        managers = team_data.get("managers")
        manager: Dict[str, Any] = {}

        if isinstance(managers, list) and managers:
            manager = managers[0]
        elif isinstance(team_data.get("manager"), dict):
            manager = team_data["manager"]

        # Extract logo URL
        logos = team_data.get("team_logos")
        logo_url = None

        if isinstance(logos, list) and logos:
            logo_url = logos[0].get("url")
        elif isinstance(team_data.get("logo"), str):
            logo_url = team_data["logo"]

        normalized_teams.append({
            "team_key": team_data.get("team_key"),
            "team_id": _safe_convert_to_int(team_data.get("team_id")),
            "name": team_data.get("name"),
            "manager": {
                "guid": manager.get("guid") if isinstance(manager, dict) else None,
                "nickname": manager.get("nickname") if isinstance(manager, dict) else None,
                "email": (manager.get("email") if isinstance(manager, dict) else None) or None,
            },
            "logo": logo_url,
            "division": team_data.get("division") or team_data.get("division_name"),
            "draft_position": _safe_convert_to_int(team_data.get("draft_position")),
            "waiver_priority": _safe_convert_to_int(team_data.get("waiver_priority")),
            "faab_balance": _safe_convert_to_int(team_data.get("faab_balance")),
            "moves": _safe_convert_to_int(team_data.get("number_of_moves") or team_data.get("moves")),
            "trades": _safe_convert_to_int(team_data.get("number_of_trades") or team_data.get("trades")),
            "clinched_playoffs": _safe_convert_to_bool(team_data.get("clinched_playoffs")),
        })

    return normalized_teams

def normalize_scoring(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize scoring settings from Yahoo API.

    Extracts and standardizes scoring-related information from
    Yahoo API settings dictionary.

    Args:
        settings: Raw league settings dictionary from Yahoo API

    Returns:
        Normalized dictionary containing standardized scoring information
    """
    return {
        "stat_categories": settings.get("stat_categories") or [],
        "stat_modifiers": settings.get("stat_modifiers") or [],
        "roster_positions": settings.get("roster_positions") or [],
        "tiebreakers": (
            settings.get("tiebreakers") or
            settings.get("tiebreaker_rules") or
            []
        ),
    }
