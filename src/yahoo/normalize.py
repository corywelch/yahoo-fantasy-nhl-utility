from typing import Any, Dict, List

def normalize_league_info(meta: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    tiebreakers = settings.get("tiebreakers") or settings.get("tiebreaker_rules") or []
    roster_positions = settings.get("roster_positions") or []
    def _to_int(x):
        try:
            return int(x) if x is not None else None
        except Exception:
            return None
    return {
        "league_key": meta.get("league_key"),
        "league_id": _to_int(meta.get("league_id")),
        "name": meta.get("name"),
        "season": _to_int(meta.get("season")),
        "start_date": meta.get("start_date"),
        "end_date": meta.get("end_date"),
        "scoring_type": meta.get("scoring_type"),
        "draft_status": meta.get("draft_status"),
        "num_teams": _to_int(meta.get("num_teams")),
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
    out: List[Dict[str, Any]] = []
    for t in raw_teams or []:
        managers = t.get("managers")
        manager = {}
        if isinstance(managers, list) and managers:
            manager = managers[0]
        elif isinstance(t.get("manager"), dict):
            manager = t["manager"]
        logos = t.get("team_logos")
        logo_url = None
        if isinstance(logos, list) and logos:
            logo_url = logos[0].get("url")
        elif isinstance(t.get("logo"), str):
            logo_url = t["logo"]
        def _int(x):
            try:
                return int(x) if x is not None else None
            except Exception:
                return None
        out.append({
            "team_key": t.get("team_key"),
            "team_id": _int(t.get("team_id")),
            "name": t.get("name"),
            "manager": {
                "guid": manager.get("guid") if isinstance(manager, dict) else None,
                "nickname": manager.get("nickname") if isinstance(manager, dict) else None,
                "email": (manager.get("email") if isinstance(manager, dict) else None) or None,
            },
            "logo": logo_url,
            "division": t.get("division") or t.get("division_name"),
            "draft_position": _int(t.get("draft_position")),
            "waiver_priority": _int(t.get("waiver_priority")),
            "faab_balance": _int(t.get("faab_balance")),
            "moves": _int(t.get("number_of_moves") or t.get("moves")),
            "trades": _int(t.get("number_of_trades") or t.get("trades")),
            "clinched_playoffs": bool(t.get("clinched_playoffs")) if t.get("clinched_playoffs") is not None else None,
        })
    return out

def normalize_scoring(settings: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "stat_categories": settings.get("stat_categories") or [],
        "stat_modifiers": settings.get("stat_modifiers") or [],
        "roster_positions": settings.get("roster_positions") or [],
        "tiebreakers": settings.get("tiebreakers") or settings.get("tiebreaker_rules") or [],
    }
