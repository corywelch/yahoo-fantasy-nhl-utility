from typing import Any, Dict, List

class YahooLeagueAPI:
    """Thin wrapper around your `YahooFantasyClient` to keep imports localized."""

    def __init__(self, client: Any):
        self.client = client

    def league_meta(self, league_key: str) -> Dict[str, Any]:
        """Return league meta dict (league id, name, season, dates, etc.)."""
        return self.client.league_meta(league_key)

    def league_settings(self, league_key: str) -> Dict[str, Any]:
        """Return league settings dict (stat categories, modifiers, roster positions, tie-breakers)."""
        return self.client.league_settings(league_key)

    def league_teams(self, league_key: str) -> List[Dict[str, Any]]:
        """Return list of raw team dicts."""
        return self.client.league_teams(league_key)
