from typing import Any, Dict, List

class YahooLeagueAPI:
    """Thin wrapper around YahooFantasyClient to keep imports localized."""

    def __init__(self, client: Any) -> None:
        """Initialize the Yahoo League API wrapper.

        Args:
            client: YahooFantasyClient instance for making API calls
        """
        self.client = client

    def league_meta(self, league_key: str) -> Dict[str, Any]:
        """Return league metadata dictionary.

        Contains league id, name, season, dates, and other basic information.

        Args:
            league_key: Yahoo league key in format 'game.l.id'

        Returns:
            Dictionary containing league metadata fields
        """
        return self.client.league_meta(league_key)

    def league_settings(self, league_key: str) -> Dict[str, Any]:
        """Return league settings dictionary.

        Contains stat categories, modifiers, roster positions, tie-breakers, etc.

        Args:
            league_key: Yahoo league key in format 'game.l.id'

        Returns:
            Dictionary containing league settings
        """
        return self.client.league_settings(league_key)

    def league_teams(self, league_key: str) -> List[Dict[str, Any]]:
        """Return list of raw team dictionaries.

        Args:
            league_key: Yahoo league key in format 'game.l.id'

        Returns:
            List of team dictionaries with raw Yahoo API data
        """
        return self.client.league_teams(league_key)
