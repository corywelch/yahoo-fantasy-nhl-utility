"""Yahoo Fantasy NHL Utility - Core package.

This package provides utilities for working with Yahoo Fantasy NHL data,
including API clients, data normalization, and export functionality.
"""

# Package metadata
__version__ = "1.0.0"
__author__ = "Yahoo Fantasy NHL Utility Team"
__license__ = "MIT"
__all__ = [
    "auth",
    "config",
    "export",
    "io",
    "yahoo",
    "util_time",
]

# Import key modules for easy access
from .yahoo.api import YahooLeagueAPI
from .yahoo.client import YahooLeagueClient
from .yahoo.normalize import normalize_league_info, normalize_teams, normalize_scoring
from .config.env import get_export_dir, get_league_export_paths, LeagueExportPaths
from .util_time import RunTimestamps, make_run_timestamps

# Export key classes and functions at package level
__all__.extend([
    "YahooLeagueAPI",
    "YahooLeagueClient",
    "normalize_league_info",
    "normalize_teams",
    "normalize_scoring",
    "get_export_dir",
    "get_league_export_paths",
    "LeagueExportPaths",
    "RunTimestamps",
    "make_run_timestamps",
])
