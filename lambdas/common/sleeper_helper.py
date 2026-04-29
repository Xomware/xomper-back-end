"""
XOMPER Sleeper Helper
=====================
Synchronous wrappers for the Sleeper.app REST API.
"""

import requests
from typing import Any

from lambdas.common.errors import SleeperAPIError
from lambdas.common.logger import get_logger

log = get_logger(__file__)

SLEEPER_URL_BASE = "https://api.sleeper.app/v1"


def _get(url: str, description: str) -> Any:
    """
    Perform a GET request to the Sleeper API.

    Args:
        url: Full URL to fetch
        description: Human-readable description for error messages

    Returns:
        Parsed JSON response

    Raises:
        SleeperAPIError: On non-200 status or request failure
    """
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return response.json()
        raise SleeperAPIError(
            message=f"{description}: HTTP {response.status_code}",
            function="_get",
            endpoint=url,
        )
    except SleeperAPIError:
        raise
    except Exception as err:
        raise SleeperAPIError(
            message=f"{description}: {err}",
            function="_get",
            endpoint=url,
        ) from err


def fetch_nfl_players() -> dict[str, Any]:
    """Fetch all NFL players from Sleeper."""
    url = f"{SLEEPER_URL_BASE}/players/nfl"
    return _get(url, "Error fetching NFL players")


def get_sleeper_user(user_id: str) -> dict[str, Any]:
    """Get a Sleeper user by ID."""
    url = f"{SLEEPER_URL_BASE}/user/{user_id}"
    return _get(url, "Error getting Sleeper user")


def get_sleeper_league(league_id: str) -> dict[str, Any]:
    """Get a Sleeper league by ID."""
    url = f"{SLEEPER_URL_BASE}/league/{league_id}"
    return _get(url, "Error getting league")


def get_sleeper_league_rosters(league_id: str) -> list[dict[str, Any]]:
    """Get all rosters for a Sleeper league."""
    url = f"{SLEEPER_URL_BASE}/league/{league_id}/rosters"
    return _get(url, "Error getting league rosters")


def get_sleeper_league_users(league_id: str) -> list[dict[str, Any]]:
    """Get all users for a Sleeper league."""
    url = f"{SLEEPER_URL_BASE}/league/{league_id}/users"
    return _get(url, "Error getting league users")


def get_sleeper_league_matchups(league_id: str, week: int) -> list[dict[str, Any]]:
    """Get all matchup entries for a league + week. Each entry is one
    roster's score; pair by `matchup_id` to assemble head-to-head."""
    url = f"{SLEEPER_URL_BASE}/league/{league_id}/matchups/{week}"
    return _get(url, f"Error getting league matchups for week {week}")


def get_nfl_state() -> dict[str, Any]:
    """Get the current NFL state (season, week, type)."""
    url = f"{SLEEPER_URL_BASE}/state/nfl"
    return _get(url, "Error getting NFL state")
