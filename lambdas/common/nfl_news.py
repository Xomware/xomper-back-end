"""
NFL News Fetcher for AI Reviews
===============================
Fetches trending players and injury news from Sleeper for inclusion
in AI-generated weekly recaps and previews.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.logger import get_logger
from lambdas.common.sleeper_helper import fetch_nfl_players, get_trending_players

log = get_logger(__file__)


def fetch_nfl_news(
    rostered_player_ids: set[str] | None = None,
    limit: int = 15,
) -> dict[str, Any]:
    """Fetch live NFL news for AI Reviews. Best-effort — never raises.

    Args:
        rostered_player_ids: Set of player IDs on league rosters (for injury filtering)
        limit: Max trending players per category

    Returns:
        Dict with trending_adds, trending_drops, injuries (or empty on failure)
    """
    try:
        # Fetch all players for enrichment
        all_players = fetch_nfl_players()

        # Get trending adds/drops
        trending_adds = get_trending_players("add", lookback_hours=24, limit=limit)
        trending_drops = get_trending_players("drop", lookback_hours=24, limit=limit)

        # Enrich with player details
        enriched_adds = _enrich_trending(trending_adds, all_players)
        enriched_drops = _enrich_trending(trending_drops, all_players)

        # Extract injuries from rostered players
        injuries = []
        if rostered_player_ids:
            for pid in rostered_player_ids:
                player = all_players.get(pid)
                if player and player.get("injury_status"):
                    injuries.append({
                        "player_id": pid,
                        "name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
                        "position": player.get("position", ""),
                        "team": player.get("team", ""),
                        "injury_status": player.get("injury_status"),
                        "injury_notes": player.get("injury_notes", ""),
                    })

        return {
            "trending_adds": enriched_adds,
            "trending_drops": enriched_drops,
            "injuries": injuries,
        }

    except Exception as e:
        log.warning(f"Failed to fetch NFL news (non-fatal): {e}")
        return {"trending_adds": [], "trending_drops": [], "injuries": []}


def _enrich_trending(
    trending: list[dict[str, Any]],
    all_players: dict[str, Any],
) -> list[dict[str, Any]]:
    """Add player name/position/team to trending entries."""
    enriched = []
    for entry in trending:
        pid = entry.get("player_id")
        player = all_players.get(pid, {})
        enriched.append({
            "player_id": pid,
            "count": entry.get("count", 0),
            "name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
            "position": player.get("position", ""),
            "team": player.get("team", ""),
            "injury_status": player.get("injury_status"),
        })
    return enriched


def format_news_for_prompt(news: dict[str, Any]) -> str:
    """Format NFL news dict into markdown for prompt injection."""
    if not news or not any(news.values()):
        return ""

    sections = []

    if news.get("trending_adds"):
        adds = news["trending_adds"][:10]
        lines = [f"- {p['name']} ({p['position']}, {p['team']})" for p in adds if p.get("name")]
        if lines:
            sections.append("**Trending Adds (last 24h):**\n" + "\n".join(lines))

    if news.get("trending_drops"):
        drops = news["trending_drops"][:10]
        lines = [f"- {p['name']} ({p['position']}, {p['team']})" for p in drops if p.get("name")]
        if lines:
            sections.append("**Trending Drops (last 24h):**\n" + "\n".join(lines))

    if news.get("injuries"):
        injured = [p for p in news["injuries"] if p.get("injury_status")]
        lines = [f"- {p['name']} ({p['position']}): {p['injury_status']}" for p in injured[:10]]
        if lines:
            sections.append("**Rostered Player Injuries:**\n" + "\n".join(lines))

    if not sections:
        return ""

    return "## LIVE NFL NEWS\n\n" + "\n\n".join(sections)
