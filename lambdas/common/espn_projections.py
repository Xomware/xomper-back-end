"""
ESPN projections, already scored under a league's own settings.

The obvious approach — translate ESPN's `statId` scoring into Sleeper stat keys
and reuse `score_players` — does not work. Leagues score receptions under
different ids (`53` in one real league, `213` in another), scoring-side statId
semantics do not match the stats feed, and defensive scoring is unmapped
entirely. A partial table values a full-PPR league as standard and says nothing.
See docs/features/fantasy-draft-helper/SPIKE-espn-scoring.md.

Asking ESPN for players *in the league's context* sidesteps all of it:
`appliedTotal` is the projection with that league's real scoring already
applied. Exact by construction, and it covers defense.

The cost is that ESPN leagues are valued off ESPN's projections while Sleeper
leagues use the warehouse, so values are not comparable across platforms. That
is fine for drafting, which only ranks players against each other inside one
league.

Public leagues only. Private ones need the member's espn_s2/SWID cookies, and
storing a full ESPN session for a feature used twice a year was not worth the
liability — ESPN drafts are covered by manual mark-off instead (2026-08-29).
"""
import json
import urllib.request
from typing import Any

from lambdas.common.espn_crosswalk import ESPN_POSITIONS, USER_AGENT

LEAGUE_URL = (
    "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{season}"
    "/segments/0/leagues/{league_id}?view=kona_player_info"
)

# statSourceId: 0 is what a player actually did, 1 is the projection. Drafting
# wants the projection.
PROJECTION_SOURCE = 1
# statSplitTypeId 0 is the season total rather than a single week.
SEASON_SPLIT = 0


def fetch_league_players(
    league_id: str,
    season: str,
    limit: int = 1500,
) -> list[dict[str, Any]]:
    """Players with the league's own scoring applied. Public leagues only."""
    headers = {
        "User-Agent": USER_AGENT,
        "x-fantasy-filter": json.dumps({
            "players": {
                "limit": limit,
                "sortPercOwned": {"sortAsc": False, "sortPriority": 1},
            }
        }),
    }
    request = urllib.request.Request(
        LEAGUE_URL.format(season=season, league_id=league_id), headers=headers
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response).get("players") or []


def fetch_league_settings(league_id: str, season: str) -> dict[str, Any] | None:
    """The league's roster shape and size. No scoring — see the module docstring."""
    headers = {"User-Agent": USER_AGENT}

    url = LEAGUE_URL.format(season=season, league_id=league_id).replace(
        "view=kona_player_info", "view=mSettings"
    )
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def applied_total(player: dict[str, Any]) -> float | None:
    """The season projection for this player under the league's scoring."""
    for entry in player.get("stats") or []:
        if (
            entry.get("statSourceId") == PROJECTION_SOURCE
            and entry.get("statSplitTypeId") == SEASON_SPLIT
        ):
            total = entry.get("appliedTotal")
            if total is not None:
                return float(total)
    return None


def scored_rows(
    espn_players: list[dict[str, Any]],
    crosswalk: dict[str, dict[str, str]],
) -> dict[str, Any]:
    """(sleeperId, position, points) rows, plus what could not be resolved.

    `crosswalk` is the mapping from `espn_crosswalk.build_crosswalk`. Anything
    unresolved is reported rather than dropped: a missing player is a hole in
    the draft board, and silently shortening the list would hide it.
    """
    rows: list[tuple[str, str, float]] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()

    for entry in espn_players:
        player = entry.get("player") or entry
        position = ESPN_POSITIONS.get(player.get("defaultPositionId"))
        if not position:
            continue

        espn_id = str(player.get("id"))
        points = applied_total(player)
        name = player.get("fullName", "")

        mapped = crosswalk.get(espn_id)
        if not mapped:
            unresolved.append({"espnId": espn_id, "name": name, "reason": "no_crosswalk"})
            continue
        if points is None:
            unresolved.append({"espnId": espn_id, "name": name, "reason": "no_projection"})
            continue

        sleeper_id = mapped["sleeperId"]
        # ESPN can list the same underlying player twice across seasons of
        # eligibility. Keeping both would double-count them in the ranking.
        if sleeper_id in seen:
            continue
        seen.add(sleeper_id)
        rows.append((sleeper_id, position, points))

    return {"rows": rows, "unresolved": unresolved}
