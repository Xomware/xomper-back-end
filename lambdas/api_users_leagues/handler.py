"""
API — The caller's leagues
==========================
GET    /me/leagues   -> every league the caller is in, with follow state
PUT    /me/follow    -> follow one league
DELETE /me/unfollow  -> unfollow one league

This is what makes Xomper a platform rather than a single-league app. The
frontend used to read one hardcoded league id from its environment, so every
user saw the same league regardless of which ones they were actually in.

The league list comes from Sleeper live rather than from storage: membership
changes without telling us, and a stale list is worse than a slow one. What
*is* stored is the follow set, because Sleeper has no concept of it.
"""
from __future__ import annotations

from typing import Any

from lambdas.common import platform_follows, platform_users
from lambdas.common.caller_identity import get_caller
from lambdas.common.errors import ValidationError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.sleeper_helper import get_nfl_state, get_user_leagues
from lambdas.common.utility_helpers import parse_body, success_response

HANDLER = "api_users_leagues"
log = get_logger(HANDLER)

# Flat paths: api-gateway-service keys one API Gateway resource per endpoint
# on `path_part`, so two methods on one part collide at apply time.
def _current_season() -> str:
    """The season Sleeper considers current.

    Not the calendar year: Sleeper rolls the NFL season over in spring, so
    deriving it from the clock serves the wrong year for months.
    """
    state = get_nfl_state() or {}
    return str(state.get("season") or "")


_ROUTES = {
    "leagues": ("list", "GET"),
    "follow": ("follow", "PUT"),
    "unfollow": ("unfollow", "DELETE"),
}


def _route(event: dict[str, Any]) -> str:
    method = (event.get("httpMethod") or "GET").upper()
    path = event.get("path") or event.get("resource") or ""
    segment = path.rstrip("/").rsplit("/", 1)[-1]

    matched = _ROUTES.get(segment)
    if not matched or matched[1] != method:
        raise ValidationError(f"Unsupported route: {method} {path}")
    return matched[0]


def _shape(league: dict[str, Any], followed: set[str]) -> dict[str, Any]:
    """Public view of a Sleeper league.

    An explicit field list, not a passthrough: Sleeper's league object is
    large and mostly irrelevant here, and the frontend re-fetches the full
    thing for whichever league it actually opens.

    `isDynasty` is derived from `settings.type` (2 = dynasty) because the
    value provider routes on it — dynasty leagues price off FantasyCalc,
    redraft off projections.
    """
    settings = league.get("settings") or {}
    league_id = str(league.get("league_id") or "")
    return {
        "leagueId": league_id,
        "name": league.get("name") or "",
        "season": str(league.get("season") or ""),
        "status": league.get("status") or "",
        "totalRosters": league.get("total_rosters") or 0,
        "avatar": league.get("avatar") or "",
        "isDynasty": settings.get("type") == 2,
        "isFollowed": league_id in followed,
    }


def _league_id_from(event: dict[str, Any]) -> str:
    league_id = str(parse_body(event).get("leagueId") or "").strip()
    if not league_id:
        raise ValidationError("leagueId is required")
    return league_id


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    caller = get_caller(event)
    action = _route(event)

    if action == "follow":
        body = parse_body(event)
        platform_follows.follow(
            caller.user_id,
            _league_id_from(event),
            name=str(body.get("name") or ""),
            season=str(body.get("season") or ""),
        )
    elif action == "unfollow":
        platform_follows.unfollow(caller.user_id, _league_id_from(event))

    profile = platform_users.get_user(caller.user_id) or {}
    sleeper_user_id = str(profile.get("sleeperUserId") or "")

    if not sleeper_user_id:
        # No linked Sleeper account means no leagues to list. An empty list
        # rather than an error: the frontend guard already routes unlinked
        # users to the link page, and this endpoint should not be a second
        # place that decides what "not linked yet" means.
        return success_response({"season": _current_season(), "count": 0, "leagues": []})

    season = _current_season()
    followed = platform_follows.followed_league_ids(caller.user_id)
    leagues = [_shape(l, followed) for l in get_user_leagues(sleeper_user_id, season)]

    # Followed first, then in-season before pre-draft, then by name — the
    # order the frontend renders a league switcher in.
    status_rank = {"in_season": 0, "drafting": 1, "post_season": 2, "pre_draft": 3}
    leagues.sort(
        key=lambda l: (
            not l["isFollowed"],
            status_rank.get(l["status"], 9),
            l["name"].lower(),
        )
    )

    log.info(f"leagues: {action} for {caller.user_id} -> {len(leagues)} leagues")
    return success_response({"season": season, "count": len(leagues), "leagues": leagues})
