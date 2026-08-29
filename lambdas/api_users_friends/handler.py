"""
API — The caller's friends
==========================
GET    /me/friends         -> friends, incoming requests, outgoing requests
                              ?suggest=1 also returns leaguemates to add
PUT    /me/friend-request  -> ask someone to be a friend
PUT    /me/friend-accept   -> accept an incoming request
DELETE /me/friend-remove   -> decline, cancel, or unfriend

Identity is the Cognito sub throughout. The Sleeper handle is never an actor
here: handle claims are unverified by design, so accepting one as a target
would let anyone befriend, or be befriended as, someone else.

Names come from `displayName` on the platform user record -- a name Xomper
owns, attached to a confirmed account. That is what this endpoint hydrates
into every row, so the client never has to resolve a person itself.
"""
from __future__ import annotations

from typing import Any

from lambdas.common import platform_users, sleeper_helper, social_store
from lambdas.common.caller_identity import get_caller
from lambdas.common.errors import ValidationError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import parse_body, success_response

HANDLER = "api_users_friends"
log = get_logger(HANDLER)

# Final path segment -> (action, method). The method is checked too, so a
# misconfigured route fails loudly instead of quietly doing the wrong thing
# to a relationship.
_ROUTES = {
    "friends": ("list", "GET"),
    "friend-request": ("request", "PUT"),
    "friend-accept": ("accept", "PUT"),
    "friend-remove": ("remove", "DELETE"),
}


def _route(event: dict[str, Any]) -> str:
    method = (event.get("httpMethod") or "GET").upper()
    path = event.get("path") or event.get("resource") or ""
    segment = path.rstrip("/").rsplit("/", 1)[-1]

    matched = _ROUTES.get(segment)
    if not matched or matched[1] != method:
        raise ValidationError(f"Unsupported route: {method} {path}")
    return matched[0]


def _target(event: dict[str, Any]) -> str:
    user_id = str(parse_body(event).get("userId") or "").strip()
    if not user_id:
        raise ValidationError("userId is required")
    return user_id


def _person(user_id: str) -> dict[str, str]:
    """The public view of another user.

    Deliberately thin: a name, a Sleeper handle for context, and the id. The
    email is not here -- a friend list is not a reason to hand out addresses.
    """
    record = platform_users.get_user(user_id) or {}
    return {
        "userId": user_id,
        "displayName": str(
            record.get("displayName") or record.get("sleeperUsername") or "Someone"
        ),
        "sleeperUsername": str(record.get("sleeperUsername") or ""),
        "sleeperAvatar": str(record.get("sleeperAvatar") or ""),
    }


def _suggestions(caller_id: str, known: set[str]) -> list[dict[str, Any]]:
    """Leaguemates with a Xomper account that the caller is not already tied to.

    People you share a league with are the only directory this app exposes.
    A global people-search would have to answer "does this Sleeper handle have
    a Xomper account", which leaks account existence for anyone whose handle
    is guessable -- and every Sleeper handle is public. Sharing a league is
    consent enough to be listed.
    """
    record = platform_users.get_user(caller_id) or {}
    sleeper_id = str(record.get("sleeperUserId") or "")
    if not sleeper_id:
        return []

    try:
        # Sleeper rolls the season over in spring, so this is not the
        # calendar year.
        season = str(sleeper_helper.get_nfl_state().get("season") or "")
        leagues = sleeper_helper.get_user_leagues(sleeper_id, season)
    except Exception:
        # A suggestion list is a nicety. Sleeper being down should not take
        # the friends page with it.
        log.warning("suggestions: could not load leagues", exc_info=True)
        return []

    leaguemates: dict[str, dict[str, Any]] = {}
    for league in leagues:
        league_id = str(league.get("league_id") or "")
        if not league_id:
            continue
        try:
            members = sleeper_helper.get_sleeper_league_users(league_id)
        except Exception:
            log.warning(f"suggestions: could not load users for {league_id}", exc_info=True)
            continue
        for member in members:
            member_id = str(member.get("user_id") or "")
            if member_id and member_id != sleeper_id:
                leaguemates.setdefault(member_id, member)

    claimed = platform_users.find_by_sleeper_ids(set(leaguemates))
    return sorted(
        (
            {
                "userId": str(record["userId"]),
                "displayName": str(
                    record.get("displayName") or record.get("sleeperUsername") or "Someone"
                ),
                "sleeperUsername": str(record.get("sleeperUsername") or ""),
                "sleeperAvatar": str(record.get("sleeperAvatar") or ""),
                "since": "",
            }
            for record in claimed.values()
            if str(record.get("userId") or "") not in known
        ),
        key=lambda person: person["displayName"].lower(),
    )


def _hydrate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {**_person(str(row["otherUserId"])), "since": str(row.get("updatedAt") or "")}
        for row in rows
        if row.get("otherUserId")
    ]


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    caller = get_caller(event)
    action = _route(event)

    if action == "request":
        social_store.request_friend(caller.user_id, _target(event))
    elif action == "accept":
        social_store.accept_friend(caller.user_id, _target(event))
    elif action == "remove":
        social_store.remove_friend(caller.user_id, _target(event))

    # Every path returns the whole graph, so one call both mutates and
    # re-syncs. The client never holds a list that disagrees with the server.
    rows = social_store.list_friends(caller.user_id)
    by_state: dict[str, list[dict[str, Any]]] = {"accepted": [], "incoming": [], "outgoing": []}
    for row in rows:
        by_state.setdefault(str(row.get("state") or ""), []).append(row)

    friends = _hydrate(by_state["accepted"])
    incoming = _hydrate(by_state["incoming"])
    outgoing = _hydrate(by_state["outgoing"])

    payload = {
        "friends": friends,
        "incoming": incoming,
        "outgoing": outgoing,
        # What the notification bell counts.
        "pendingCount": len(incoming),
        "suggestions": [],
    }

    # Opt-in, because it costs a scan and a fan-out over Sleeper. The friends
    # page asks for it; the auth guard, which loads this graph on every
    # protected navigation, does not.
    params = event.get("queryStringParameters") or {}
    if str(params.get("suggest") or "") == "1":
        known = {person["userId"] for person in friends + incoming + outgoing}
        payload["suggestions"] = _suggestions(caller.user_id, known)

    log.info(f"friends: {action} for {caller.user_id}")
    return success_response(payload)
