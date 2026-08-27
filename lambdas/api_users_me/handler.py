"""
API — Current user
==================
GET    /me/profile         -> the caller's platform record
PUT    /me/sleeper-link    -> link a Sleeper account
DELETE /me/sleeper-unlink  -> unlink it

One Lambda for the whole `/me` surface. These share the same record, the same
identity read and the same response shape, and splitting them across three
functions would mean three cold starts and three sets of IAM to keep in
agreement for what is one small table.

The paths are flat rather than the more natural `/me/sleeper` with the method
carrying the verb: the api-gateway-service module keys one API Gateway
resource per endpoint on `path_part`, so two methods sharing a part would
collide at apply time.

Replaces the Supabase `profiles` read that the frontend did directly against
the database. The link step now resolves the username against Sleeper
server-side, so an unlinkable handle is rejected at the source rather than
being stored and failing later during roster matching.
"""
from typing import Any

from lambdas.common import platform_follows, platform_users
from lambdas.common.caller_identity import get_caller
from lambdas.common.errors import ValidationError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.sleeper_helper import get_nfl_state, get_sleeper_user, get_user_leagues
from lambdas.common.utility_helpers import parse_body, success_response

HANDLER = "api_users_me"
log = get_logger(HANDLER)


def _shape(record: dict[str, Any]) -> dict[str, Any]:
    """Public view of a user record.

    Explicit field list rather than returning the item: the table is the
    natural home for anything else we hang off a user, and a passthrough
    would leak each new field to the client the moment it is written.

    `hasLinkedSleeper` is computed here so the frontend guard has one boolean
    to check instead of re-deriving the rule from field presence.
    """
    sleeper_user_id = record.get("sleeperUserId") or ""
    return {
        "userId": record.get("userId", ""),
        "email": record.get("email", ""),
        "sleeperUserId": sleeper_user_id,
        "sleeperUsername": record.get("sleeperUsername", ""),
        "sleeperAvatar": record.get("sleeperAvatar", ""),
        "hasLinkedSleeper": bool(sleeper_user_id),
        "createdAt": record.get("createdAt", ""),
        "updatedAt": record.get("updatedAt", ""),
    }


# Final path segment -> (action, expected method). The method is checked as
# well as the segment so a misconfigured route fails loudly here rather than
# quietly performing the wrong action on the record.
_ROUTES = {
    "profile": ("get", "GET"),
    "sleeper-link": ("link", "PUT"),
    "sleeper-unlink": ("unlink", "DELETE"),
}


def _route(event: dict[str, Any]) -> str:
    method = (event.get("httpMethod") or "GET").upper()
    path = event.get("path") or event.get("resource") or ""
    segment = path.rstrip("/").rsplit("/", 1)[-1]

    matched = _ROUTES.get(segment)
    if not matched or matched[1] != method:
        raise ValidationError(f"Unsupported route: {method} {path}")
    return matched[0]


def _link(user_id: str, event: dict[str, Any]) -> dict[str, Any]:
    body = parse_body(event)
    username = str(body.get("sleeperUsername") or "").strip()
    if not username:
        raise ValidationError("sleeperUsername is required")

    # Sleeper's /user/{id_or_username} accepts either form, so this both
    # validates the handle and resolves it to the stable numeric id that
    # every roster lookup keys on. Storing the username alone would break
    # the moment someone renames.
    # Sleeper answers an unknown handle with HTTP 200 and a null body
    # rather than a 404, so the miss shows up here as a falsy profile,
    # not as a raised SleeperAPIError.
    profile = get_sleeper_user(username)
    if not profile or not profile.get("user_id"):
        raise ValidationError(f"No Sleeper account found for '{username}'")

    sleeper_user_id = str(profile["user_id"])
    record = platform_users.link_sleeper(
        user_id=user_id,
        sleeper_user_id=sleeper_user_id,
        sleeper_username=str(profile.get("username") or username),
        avatar=profile.get("avatar"),
    )

    _auto_follow_leagues(user_id, sleeper_user_id)
    return record


def _auto_follow_leagues(user_id: str, sleeper_user_id: str) -> None:
    """Follow everything this Sleeper account is already in.

    Without it a freshly linked user has an empty app and no obvious way to
    fill it. `follow_many` skips leagues already followed, so re-linking
    never resurrects one the user deliberately unfollowed.

    Best-effort: a Sleeper outage must not fail the link itself, which is the
    step the user actually asked for and the one the guard is waiting on.
    """
    try:
        season = str((get_nfl_state() or {}).get("season") or "")
        if not season:
            return
        leagues = [
            {
                "leagueId": str(l.get("league_id") or ""),
                "name": l.get("name") or "",
                "season": str(l.get("season") or ""),
            }
            for l in get_user_leagues(sleeper_user_id, season)
            if l.get("league_id")
        ]
        platform_follows.follow_many(user_id, leagues)
    except Exception as err:
        log.warning(f"users/me: auto-follow skipped for {user_id} - {err}")


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    caller = get_caller(event)
    action = _route(event)

    # Every path needs the record to exist first: link and unlink are
    # update_item calls, which would otherwise create a partial row with no
    # email or createdAt.
    record = platform_users.ensure_user(caller.user_id, caller.email)

    if action == "link":
        record = _link(caller.user_id, event)
    elif action == "unlink":
        record = platform_users.unlink_sleeper(caller.user_id)

    log.info(f"users/me: {action} for {caller.user_id} via {caller.provider}")
    return success_response({"user": _shape(record)})
