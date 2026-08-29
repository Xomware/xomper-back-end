"""
API — The caller's friends
==========================
GET    /me/friends         -> friends, incoming requests, outgoing requests
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

from lambdas.common import platform_users, social_store
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

    log.info(f"friends: {action} for {caller.user_id}")
    return success_response(
        {
            "friends": _hydrate(by_state["accepted"]),
            "incoming": _hydrate(by_state["incoming"]),
            "outgoing": _hydrate(by_state["outgoing"]),
            # What the notification bell counts.
            "pendingCount": len(by_state["incoming"]),
        }
    )
