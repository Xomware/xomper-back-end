"""
API — Comments
==============
GET    /comments/list    -> a thread, oldest first, with authors and likes
PUT    /comments/add     -> post to a thread
DELETE /comments/delete  -> remove your own
PUT    /comments/react   -> like or unlike

A thread hangs off a target: a league, a player, or a trade. Authors are
Cognito subs and rendered through `displayName`, the name Xomper owns -- the
Sleeper handle is unverified, so anyone could otherwise appear as anyone.

Reads are open to any signed-in user because everything a comment can be
attached to is already public data. Writes are attributed to the caller and
deletes are restricted to the author, both enforced below the handler.
"""
from __future__ import annotations

from typing import Any

from lambdas.common import comment_store, platform_users
from lambdas.common.caller_identity import get_caller
from lambdas.common.errors import ValidationError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import (
    get_query_params,
    parse_body,
    success_response,
)

HANDLER = "api_comments"
log = get_logger(HANDLER)

_ROUTES = {
    "list": ("list", "GET"),
    "add": ("add", "PUT"),
    "delete": ("delete", "DELETE"),
    "react": ("react", "PUT"),
}


def _route(event: dict[str, Any]) -> str:
    method = (event.get("httpMethod") or "GET").upper()
    path = event.get("path") or event.get("resource") or ""
    segment = path.rstrip("/").rsplit("/", 1)[-1]

    matched = _ROUTES.get(segment)
    if not matched or matched[1] != method:
        raise ValidationError(f"Unsupported route: {method} {path}")
    return matched[0]


def _target(source: dict[str, Any]) -> tuple[str, str]:
    target_type = str(source.get("targetType") or "").strip()
    target_id = str(source.get("targetId") or "").strip()
    if not target_type or not target_id:
        raise ValidationError("targetType and targetId are required")
    return target_type, target_id


def _authors(user_ids: set[str]) -> dict[str, dict[str, str]]:
    """Resolve every distinct author in one pass.

    A thread of a hundred comments from six people is six lookups, not a
    hundred -- and the client never has to resolve a person itself.
    """
    out: dict[str, dict[str, str]] = {}
    for user_id in user_ids:
        record = platform_users.get_user(user_id) or {}
        out[user_id] = {
            "userId": user_id,
            "displayName": str(
                record.get("displayName") or record.get("sleeperUsername") or "Someone"
            ),
            "sleeperAvatar": str(record.get("sleeperAvatar") or ""),
        }
    return out


def _render(comments: list[dict[str, Any]], caller_id: str) -> list[dict[str, Any]]:
    ids = [str(c["commentId"]) for c in comments if c.get("commentId")]
    likes = comment_store.reactions_for(ids) if ids else {}
    authors = _authors({str(c.get("authorId") or "") for c in comments})

    return [
        {
            "commentId": str(c.get("commentId") or ""),
            "body": str(c.get("body") or ""),
            "createdAt": str(c.get("createdAt") or ""),
            "author": authors.get(str(c.get("authorId") or ""), {}),
            "mentions": list(c.get("mentions") or []),
            "likeCount": len(likes.get(str(c.get("commentId") or ""), [])),
            # Whether *you* liked it, so the client does not have to search a
            # list of ids to render one button.
            "likedByMe": caller_id in likes.get(str(c.get("commentId") or ""), []),
            # Same, for whether to offer a delete control.
            "mine": str(c.get("authorId") or "") == caller_id,
        }
        for c in comments
    ]


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    caller = get_caller(event)
    action = _route(event)
    body = parse_body(event)

    if action == "list":
        target_type, target_id = _target(get_query_params(event))
    else:
        target_type, target_id = _target(body)

    if action == "add":
        comment_store.add_comment(
            author_id=caller.user_id,
            target_type=target_type,
            target_id=target_id,
            body=str(body.get("body") or ""),
            mentions=[str(m) for m in (body.get("mentions") or [])],
        )
    elif action == "delete":
        comment_id = str(body.get("commentId") or "").strip()
        if not comment_id:
            raise ValidationError("commentId is required")
        comment_store.delete_comment(caller.user_id, target_type, target_id, comment_id)
    elif action == "react":
        comment_id = str(body.get("commentId") or "").strip()
        if not comment_id:
            raise ValidationError("commentId is required")
        comment_store.set_reaction(caller.user_id, comment_id, bool(body.get("liked")))

    # Every path returns the thread, so one call both changes and re-syncs.
    comments = comment_store.list_comments(target_type, target_id)
    log.info(f"comments: {action} on {target_type}/{target_id} by {caller.user_id}")
    return success_response(
        {
            "targetType": target_type,
            "targetId": target_id,
            "count": len(comments),
            "comments": _render(comments, caller.user_id),
        }
    )
