"""
Comments and reactions
======================
The other half of `xomper-social`.

Same table as friendships, different key prefix. Friendships are
`USER#<id>` / `FRIEND#<other>`; a comment thread is `THREAD#<type>#<id>` /
`COMMENT#<isoTimestamp>#<uuid>`, and a reaction is `COMMENT#<id>` /
`LIKE#<userId>`.

Sorting by timestamp inside the sort key is what makes "the comments on this
league, oldest first" a plain query with no index and no sort in Python. The
uuid suffix is there because two comments in the same millisecond would
otherwise collide and one would overwrite the other.

Authors are Cognito subs. The Sleeper handle is never an author: handle
claims are unverified, so anyone could post as anyone.
"""
from __future__ import annotations

import uuid
from typing import Any, Literal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from lambdas.common.constants import SOCIAL_TABLE
from lambdas.common.errors import DynamoDBError, NotFoundError, ValidationError
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import get_iso_timestamp

log = get_logger(__name__)

TargetType = Literal["league", "player", "trade"]
VALID_TARGETS = ("league", "player", "trade")

# Long enough for a real thought, short enough that one comment cannot
# dominate a thread or a notification.
COMMENT_MAX = 1000


def _table() -> Any:
    return boto3.resource("dynamodb").Table(SOCIAL_TABLE)


def _thread_pk(target_type: str, target_id: str) -> str:
    return f"THREAD#{target_type}#{target_id}"


def add_comment(
    author_id: str,
    target_type: str,
    target_id: str,
    body: str,
    mentions: list[str] | None = None,
) -> dict[str, Any]:
    """Post a comment.

    `mentions` are Cognito subs resolved by the caller, not handles parsed out
    of the text. Parsing @names server-side would mean matching on display
    names, which are not unique -- the client knows who it offered.
    """
    if target_type not in VALID_TARGETS:
        raise ValidationError(f"Unknown target type: {target_type}")
    if not target_id:
        raise ValidationError("targetId is required")

    text = (body or "").strip()
    if not text:
        raise ValidationError("Comment cannot be empty")
    if len(text) > COMMENT_MAX:
        raise ValidationError(f"Comment must be {COMMENT_MAX} characters or fewer")

    now = get_iso_timestamp()
    comment_id = str(uuid.uuid4())
    item = {
        "pk": _thread_pk(target_type, target_id),
        # Timestamp first so a query returns chronological order for free;
        # the uuid breaks ties within the same millisecond.
        "sk": f"COMMENT#{now}#{comment_id}",
        "commentId": comment_id,
        "authorId": author_id,
        "targetType": target_type,
        "targetId": target_id,
        "body": text,
        "mentions": mentions or [],
        "createdAt": now,
    }
    try:
        _table().put_item(Item=item)
    except ClientError as err:
        raise DynamoDBError(f"add_comment failed: {err}") from err

    log.info(f"social: {author_id} commented on {target_type}/{target_id}")
    return item


def list_comments(target_type: str, target_id: str, limit: int = 100) -> list[dict[str, Any]]:
    """A thread, oldest first.

    Capped because a thread is rendered in full; there is no pagination in
    the UI yet, and returning ten thousand comments would be a worse answer
    than returning the first hundred.
    """
    try:
        response = _table().query(
            KeyConditionExpression=Key("pk").eq(_thread_pk(target_type, target_id))
            & Key("sk").begins_with("COMMENT#"),
            Limit=limit,
        )
    except ClientError as err:
        raise DynamoDBError(f"list_comments failed: {err}") from err
    return response.get("Items", [])


def get_comment(target_type: str, target_id: str, comment_id: str) -> dict[str, Any] | None:
    for item in list_comments(target_type, target_id):
        if item.get("commentId") == comment_id:
            return item
    return None


def delete_comment(author_id: str, target_type: str, target_id: str, comment_id: str) -> None:
    """Remove a comment. Only its author may.

    Checked here rather than trusted from the caller: the handler knows who
    is asking, but only the stored row knows who wrote it.
    """
    item = get_comment(target_type, target_id, comment_id)
    if not item:
        raise NotFoundError("Comment not found")
    if item.get("authorId") != author_id:
        raise ValidationError("You can only delete your own comments")

    try:
        _table().delete_item(Key={"pk": item["pk"], "sk": item["sk"]})
    except ClientError as err:
        raise DynamoDBError(f"delete_comment failed: {err}") from err
    log.info(f"social: {author_id} deleted comment {comment_id}")


def set_reaction(user_id: str, comment_id: str, liked: bool) -> None:
    """Like or unlike. Idempotent in both directions.

    One row per person per comment, so a double-tap cannot count twice and
    un-liking something never liked is a no-op rather than an error.
    """
    key = {"pk": f"COMMENT#{comment_id}", "sk": f"LIKE#{user_id}"}
    try:
        if liked:
            _table().put_item(Item={**key, "userId": user_id, "commentId": comment_id})
        else:
            _table().delete_item(Key=key)
    except ClientError as err:
        raise DynamoDBError(f"set_reaction failed: {err}") from err


def reactions_for(comment_ids: list[str]) -> dict[str, list[str]]:
    """Who liked each comment.

    One query per comment. Fine for a rendered thread of a hundred; if
    threads ever get long enough for this to hurt, the fix is a projection on
    the thread row, not a scan.
    """
    out: dict[str, list[str]] = {}
    for comment_id in comment_ids:
        try:
            response = _table().query(
                KeyConditionExpression=Key("pk").eq(f"COMMENT#{comment_id}")
                & Key("sk").begins_with("LIKE#")
            )
        except ClientError as err:
            raise DynamoDBError(f"reactions_for failed: {err}") from err
        out[comment_id] = [
            str(i["userId"]) for i in response.get("Items", []) if i.get("userId")
        ]
    return out
