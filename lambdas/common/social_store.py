"""
Social graph
============
The `xomper-social` table: who is friends with whom.

Keyed on the **Cognito sub**, never the Sleeper handle. Handle claims are
unverified by design -- any account can claim any handle, and more than one
can claim the same one -- so a social graph built on them would let anyone
befriend or be befriended as someone else. The confirmed account is the only
thing worth keying an identity on. Names for humans come from `displayName`.

One table, two rows per friendship. A request writes both directions at once:
the requester's row is `outgoing`, the recipient's `incoming`. Accepting flips
both to `accepted`. Storing both means "who are my friends" and "who wants to
be mine" are each a single query on the partition key, with no index and no
scan -- the two questions the UI actually asks.
"""
from __future__ import annotations

from typing import Any, Literal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from lambdas.common.constants import SOCIAL_TABLE
from lambdas.common.errors import DynamoDBError, ValidationError
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import get_iso_timestamp

log = get_logger(__name__)

FriendState = Literal["outgoing", "incoming", "accepted"]

_FRIEND_PREFIX = "FRIEND#"


def _table() -> Any:
    return boto3.resource("dynamodb").Table(SOCIAL_TABLE)


def _key(user_id: str, other_id: str) -> dict[str, str]:
    return {"pk": f"USER#{user_id}", "sk": f"{_FRIEND_PREFIX}{other_id}"}


def _row(user_id: str, other_id: str, state: FriendState, now: str) -> dict[str, Any]:
    return {
        **_key(user_id, other_id),
        "userId": user_id,
        "otherUserId": other_id,
        "state": state,
        "updatedAt": now,
    }


def request_friend(user_id: str, other_id: str) -> dict[str, Any]:
    """Ask to be someone's friend.

    Writing both directions is what makes the incoming list a plain query
    rather than a scan or a second index.
    """
    if user_id == other_id:
        raise ValidationError("You cannot friend yourself")

    existing = get_friendship(user_id, other_id)
    if existing:
        state = existing.get("state")
        if state == "accepted":
            raise ValidationError("Already friends")
        if state == "outgoing":
            raise ValidationError("Request already sent")
        if state == "incoming":
            # They asked first. Treat this as the accept it plainly is,
            # rather than making two people stare at pending requests.
            return accept_friend(user_id, other_id)

    now = get_iso_timestamp()
    try:
        with _table().batch_writer() as batch:
            batch.put_item(Item=_row(user_id, other_id, "outgoing", now))
            batch.put_item(Item=_row(other_id, user_id, "incoming", now))
    except ClientError as err:
        raise DynamoDBError(f"request_friend failed: {err}") from err

    log.info(f"social: {user_id} requested {other_id}")
    return _row(user_id, other_id, "outgoing", now)


def accept_friend(user_id: str, other_id: str) -> dict[str, Any]:
    """Accept a request. Only the recipient of an incoming request may."""
    existing = get_friendship(user_id, other_id)
    if not existing or existing.get("state") != "incoming":
        raise ValidationError("No pending request from that user")

    now = get_iso_timestamp()
    try:
        with _table().batch_writer() as batch:
            batch.put_item(Item=_row(user_id, other_id, "accepted", now))
            batch.put_item(Item=_row(other_id, user_id, "accepted", now))
    except ClientError as err:
        raise DynamoDBError(f"accept_friend failed: {err}") from err

    log.info(f"social: {user_id} accepted {other_id}")
    return _row(user_id, other_id, "accepted", now)


def remove_friend(user_id: str, other_id: str) -> None:
    """Decline, cancel, or unfriend.

    All three are the same operation on the same two rows, and distinguishing
    them in the API would only invite the caller to get it wrong.
    """
    try:
        with _table().batch_writer() as batch:
            batch.delete_item(Key=_key(user_id, other_id))
            batch.delete_item(Key=_key(other_id, user_id))
    except ClientError as err:
        raise DynamoDBError(f"remove_friend failed: {err}") from err
    log.info(f"social: {user_id} removed {other_id}")


def get_friendship(user_id: str, other_id: str) -> dict[str, Any] | None:
    try:
        response = _table().get_item(Key=_key(user_id, other_id))
    except ClientError as err:
        raise DynamoDBError(f"get_friendship failed: {err}") from err
    return response.get("Item")


def list_friends(user_id: str, state: FriendState | None = None) -> list[dict[str, Any]]:
    """Every friendship row for a user, optionally filtered by state.

    One query on the partition key. Filtering in Python rather than with a
    FilterExpression because a user's friend list is small and the read is
    charged on what the query returns either way.
    """
    try:
        response = _table().query(
            KeyConditionExpression=Key("pk").eq(f"USER#{user_id}")
            & Key("sk").begins_with(_FRIEND_PREFIX)
        )
    except ClientError as err:
        raise DynamoDBError(f"list_friends failed: {err}") from err

    rows = response.get("Items", [])
    if state:
        rows = [r for r in rows if r.get("state") == state]
    return rows


def pending_count(user_id: str) -> int:
    """Incoming requests awaiting an answer. What the bell counts."""
    return len(list_friends(user_id, "incoming"))
