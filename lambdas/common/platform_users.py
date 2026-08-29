"""
Platform user store
===================
The `xomper-users` table: one item per Cognito user, holding what the pool
does not.

The shared `xomware-users` pool is estate-wide — the same identity signs into
xomware.com, xomforms and Xomper. Sleeper linkage is Xomper's alone, so it
lives here rather than as a custom pool attribute. That also keeps the record
extensible (followed leagues, strategy presets) without touching a pool five
other apps depend on.

This replaces the Supabase `profiles` contract. There is no migration: the
platform starts empty, and the existing `profiles` rows stay with
clt-dynasty-league.
"""
from __future__ import annotations

from typing import Any

import boto3
from botocore.exceptions import ClientError

from lambdas.common.constants import PLATFORM_USERS_TABLE
from lambdas.common.errors import DynamoDBError
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import get_iso_timestamp

log = get_logger(__name__)


def _table() -> Any:
    return boto3.resource("dynamodb").Table(PLATFORM_USERS_TABLE)


def get_user(user_id: str) -> dict[str, Any] | None:
    """Return the stored record, or None if this user has never been seen."""
    try:
        response = _table().get_item(Key={"userId": user_id})
    except ClientError as err:
        raise DynamoDBError(f"get_user failed: {err}") from err
    return response.get("Item")


def ensure_user(user_id: str, email: str | None) -> dict[str, Any]:
    """Return the user's record, creating a bare one on first sight.

    Sign-up happens entirely in Cognito, so the first this table hears of a
    user is their first authenticated request. Creating the row lazily here
    means there is no Cognito trigger to keep in sync, and no window where a
    signed-in user has no record.
    """
    existing = get_user(user_id)
    if existing:
        # Backfill an email that arrived later — federated sign-ups can reach
        # us before the pool has the claim populated.
        if email and not existing.get("email"):
            try:
                _table().update_item(
                    Key={"userId": user_id},
                    UpdateExpression="SET email = :e, updatedAt = :t",
                    ExpressionAttributeValues={
                        ":e": email,
                        ":t": get_iso_timestamp(),
                    },
                )
            except ClientError as err:
                raise DynamoDBError(f"ensure_user backfill failed: {err}") from err
            existing["email"] = email
        return existing

    now = get_iso_timestamp()
    record = {
        "userId": user_id,
        "email": email or "",
        "createdAt": now,
        "updatedAt": now,
    }
    try:
        # Guard against two concurrent first requests racing to create the
        # row. Losing the race is not an error — the winner wrote the same
        # thing — so fall through to a read.
        _table().put_item(
            Item=record,
            ConditionExpression="attribute_not_exists(userId)",
        )
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return get_user(user_id) or record
        raise DynamoDBError(f"ensure_user create failed: {err}") from err

    log.info(f"platform_users: created record for {user_id}")
    return record


def set_display_name(user_id: str, display_name: str) -> dict[str, Any]:
    """Set the name this user goes by in Xomper.

    Xomper owns this rather than borrowing the Sleeper handle. The handle is
    unverified -- anyone can claim any of them -- so presenting it as identity
    means the app asserts something it never checked. A name attached to a
    confirmed account is a claim we can actually stand behind.
    """
    try:
        response = _table().update_item(
            Key={"userId": user_id},
            UpdateExpression="SET displayName = :n, updatedAt = :t",
            ExpressionAttributeValues={
                ":n": display_name,
                ":t": get_iso_timestamp(),
            },
            ReturnValues="ALL_NEW",
        )
    except ClientError as err:
        raise DynamoDBError(f"set_display_name failed: {err}") from err
    return response.get("Attributes", {})


def link_sleeper(
    user_id: str,
    sleeper_user_id: str,
    sleeper_username: str,
    avatar: str | None = None,
) -> dict[str, Any]:
    """Attach a verified Sleeper account to this user.

    The caller is responsible for having resolved the username against
    Sleeper first — this writes what it is given.
    """
    # if_not_exists on displayName: seed it from the Sleeper handle so a new
    # user is never nameless, but never overwrite a name they chose.
    try:
        response = _table().update_item(
            Key={"userId": user_id},
            UpdateExpression=(
                "SET sleeperUserId = :sid, sleeperUsername = :name, "
                "sleeperAvatar = :av, updatedAt = :t, "
                "displayName = if_not_exists(displayName, :name)"
            ),
            ExpressionAttributeValues={
                ":sid": sleeper_user_id,
                ":name": sleeper_username,
                ":av": avatar or "",
                ":t": get_iso_timestamp(),
            },
            ReturnValues="ALL_NEW",
        )
    except ClientError as err:
        raise DynamoDBError(f"link_sleeper failed: {err}") from err

    log.info(f"platform_users: linked {user_id} -> sleeper {sleeper_user_id}")
    return response.get("Attributes", {})


def unlink_sleeper(user_id: str) -> dict[str, Any]:
    """Detach the Sleeper account, leaving the rest of the record intact."""
    try:
        response = _table().update_item(
            Key={"userId": user_id},
            UpdateExpression=(
                "REMOVE sleeperUserId, sleeperUsername, sleeperAvatar "
                "SET updatedAt = :t"
            ),
            ExpressionAttributeValues={":t": get_iso_timestamp()},
            ReturnValues="ALL_NEW",
        )
    except ClientError as err:
        raise DynamoDBError(f"unlink_sleeper failed: {err}") from err
    return response.get("Attributes", {})
