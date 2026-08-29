"""
Per-user ESPN cookies.

A private ESPN league is only readable with the member's `espn_s2` and `SWID`
cookies, and a browser cannot send them cross-site, so they have to live
server-side. They are stored on the caller's own row in the platform users
table rather than in SSM or Secrets Manager: those hold one ops-managed secret
for the whole app, while these are user data, one set per user, read per
request.

Treat them as a **full ESPN session**, not a scoped read token. They are not
limited to fantasy, so they never appear in a log line or an API response, and
`clear_espn` exists so a user can revoke without deleting their account.
"""
from typing import Any

from botocore.exceptions import ClientError

from lambdas.common.errors import DynamoDBError
from lambdas.common.logger import get_logger
from lambdas.common.platform_users import _table
from lambdas.common.utility_helpers import get_iso_timestamp

log = get_logger("espn_credentials")


def store_espn(user_id: str, espn_s2: str, swid: str) -> None:
    """Attach ESPN cookies to this user. Overwrites any existing pair."""
    try:
        _table().update_item(
            Key={"userId": user_id},
            UpdateExpression="SET espnS2 = :s2, espnSwid = :swid, updatedAt = :t",
            ExpressionAttributeValues={
                ":s2": espn_s2,
                ":swid": swid,
                ":t": get_iso_timestamp(),
            },
        )
    except ClientError as err:
        raise DynamoDBError(f"store_espn failed: {err}") from err
    # Deliberately no value in the log line.
    log.info(f"espn_credentials: stored for {user_id}")


def get_espn(user_id: str) -> dict[str, str] | None:
    """The user's cookies, or None if they have not connected ESPN."""
    try:
        item = (_table().get_item(Key={"userId": user_id}) or {}).get("Item") or {}
    except ClientError as err:
        raise DynamoDBError(f"get_espn failed: {err}") from err

    espn_s2, swid = item.get("espnS2"), item.get("espnSwid")
    if not espn_s2 or not swid:
        return None
    return {"espn_s2": str(espn_s2), "SWID": str(swid)}


def clear_espn(user_id: str) -> None:
    """Revoke. Leaves the rest of the user record intact."""
    try:
        _table().update_item(
            Key={"userId": user_id},
            UpdateExpression="REMOVE espnS2, espnSwid SET updatedAt = :t",
            ExpressionAttributeValues={":t": get_iso_timestamp()},
        )
    except ClientError as err:
        raise DynamoDBError(f"clear_espn failed: {err}") from err
    log.info(f"espn_credentials: cleared for {user_id}")


def has_espn(user: dict[str, Any] | None) -> bool:
    """Whether a user record carries ESPN cookies, without reading them."""
    return bool((user or {}).get("espnS2") and (user or {}).get("espnSwid"))
