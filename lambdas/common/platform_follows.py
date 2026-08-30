"""
Followed leagues
================
The `xomper-follows` table: which leagues a user cares about.

This is the inversion of CLT's `whitelisted_leagues`. Instead of an admin
allowing leagues in, users follow the leagues they are already in on Sleeper.

**It is the cost control.** Every scheduled job iterates followed leagues, so
recurring work scales with what people actually use rather than with every
league on Sleeper. The `leagueId-index` GSI is what lets a cron go the other
way — from a league to everyone following it — without scanning.
"""
from __future__ import annotations

from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from lambdas.common.constants import PLATFORM_FOLLOWS_TABLE
from lambdas.common.errors import DynamoDBError
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import get_iso_timestamp

log = get_logger(__name__)


def _table() -> Any:
    return boto3.resource("dynamodb").Table(PLATFORM_FOLLOWS_TABLE)


def _all_rows(user_id: str) -> list[dict[str, Any]]:
    """Every row for this user, tombstones included."""
    try:
        response = _table().query(
            KeyConditionExpression=Key("userId").eq(user_id)
        )
    except ClientError as err:
        raise DynamoDBError(f"list_for_user failed: {err}") from err
    return response.get("Items", [])


def list_for_user(user_id: str) -> list[dict[str, Any]]:
    """Every league this user actively follows."""
    return [row for row in _all_rows(user_id) if not row.get("unfollowed")]


def followed_league_ids(user_id: str) -> set[str]:
    return {str(item["leagueId"]) for item in list_for_user(user_id) if item.get("leagueId")}


def _known_league_ids(user_id: str) -> set[str]:
    """Leagues this user has ever decided about, unfollows included.

    Auto-follow keys on this rather than on the active set, so a league the
    user deliberately unfollowed is not re-added the next time they link.
    """
    return {str(item["leagueId"]) for item in _all_rows(user_id) if item.get("leagueId")}


def follow(user_id: str, league_id: str, name: str = "", season: str = "") -> dict[str, Any]:
    """Start following a league.

    The name and season are denormalised so a cron can report on a league
    without a Sleeper round trip. They are a snapshot, not a source of truth —
    a renamed league keeps the old name here until the next follow.
    """
    item = {
        "userId": user_id,
        "leagueId": league_id,
        "name": name,
        "season": season,
        "followedAt": get_iso_timestamp(),
        # Overwrites any tombstone from a previous unfollow.
        "unfollowed": False,
    }
    try:
        _table().put_item(Item=item)
    except ClientError as err:
        raise DynamoDBError(f"follow failed: {err}") from err

    log.info(f"follows: {user_id} -> {league_id}")
    return item


def unfollow(user_id: str, league_id: str) -> None:
    """Stop following a league.

    Tombstoned rather than deleted. A deleted row is indistinguishable from
    one that never existed, so auto-follow would re-add the league every time
    the user re-linked their Sleeper account and they would have no way to
    keep it out.
    """
    try:
        _table().update_item(
            Key={"userId": user_id, "leagueId": league_id},
            UpdateExpression="SET unfollowed = :t, unfollowedAt = :at",
            ExpressionAttributeValues={":t": True, ":at": get_iso_timestamp()},
        )
    except ClientError as err:
        raise DynamoDBError(f"unfollow failed: {err}") from err
    log.info(f"follows: {user_id} unfollowed {league_id}")


def follow_many(user_id: str, leagues: list[dict[str, Any]]) -> int:
    """Follow several leagues at once, skipping any already followed.

    Used when a user links their Sleeper account: everything they are already
    in becomes followed, so the app has something to show without asking them
    to pick first.

    Skips any league the user has already decided about — including ones they
    unfollowed, which is why this reads `_known_league_ids` and not the active
    set. Re-linking must not resurrect a league they deliberately removed.
    """
    known = _known_league_ids(user_id)
    fresh = [l for l in leagues if str(l.get("leagueId")) not in known]
    if not fresh:
        return 0

    try:
        with _table().batch_writer() as batch:
            for league in fresh:
                batch.put_item(
                    Item={
                        "userId": user_id,
                        "leagueId": str(league["leagueId"]),
                        "name": str(league.get("name") or ""),
                        "season": str(league.get("season") or ""),
                        "followedAt": get_iso_timestamp(),
                        "unfollowed": False,
                    }
                )
    except ClientError as err:
        raise DynamoDBError(f"follow_many failed: {err}") from err

    log.info(f"follows: {user_id} auto-followed {len(fresh)} leagues")
    return len(fresh)


def followers_of(league_id: str) -> list[str]:
    """Every user following a league. The read path for scheduled jobs."""
    try:
        response = _table().query(
            IndexName="leagueId-index",
            KeyConditionExpression=Key("leagueId").eq(league_id),
        )
    except ClientError as err:
        raise DynamoDBError(f"followers_of failed: {err}") from err
    return [
        str(item["userId"])
        for item in response.get("Items", [])
        if item.get("userId") and not item.get("unfollowed")
    ]


def all_followed_leagues() -> dict[str, list[str]]:
    """Every league someone follows, mapped to its followers.

    One scan rather than a query per league: a scheduled job needs the whole
    picture, and there is no key that enumerates distinct leagues. The
    leagueId-index answers the per-league question; nothing answers "which
    leagues exist" without reading the table.

    This is the cost control the follow table was built for. A league nobody
    follows is not in the result, so a job driven by this never pays Sleeper
    or SES for an audience of zero.
    """
    leagues: dict[str, list[str]] = {}
    kwargs: dict[str, Any] = {
        "ProjectionExpression": "userId, leagueId, unfollowed",
    }
    try:
        while True:
            response = _table().scan(**kwargs)
            for item in response.get("Items", []):
                league_id = str(item.get("leagueId") or "")
                user_id = str(item.get("userId") or "")
                if not league_id or not user_id or item.get("unfollowed"):
                    continue
                leagues.setdefault(league_id, []).append(user_id)
            token = response.get("LastEvaluatedKey")
            if not token:
                return leagues
            kwargs["ExclusiveStartKey"] = token
    except ClientError as err:
        raise DynamoDBError(f"all_followed_leagues failed: {err}") from err
