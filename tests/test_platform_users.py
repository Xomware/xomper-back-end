"""
Tests for `lambdas.common.platform_users`.

Uses `moto` against the `xomper-users` schema from
xomper-infrastructure/terraform/platform_users.tf:
- PK `userId` (S)
- GSI `sleeperUserId-index` on `sleeperUserId` (S)

The behaviour worth pinning here is lazy creation. Sign-up happens entirely
in Cognito, so this table first hears of a user on their first authenticated
request — and two requests can land at once.
"""
from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import PLATFORM_USERS_TABLE


@pytest.fixture
def users_table():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName=PLATFORM_USERS_TABLE,
            KeySchema=[{"AttributeName": "userId", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "userId", "AttributeType": "S"},
                {"AttributeName": "sleeperUserId", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "sleeperUserId-index",
                    "KeySchema": [
                        {"AttributeName": "sleeperUserId", "KeyType": "HASH"}
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield dynamodb.Table(PLATFORM_USERS_TABLE)


@pytest.fixture
def store():
    import importlib

    from lambdas.common import platform_users

    return importlib.reload(platform_users)


def test_get_user_returns_none_when_absent(users_table, store):
    assert store.get_user("nobody") is None


def test_ensure_user_creates_on_first_sight(users_table, store):
    record = store.ensure_user("cog-1", "dom@example.com")

    assert record["userId"] == "cog-1"
    assert record["email"] == "dom@example.com"
    assert record["createdAt"]
    assert store.get_user("cog-1") is not None


def test_ensure_user_is_idempotent(users_table, store):
    first = store.ensure_user("cog-1", "dom@example.com")
    second = store.ensure_user("cog-1", "dom@example.com")

    # The second call must not reset createdAt — that timestamp is the only
    # record of when the account started here.
    assert second["createdAt"] == first["createdAt"]


def test_ensure_user_backfills_a_late_email(users_table, store):
    store.ensure_user("cog-1", None)
    record = store.ensure_user("cog-1", "later@example.com")

    assert record["email"] == "later@example.com"


def test_ensure_user_does_not_clobber_a_link(users_table, store):
    store.ensure_user("cog-1", "dom@example.com")
    store.link_sleeper("cog-1", "594625531702460416", "dgiordano")

    record = store.ensure_user("cog-1", "dom@example.com")

    # A second sign-in must not wipe the Sleeper linkage, which would bounce
    # the user back to /link-sleeper on every visit.
    assert record["sleeperUserId"] == "594625531702460416"


def test_link_sleeper_stores_id_username_and_avatar(users_table, store):
    store.ensure_user("cog-1", "dom@example.com")
    record = store.link_sleeper(
        "cog-1", "594625531702460416", "dgiordano", avatar="abc123"
    )

    assert record["sleeperUserId"] == "594625531702460416"
    assert record["sleeperUsername"] == "dgiordano"
    assert record["sleeperAvatar"] == "abc123"


def test_link_sleeper_is_reachable_by_the_gsi(users_table, store):
    store.ensure_user("cog-1", "dom@example.com")
    store.link_sleeper("cog-1", "594625531702460416", "dgiordano")

    found = users_table.query(
        IndexName="sleeperUserId-index",
        KeyConditionExpression=boto3.dynamodb.conditions.Key(
            "sleeperUserId"
        ).eq("594625531702460416"),
    )

    # This index is how a shared league resolves which other managers on a
    # roster have accounts here.
    assert found["Count"] == 1
    assert found["Items"][0]["userId"] == "cog-1"


def test_unlink_removes_the_link_but_keeps_the_user(users_table, store):
    store.ensure_user("cog-1", "dom@example.com")
    store.link_sleeper("cog-1", "594625531702460416", "dgiordano")

    record = store.unlink_sleeper("cog-1")

    assert "sleeperUserId" not in record
    assert record["email"] == "dom@example.com"
