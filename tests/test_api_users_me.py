"""
Tests for `lambdas.api_users_me.handler`.

This endpoint replaces the direct Supabase `profiles` read the frontend used
to do. The behaviour that matters is that identity comes from the authorizer
context and nowhere else, and that a Sleeper handle is verified against
Sleeper before it is stored.
"""
from __future__ import annotations

import importlib
import json

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import PLATFORM_FOLLOWS_TABLE, PLATFORM_USERS_TABLE

SLEEPER_LEAGUES = [
    {"league_id": "1317249551823814656", "name": "Charlotte Dynasty League", "season": "2026"},
    {"league_id": "1389328793713250304", "name": "CLIT Fantasy Football", "season": "2026"},
]

SLEEPER_PROFILE = {
    "user_id": "594625531702460416",
    "username": "dgiordano",
    "avatar": "abc123",
}


@pytest.fixture
def users_table():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName=PLATFORM_USERS_TABLE,
            KeySchema=[{"AttributeName": "userId", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "userId", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        # Linking auto-follows the account's leagues, so this table has to
        # exist for the link path even though this module does not assert on
        # it beyond the auto-follow cases below.
        dynamodb.create_table(
            TableName=PLATFORM_FOLLOWS_TABLE,
            KeySchema=[
                {"AttributeName": "userId", "KeyType": "HASH"},
                {"AttributeName": "leagueId", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "userId", "AttributeType": "S"},
                {"AttributeName": "leagueId", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "leagueId-index",
                    "KeySchema": [{"AttributeName": "leagueId", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield dynamodb.Table(PLATFORM_USERS_TABLE)


@pytest.fixture
def mod(users_table, monkeypatch):
    from lambdas.common import platform_follows, platform_users

    importlib.reload(platform_follows)
    importlib.reload(platform_users)
    from lambdas.api_users_me import handler as handler_mod

    handler_mod = importlib.reload(handler_mod)
    monkeypatch.setattr(handler_mod, "get_nfl_state", lambda: {"season": "2026"})
    monkeypatch.setattr(handler_mod, "get_user_leagues", lambda _uid, _s: SLEEPER_LEAGUES)
    return handler_mod


def event(method="GET", path="/me/profile", body=None, sub="cog-1", email="d@x.com"):
    return {
        "httpMethod": method,
        "path": path,
        "body": json.dumps(body) if body is not None else None,
        "requestContext": {
            "authorizer": {"sub": sub, "email": email, "provider": "cognito"}
        },
    }


def body_of(response):
    return json.loads(response["body"])


def test_get_creates_the_record_on_first_call(mod):
    response = mod.handler(event(), None)

    assert response["statusCode"] == 200
    user = body_of(response)["user"]
    assert user["userId"] == "cog-1"
    assert user["email"] == "d@x.com"
    assert user["hasLinkedSleeper"] is False


def test_identity_comes_from_the_authorizer_not_the_body(mod):
    # A caller must not be able to read or mutate someone else's record by
    # naming them in the payload.
    response = mod.handler(
        event(body={"userId": "someone-else"}, sub="cog-1"), None
    )

    assert body_of(response)["user"]["userId"] == "cog-1"


def test_missing_authorizer_context_is_rejected(mod):
    bare = event()
    bare["requestContext"] = {}

    response = mod.handler(bare, None)

    # Fail closed: a route accidentally wired without an authorizer must not
    # fall through to an anonymous user.
    assert response["statusCode"] == 401


def test_link_resolves_the_username_against_sleeper(mod, monkeypatch):
    monkeypatch.setattr(mod, "get_sleeper_user", lambda _: SLEEPER_PROFILE)

    response = mod.handler(
        event(
            method="PUT",
            path="/me/sleeper-link",
            body={"sleeperUsername": "dgiordano"},
        ),
        None,
    )

    user = body_of(response)["user"]
    assert user["sleeperUserId"] == "594625531702460416"
    assert user["sleeperUsername"] == "dgiordano"
    assert user["hasLinkedSleeper"] is True


def test_link_rejects_an_unknown_handle(mod, monkeypatch):
    # Sleeper answers an unknown handle with HTTP 200 and a null body.
    monkeypatch.setattr(mod, "get_sleeper_user", lambda _: None)

    response = mod.handler(
        event(
            method="PUT",
            path="/me/sleeper-link",
            body={"sleeperUsername": "not-a-real-handle"},
        ),
        None,
    )

    assert response["statusCode"] == 400
    assert mod.platform_users.get_user("cog-1").get("sleeperUserId") is None


def test_link_requires_a_username(mod):
    response = mod.handler(
        event(method="PUT", path="/me/sleeper-link", body={}), None
    )

    assert response["statusCode"] == 400


def test_unlink_clears_the_link(mod, monkeypatch):
    monkeypatch.setattr(mod, "get_sleeper_user", lambda _: SLEEPER_PROFILE)
    mod.handler(
        event(
            method="PUT",
            path="/me/sleeper-link",
            body={"sleeperUsername": "dgiordano"},
        ),
        None,
    )

    response = mod.handler(
        event(method="DELETE", path="/me/sleeper-unlink"), None
    )

    assert body_of(response)["user"]["hasLinkedSleeper"] is False


def test_response_does_not_leak_unlisted_fields(mod, users_table):
    mod.handler(event(), None)
    users_table.update_item(
        Key={"userId": "cog-1"},
        UpdateExpression="SET espnCookie = :c",
        ExpressionAttributeValues={":c": "secret"},
    )

    user = body_of(mod.handler(event(), None))["user"]

    # The shape is an explicit allowlist so future columns are opt-in.
    assert "espnCookie" not in user


def test_a_method_mismatch_is_rejected(mod):
    # The path segment alone does not decide the action — a route wired with
    # the wrong method must fail rather than silently unlink on a PUT.
    response = mod.handler(
        event(method="PUT", path="/me/sleeper-unlink", body={}), None
    )

    assert response["statusCode"] == 400


def test_linking_auto_follows_the_accounts_leagues(mod, monkeypatch):
    monkeypatch.setattr(mod, "get_sleeper_user", lambda _: SLEEPER_PROFILE)

    mod.handler(
        event(method="PUT", path="/me/sleeper-link", body={"sleeperUsername": "dgiordano"}),
        None,
    )

    # Without this a freshly linked user lands on an app with no leagues and
    # no obvious way to add one.
    assert mod.platform_follows.followed_league_ids("cog-1") == {
        "1317249551823814656",
        "1389328793713250304",
    }


def test_a_sleeper_outage_does_not_fail_the_link(mod, monkeypatch):
    monkeypatch.setattr(mod, "get_sleeper_user", lambda _: SLEEPER_PROFILE)

    def boom(*_args):
        raise RuntimeError("sleeper down")

    monkeypatch.setattr(mod, "get_user_leagues", boom)

    response = mod.handler(
        event(method="PUT", path="/me/sleeper-link", body={"sleeperUsername": "dgiordano"}),
        None,
    )

    # Linking is the step the user asked for and the one the guard waits on.
    # Auto-follow is a convenience on top and must not take it down.
    assert response["statusCode"] == 200
    assert body_of(response)["user"]["hasLinkedSleeper"] is True
