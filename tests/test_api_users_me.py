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

from lambdas.common.constants import PLATFORM_USERS_TABLE

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
        yield dynamodb.Table(PLATFORM_USERS_TABLE)


@pytest.fixture
def mod(users_table):
    from lambdas.common import platform_users

    importlib.reload(platform_users)
    from lambdas.api_users_me import handler as handler_mod

    return importlib.reload(handler_mod)


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
