"""
Tests for `lambdas.api_users_friends.handler`.

Identity here is the Cognito sub throughout. The failure that matters is a
caller acting as, or on, someone they are not — so the tests lean on where
the actor comes from, not just on the happy path.
"""
from __future__ import annotations

import importlib
import json

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import PLATFORM_USERS_TABLE, SOCIAL_TABLE

A = "cog-a"
B = "cog-b"


@pytest.fixture
def tables():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName=SOCIAL_TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        users = dynamodb.create_table(
            TableName=PLATFORM_USERS_TABLE,
            KeySchema=[{"AttributeName": "userId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "userId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        users.put_item(
            Item={
                "userId": B,
                "email": "b@x.com",
                "displayName": "Bee",
                "sleeperUsername": "beehandle",
                "sleeperAvatar": "av",
            }
        )
        yield dynamodb


@pytest.fixture
def mod(tables):
    from lambdas.common import platform_users, social_store

    importlib.reload(social_store)
    importlib.reload(platform_users)
    from lambdas.api_users_friends import handler as handler_mod

    return importlib.reload(handler_mod)


def event(method="GET", path="/me/friends", body=None, sub=A):
    return {
        "httpMethod": method,
        "path": path,
        "body": json.dumps(body) if body is not None else None,
        "requestContext": {"authorizer": {"sub": sub, "email": "a@x.com", "provider": "cognito"}},
    }


def body_of(response):
    return json.loads(response["body"])


def test_empty_graph(mod):
    payload = body_of(mod.handler(event(), None))

    assert payload == {
        "friends": [],
        "incoming": [],
        "outgoing": [],
        "pendingCount": 0,
        "suggestions": [],
    }


def test_requesting_shows_on_both_sides(mod):
    mod.handler(event("PUT", "/me/friend-request", {"userId": B}), None)

    mine = body_of(mod.handler(event(), None))
    theirs = body_of(mod.handler(event(sub=B), None))

    assert len(mine["outgoing"]) == 1
    assert len(theirs["incoming"]) == 1
    assert theirs["pendingCount"] == 1


def test_rows_carry_the_xomper_display_name(mod):
    mod.handler(event("PUT", "/me/friend-request", {"userId": B}), None)

    person = body_of(mod.handler(event(), None))["outgoing"][0]

    # displayName, not the Sleeper handle — the handle is unverified.
    assert person["displayName"] == "Bee"
    assert person["sleeperUsername"] == "beehandle"


def test_a_person_without_a_record_still_renders(mod):
    mod.handler(event("PUT", "/me/friend-request", {"userId": "ghost"}), None)

    person = body_of(mod.handler(event(), None))["outgoing"][0]

    # A friend row must never render blank just because the other user has
    # not been seen by this table yet.
    assert person["displayName"] == "Someone"


def test_no_email_is_exposed(mod):
    mod.handler(event("PUT", "/me/friend-request", {"userId": B}), None)

    person = body_of(mod.handler(event(), None))["outgoing"][0]

    # A friend list is not a reason to hand out addresses.
    assert "email" not in person


def test_accepting_moves_both_to_friends(mod):
    mod.handler(event("PUT", "/me/friend-request", {"userId": B}), None)

    mod.handler(event("PUT", "/me/friend-accept", {"userId": A}, sub=B), None)

    assert len(body_of(mod.handler(event(), None))["friends"]) == 1
    assert len(body_of(mod.handler(event(sub=B), None))["friends"]) == 1


def test_the_requester_cannot_accept_their_own_request(mod):
    mod.handler(event("PUT", "/me/friend-request", {"userId": B}), None)

    response = mod.handler(event("PUT", "/me/friend-accept", {"userId": B}), None)

    assert response["statusCode"] == 400


def test_removing_clears_it_for_both(mod):
    mod.handler(event("PUT", "/me/friend-request", {"userId": B}), None)
    mod.handler(event("PUT", "/me/friend-accept", {"userId": A}, sub=B), None)

    mod.handler(event("DELETE", "/me/friend-remove", {"userId": B}), None)

    assert body_of(mod.handler(event(), None))["friends"] == []
    assert body_of(mod.handler(event(sub=B), None))["friends"] == []


def test_actor_comes_from_the_authorizer_not_the_body(mod):
    # Naming someone else as the actor must not make them the requester.
    mod.handler(
        event("PUT", "/me/friend-request", {"userId": B, "sub": "someone-else"}), None
    )

    assert len(body_of(mod.handler(event(), None))["outgoing"]) == 1
    assert body_of(mod.handler(event(sub="someone-else"), None))["outgoing"] == []


def test_missing_authorizer_context_is_rejected(mod):
    bare = event()
    bare["requestContext"] = {}

    assert mod.handler(bare, None)["statusCode"] == 401


def test_target_is_required(mod):
    response = mod.handler(event("PUT", "/me/friend-request", {}), None)

    assert response["statusCode"] == 400


def test_a_method_mismatch_is_rejected(mod):
    response = mod.handler(event("PUT", "/me/friend-remove", {"userId": B}), None)

    # The segment alone must not decide: a misconfigured route should fail
    # rather than quietly do the wrong thing to a relationship.
    assert response["statusCode"] == 400


def test_cannot_friend_yourself(mod):
    response = mod.handler(event("PUT", "/me/friend-request", {"userId": A}), None)

    assert response["statusCode"] == 400


def _sleeper(monkeypatch, mod, leagues, members):
    monkeypatch.setattr(
        mod.sleeper_helper, "get_nfl_state", lambda: {"season": "2026"}
    )
    monkeypatch.setattr(
        mod.sleeper_helper, "get_user_leagues", lambda user_id, season: leagues
    )
    monkeypatch.setattr(
        mod.sleeper_helper, "get_sleeper_league_users", lambda league_id: members
    )


def test_suggestions_are_off_unless_asked(mod, monkeypatch, tables):
    """The auth guard loads this graph on every navigation; it must stay cheap."""
    called = []
    monkeypatch.setattr(
        mod.sleeper_helper,
        "get_nfl_state",
        lambda: called.append(1) or {"season": "2026"},
    )

    payload = body_of(mod.handler(event(), None))

    assert payload["suggestions"] == []
    assert called == []


def _link_a(tables):
    tables.Table(PLATFORM_USERS_TABLE).put_item(
        Item={"userId": A, "email": "a@x.com", "displayName": "Ay", "sleeperUserId": "s-a"}
    )


def test_suggests_a_leaguemate_with_an_account(mod, monkeypatch, tables):
    _link_a(tables)
    tables.Table(PLATFORM_USERS_TABLE).update_item(
        Key={"userId": B},
        UpdateExpression="SET sleeperUserId = :s",
        ExpressionAttributeValues={":s": "s-b"},
    )
    _sleeper(
        monkeypatch,
        mod,
        [{"league_id": "L1"}],
        [{"user_id": "s-a"}, {"user_id": "s-b"}],
    )

    payload = body_of(
        mod.handler({**event(), "queryStringParameters": {"suggest": "1"}}, None)
    )

    assert [p["userId"] for p in payload["suggestions"]] == [B]
    assert payload["suggestions"][0]["displayName"] == "Bee"


def test_leaguemates_without_an_account_are_not_suggested(mod, monkeypatch, tables):
    _link_a(tables)
    _sleeper(
        monkeypatch,
        mod,
        [{"league_id": "L1"}],
        [{"user_id": "s-a"}, {"user_id": "s-stranger"}],
    )

    payload = body_of(
        mod.handler({**event(), "queryStringParameters": {"suggest": "1"}}, None)
    )

    # Suggesting them would confirm that a given Sleeper handle has no Xomper
    # account, which is the same leak in reverse.
    assert payload["suggestions"] == []


def test_existing_relationships_are_not_suggested(mod, monkeypatch, tables):
    _link_a(tables)
    tables.Table(PLATFORM_USERS_TABLE).update_item(
        Key={"userId": B},
        UpdateExpression="SET sleeperUserId = :s",
        ExpressionAttributeValues={":s": "s-b"},
    )
    _sleeper(monkeypatch, mod, [{"league_id": "L1"}], [{"user_id": "s-b"}])
    mod.handler(event("PUT", "/me/friend-request", {"userId": B}), None)

    payload = body_of(
        mod.handler({**event(), "queryStringParameters": {"suggest": "1"}}, None)
    )

    assert payload["suggestions"] == []
    assert len(payload["outgoing"]) == 1


def test_the_caller_is_never_suggested_to_themselves(mod, monkeypatch, tables):
    _link_a(tables)
    _sleeper(monkeypatch, mod, [{"league_id": "L1"}], [{"user_id": "s-a"}])

    payload = body_of(
        mod.handler({**event(), "queryStringParameters": {"suggest": "1"}}, None)
    )

    assert payload["suggestions"] == []


def test_sleeper_being_down_does_not_break_the_page(mod, monkeypatch, tables):
    _link_a(tables)

    def boom(*args, **kwargs):
        raise RuntimeError("sleeper is down")

    monkeypatch.setattr(mod.sleeper_helper, "get_nfl_state", boom)

    payload = body_of(
        mod.handler({**event(), "queryStringParameters": {"suggest": "1"}}, None)
    )

    # The graph itself is what matters; suggestions are a nicety.
    assert payload["suggestions"] == []
    assert payload["friends"] == []


def test_a_caller_with_no_linked_handle_gets_nothing(mod, monkeypatch, tables):
    _sleeper(monkeypatch, mod, [{"league_id": "L1"}], [{"user_id": "s-b"}])

    payload = body_of(
        mod.handler({**event(), "queryStringParameters": {"suggest": "1"}}, None)
    )

    assert payload["suggestions"] == []
