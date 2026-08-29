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

    assert payload == {"friends": [], "incoming": [], "outgoing": [], "pendingCount": 0}


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
