"""
Tests for `lambdas.api_comments.handler`.

The handler's job is attribution and rendering: the author is whoever the
authorizer says, and each row carries enough for the client to draw it
without resolving anyone. Both are the kind of thing that fails quietly.
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
        users.put_item(Item={"userId": A, "displayName": "Ay", "sleeperUsername": "ayy"})
        yield dynamodb


@pytest.fixture
def mod(tables):
    from lambdas.common import comment_store, platform_users

    importlib.reload(comment_store)
    importlib.reload(platform_users)
    from lambdas.api_comments import handler as handler_mod

    return importlib.reload(handler_mod)


def event(method="GET", path="/comments/list", body=None, query=None, sub=A):
    return {
        "httpMethod": method,
        "path": path,
        "body": json.dumps(body) if body is not None else None,
        "queryStringParameters": query,
        "requestContext": {"authorizer": {"sub": sub, "email": "a@x.com", "provider": "cognito"}},
    }


def body_of(response):
    return json.loads(response["body"])


LEAGUE = {"targetType": "league", "targetId": "l1"}


def test_empty_thread(mod):
    payload = body_of(mod.handler(event(query=LEAGUE), None))

    assert payload["count"] == 0
    assert payload["comments"] == []


def test_adding_returns_the_thread(mod):
    payload = body_of(
        mod.handler(event("PUT", "/comments/add", {**LEAGUE, "body": "hello"}), None)
    )

    assert payload["count"] == 1
    assert payload["comments"][0]["body"] == "hello"


def test_author_comes_from_the_authorizer(mod):
    # Naming someone else in the payload must not attribute the comment.
    mod.handler(
        event("PUT", "/comments/add", {**LEAGUE, "body": "x", "authorId": B}), None
    )

    comment = body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]
    assert comment["author"]["userId"] == A


def test_rows_carry_the_display_name(mod):
    mod.handler(event("PUT", "/comments/add", {**LEAGUE, "body": "x"}), None)

    comment = body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]
    # displayName, not the unverified Sleeper handle.
    assert comment["author"]["displayName"] == "Ay"


def test_an_unknown_author_still_renders(mod):
    mod.handler(event("PUT", "/comments/add", {**LEAGUE, "body": "x"}, sub="ghost"), None)

    comment = body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]
    assert comment["author"]["displayName"] == "Someone"


def test_mine_marks_only_your_own(mod):
    mod.handler(event("PUT", "/comments/add", {**LEAGUE, "body": "mine"}), None)

    as_a = body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]
    as_b = body_of(mod.handler(event(query=LEAGUE, sub=B), None))["comments"][0]

    # Drives whether a delete control is offered.
    assert as_a["mine"] is True
    assert as_b["mine"] is False


def test_likes_are_counted_and_attributed(mod):
    mod.handler(event("PUT", "/comments/add", {**LEAGUE, "body": "x"}), None)
    cid = body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]["commentId"]

    mod.handler(
        event("PUT", "/comments/react", {**LEAGUE, "commentId": cid, "liked": True}, sub=B),
        None,
    )

    as_b = body_of(mod.handler(event(query=LEAGUE, sub=B), None))["comments"][0]
    as_a = body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]

    assert as_b["likeCount"] == 1
    # likedByMe saves the client searching a list of ids to draw one button.
    assert as_b["likedByMe"] is True
    assert as_a["likedByMe"] is False


def test_unliking_drops_the_count(mod):
    mod.handler(event("PUT", "/comments/add", {**LEAGUE, "body": "x"}), None)
    cid = body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]["commentId"]
    mod.handler(
        event("PUT", "/comments/react", {**LEAGUE, "commentId": cid, "liked": True}, sub=B),
        None,
    )

    mod.handler(
        event("PUT", "/comments/react", {**LEAGUE, "commentId": cid, "liked": False}, sub=B),
        None,
    )

    assert body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]["likeCount"] == 0


def test_only_the_author_can_delete(mod):
    mod.handler(event("PUT", "/comments/add", {**LEAGUE, "body": "mine"}), None)
    cid = body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]["commentId"]

    response = mod.handler(
        event("DELETE", "/comments/delete", {**LEAGUE, "commentId": cid}, sub=B), None
    )

    assert response["statusCode"] == 400
    assert body_of(mod.handler(event(query=LEAGUE), None))["count"] == 1


def test_the_author_can_delete(mod):
    mod.handler(event("PUT", "/comments/add", {**LEAGUE, "body": "oops"}), None)
    cid = body_of(mod.handler(event(query=LEAGUE), None))["comments"][0]["commentId"]

    mod.handler(event("DELETE", "/comments/delete", {**LEAGUE, "commentId": cid}), None)

    assert body_of(mod.handler(event(query=LEAGUE), None))["count"] == 0


def test_target_is_required(mod):
    assert mod.handler(event(query={}), None)["statusCode"] == 400


def test_missing_authorizer_context_is_rejected(mod):
    bare = event(query=LEAGUE)
    bare["requestContext"] = {}

    assert mod.handler(bare, None)["statusCode"] == 401


def test_method_mismatch_is_rejected(mod):
    response = mod.handler(event("PUT", "/comments/delete", {**LEAGUE}), None)

    assert response["statusCode"] == 400


def test_threads_stay_separate(mod):
    mod.handler(event("PUT", "/comments/add", {**LEAGUE, "body": "league"}), None)
    mod.handler(
        event("PUT", "/comments/add", {"targetType": "player", "targetId": "4984", "body": "player"}),
        None,
    )

    assert body_of(mod.handler(event(query=LEAGUE), None))["count"] == 1
