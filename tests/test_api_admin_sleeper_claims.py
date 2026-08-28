"""
Tests for `lambdas.api_admin_sleeper_claims.handler`.

Sleeper linking is unverified by design, so more than one platform user can
claim the same handle. Users are never told; admins see it here. The
behaviour worth pinning is that a contested claim is actually detected and
surfaced first, and that a non-admin cannot read any of it.
"""
from __future__ import annotations

import importlib
import json

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import PLATFORM_USERS_TABLE


@pytest.fixture
def users_table():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName=PLATFORM_USERS_TABLE,
            KeySchema=[{"AttributeName": "userId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "userId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield table


@pytest.fixture
def mod(users_table):
    from lambdas.api_admin_sleeper_claims import handler as handler_mod

    return importlib.reload(handler_mod)


def seed(table, user_id, email, sleeper_id="", username="", updated="2026-01-01"):
    item = {"userId": user_id, "email": email, "updatedAt": updated}
    if sleeper_id:
        item["sleeperUserId"] = sleeper_id
        item["sleeperUsername"] = username
    table.put_item(Item=item)


def event(groups="admin", sub="admin-1"):
    return {
        "httpMethod": "GET",
        "path": "/admin/sleeper-claims",
        "requestContext": {
            "authorizer": {"sub": sub, "email": "a@x.com", "groups": groups}
        },
    }


def body_of(response):
    return json.loads(response["body"])


def test_non_admin_is_refused(mod, users_table):
    response = mod.handler(event(groups=""), None)

    assert response["statusCode"] == 401


def test_missing_authorizer_context_is_refused(mod, users_table):
    bare = event()
    bare["requestContext"] = {}

    assert mod.handler(bare, None)["statusCode"] == 401


def test_empty_table_reports_zeroes(mod, users_table):
    payload = body_of(mod.handler(event(), None))

    assert payload["totalUsers"] == 0
    assert payload["accounts"] == []


def test_counts_users_with_no_link(mod, users_table):
    seed(users_table, "u1", "a@x.com")
    seed(users_table, "u2", "b@x.com", "111", "dom")

    payload = body_of(mod.handler(event(), None))

    assert payload["totalUsers"] == 2
    assert payload["unlinkedUsers"] == 1
    assert payload["linkedAccounts"] == 1


def test_a_single_claim_is_not_contested(mod, users_table):
    seed(users_table, "u1", "a@x.com", "111", "dom")

    account = body_of(mod.handler(event(), None))["accounts"][0]

    assert account["claimCount"] == 1
    assert account["isContested"] is False


def test_detects_two_accounts_claiming_one_handle(mod, users_table):
    seed(users_table, "u1", "a@x.com", "111", "dom")
    seed(users_table, "u2", "b@x.com", "111", "dom")

    payload = body_of(mod.handler(event(), None))
    account = payload["accounts"][0]

    # The whole reason this endpoint exists.
    assert payload["contestedAccounts"] == 1
    assert account["claimCount"] == 2
    assert {c["email"] for c in account["claimants"]} == {"a@x.com", "b@x.com"}


def test_contested_accounts_sort_first(mod, users_table):
    seed(users_table, "u1", "a@x.com", "111", "solo")
    seed(users_table, "u2", "b@x.com", "222", "shared")
    seed(users_table, "u3", "c@x.com", "222", "shared")

    accounts = body_of(mod.handler(event(), None))["accounts"]

    # Otherwise the one thing worth looking at sorts into the middle of a
    # long list.
    assert accounts[0]["sleeperUserId"] == "222"


def test_claimants_are_ordered_by_when_they_linked(mod, users_table):
    seed(users_table, "later", "b@x.com", "111", "dom", updated="2026-06-01")
    seed(users_table, "first", "a@x.com", "111", "dom", updated="2026-01-01")

    claimants = body_of(mod.handler(event(), None))["accounts"][0]["claimants"]

    # Who got there first is the only ordering an admin can act on.
    assert [c["userId"] for c in claimants] == ["first", "later"]


def test_admin_group_among_several_is_accepted(mod, users_table):
    seed(users_table, "u1", "a@x.com", "111", "dom")

    response = mod.handler(event(groups="beta,admin"), None)

    assert response["statusCode"] == 200
