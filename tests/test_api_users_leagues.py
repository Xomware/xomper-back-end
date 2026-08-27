"""
Tests for `lambdas.api_users_leagues.handler`.

This endpoint is what makes Xomper multi-league. The frontend previously read
one hardcoded league id from its environment, so every user saw the same
league regardless of which ones they were actually in.

The league list is fetched from Sleeper live rather than stored: membership
changes without telling us. The follow set is stored, because Sleeper has no
concept of it.
"""
from __future__ import annotations

import importlib
import json

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import PLATFORM_FOLLOWS_TABLE, PLATFORM_USERS_TABLE

SLEEPER_LEAGUES = [
    {
        "league_id": "1394061072742227968",
        "name": "Smirnoff League",
        "season": "2026",
        "status": "pre_draft",
        "total_rosters": 14,
        "settings": {"type": 0},
    },
    {
        "league_id": "1317249551823814656",
        "name": "Charlotte Dynasty League",
        "season": "2026",
        "status": "in_season",
        "total_rosters": 12,
        "settings": {"type": 2},
    },
]


@pytest.fixture
def tables():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName=PLATFORM_USERS_TABLE,
            KeySchema=[{"AttributeName": "userId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "userId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
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
        yield dynamodb


@pytest.fixture
def mod(tables, monkeypatch):
    from lambdas.common import platform_follows, platform_users

    importlib.reload(platform_follows)
    importlib.reload(platform_users)
    from lambdas.api_users_leagues import handler as handler_mod

    handler_mod = importlib.reload(handler_mod)
    monkeypatch.setattr(handler_mod, "get_nfl_state", lambda: {"season": "2026"})
    monkeypatch.setattr(handler_mod, "get_user_leagues", lambda _uid, _s: SLEEPER_LEAGUES)

    # A linked user; without sleeperUserId there are no leagues to list.
    platform_users.ensure_user("cog-1", "d@x.com")
    platform_users.link_sleeper("cog-1", "594625531702460416", "domgiordano")
    return handler_mod


def event(method="GET", path="/me/leagues", body=None, sub="cog-1"):
    return {
        "httpMethod": method,
        "path": path,
        "body": json.dumps(body) if body is not None else None,
        "requestContext": {"authorizer": {"sub": sub, "email": "d@x.com", "provider": "cognito"}},
    }


def body_of(response):
    return json.loads(response["body"])


def test_lists_the_callers_leagues(mod):
    response = mod.handler(event(), None)

    assert response["statusCode"] == 200
    payload = body_of(response)
    assert payload["count"] == 2
    assert {l["leagueId"] for l in payload["leagues"]} == {
        "1394061072742227968",
        "1317249551823814656",
    }


def test_derives_dynasty_from_the_settings_type(mod):
    leagues = {l["leagueId"]: l for l in body_of(mod.handler(event(), None))["leagues"]}

    # The value provider routes on this: dynasty prices off FantasyCalc,
    # redraft off projections. Getting it wrong silently prices a league with
    # the wrong source.
    assert leagues["1317249551823814656"]["isDynasty"] is True
    assert leagues["1394061072742227968"]["isDynasty"] is False


def test_reports_follow_state(mod):
    mod.platform_follows.follow("cog-1", "1317249551823814656")

    leagues = {l["leagueId"]: l for l in body_of(mod.handler(event(), None))["leagues"]}

    assert leagues["1317249551823814656"]["isFollowed"] is True
    assert leagues["1394061072742227968"]["isFollowed"] is False


def test_sorts_followed_and_in_season_first(mod):
    mod.platform_follows.follow("cog-1", "1394061072742227968")

    order = [l["leagueId"] for l in body_of(mod.handler(event(), None))["leagues"]]

    # Followed beats in-season: the switcher shows what the user chose first.
    assert order[0] == "1394061072742227968"


def test_follow_then_list_marks_it_followed(mod):
    response = mod.handler(
        event(method="PUT", path="/me/follow", body={"leagueId": "1317249551823814656"}),
        None,
    )

    leagues = {l["leagueId"]: l for l in body_of(response)["leagues"]}
    assert leagues["1317249551823814656"]["isFollowed"] is True


def test_unfollow_clears_it(mod):
    mod.platform_follows.follow("cog-1", "1317249551823814656")

    response = mod.handler(
        event(method="DELETE", path="/me/unfollow", body={"leagueId": "1317249551823814656"}),
        None,
    )

    leagues = {l["leagueId"]: l for l in body_of(response)["leagues"]}
    assert leagues["1317249551823814656"]["isFollowed"] is False


def test_follow_requires_a_league_id(mod):
    response = mod.handler(event(method="PUT", path="/me/follow", body={}), None)

    assert response["statusCode"] == 400


def test_an_unlinked_user_gets_an_empty_list_not_an_error(mod):
    mod.platform_users.unlink_sleeper("cog-1")

    response = mod.handler(event(), None)

    # The frontend guard already routes unlinked users to the link page; this
    # endpoint should not be a second place that decides what unlinked means.
    assert response["statusCode"] == 200
    assert body_of(response)["leagues"] == []


def test_missing_authorizer_context_is_rejected(mod):
    bare = event()
    bare["requestContext"] = {}

    assert mod.handler(bare, None)["statusCode"] == 401


def test_identity_comes_from_the_authorizer_not_the_body(mod):
    mod.platform_follows.follow("cog-2", "1317249551823814656")

    leagues = {
        l["leagueId"]: l
        for l in body_of(mod.handler(event(body={"userId": "cog-2"}), None))["leagues"]
    }

    # cog-2's follow must not show up for cog-1.
    assert leagues["1317249551823814656"]["isFollowed"] is False


def test_response_does_not_pass_sleeper_fields_through(mod, monkeypatch):
    monkeypatch.setattr(
        mod,
        "get_user_leagues",
        lambda _uid, _s: [{**SLEEPER_LEAGUES[0], "draft_id": "secret", "scoring_settings": {}}],
    )

    league = body_of(mod.handler(event(), None))["leagues"][0]

    # Explicit allowlist: Sleeper's league object is large and mostly
    # irrelevant here, and the frontend re-fetches the full one it opens.
    assert "draft_id" not in league
    assert "scoring_settings" not in league
