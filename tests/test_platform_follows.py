"""
Tests for `lambdas.common.platform_follows`.

The follow set is the cost control: scheduled work iterates followed leagues,
so what is in this table bounds recurring spend. The two behaviours worth
pinning are that auto-follow does not resurrect a deliberate unfollow, and
that the GSI answers the cron's question (who follows this league) without a
scan.
"""
from __future__ import annotations

import importlib

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import PLATFORM_FOLLOWS_TABLE


@pytest.fixture
def follows_table():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
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
        yield dynamodb.Table(PLATFORM_FOLLOWS_TABLE)


@pytest.fixture
def store(follows_table):
    from lambdas.common import platform_follows

    return importlib.reload(platform_follows)


LEAGUES = [
    {"leagueId": "1317249551823814656", "name": "Charlotte Dynasty League", "season": "2026"},
    {"leagueId": "1389328793713250304", "name": "CLIT Fantasy Football", "season": "2026"},
]


def test_no_follows_for_a_new_user(store):
    assert store.list_for_user("cog-1") == []
    assert store.followed_league_ids("cog-1") == set()


def test_follow_then_list(store):
    store.follow("cog-1", "1317249551823814656", name="CLT", season="2026")

    items = store.list_for_user("cog-1")
    assert len(items) == 1
    assert items[0]["name"] == "CLT"
    assert items[0]["followedAt"]


def test_follow_is_idempotent(store):
    store.follow("cog-1", "abc")
    store.follow("cog-1", "abc")

    assert len(store.list_for_user("cog-1")) == 1


def test_unfollow_removes_only_that_league(store):
    store.follow_many("cog-1", LEAGUES)

    store.unfollow("cog-1", "1317249551823814656")

    assert store.followed_league_ids("cog-1") == {"1389328793713250304"}


def test_follow_many_adds_all_for_a_new_user(store):
    added = store.follow_many("cog-1", LEAGUES)

    assert added == 2
    assert len(store.followed_league_ids("cog-1")) == 2


def test_follow_many_skips_ones_already_followed(store):
    store.follow_many("cog-1", LEAGUES)

    added = store.follow_many("cog-1", LEAGUES)

    assert added == 0


def test_follow_many_does_not_resurrect_an_unfollow(store):
    store.follow_many("cog-1", LEAGUES)
    store.unfollow("cog-1", "1389328793713250304")

    # Re-linking a Sleeper account runs auto-follow again. It must not undo a
    # deliberate unfollow, or the league reappears every time and the user
    # has no way to keep it out.
    store.follow_many("cog-1", LEAGUES)

    assert store.followed_league_ids("cog-1") == {"1317249551823814656"}


def test_follows_are_per_user(store):
    store.follow("cog-1", "abc")
    store.follow("cog-2", "xyz")

    assert store.followed_league_ids("cog-1") == {"abc"}
    assert store.followed_league_ids("cog-2") == {"xyz"}


def test_followers_of_answers_the_cron_question(store):
    store.follow("cog-1", "shared-league")
    store.follow("cog-2", "shared-league")
    store.follow("cog-3", "other-league")

    followers = store.followers_of("shared-league")

    # This is the read path for every scheduled job: league -> who cares.
    assert sorted(followers) == ["cog-1", "cog-2"]


def test_followers_of_is_empty_for_an_unfollowed_league(store):
    assert store.followers_of("nobody-follows-this") == []


def test_refollowing_clears_the_tombstone(store):
    store.follow("cog-1", "abc")
    store.unfollow("cog-1", "abc")

    store.follow("cog-1", "abc")

    assert store.followed_league_ids("cog-1") == {"abc"}
    assert store.followers_of("abc") == ["cog-1"]


def test_an_unfollowed_league_drops_out_of_the_cron_read(store):
    store.follow("cog-1", "abc")
    store.follow("cog-2", "abc")

    store.unfollow("cog-1", "abc")

    # A tombstone must not keep a user on the cron's list, or unfollowing
    # stops meaning anything for cost.
    assert store.followers_of("abc") == ["cog-2"]
