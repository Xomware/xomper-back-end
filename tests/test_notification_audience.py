"""
Tests for `lambdas.common.notification_audience`.

This is what makes a scheduled job multi-league. The failures that matter are
the ones that would email the wrong person or nobody: a follower with no
address counted as reachable, a league nobody follows still costing a fan-out,
or one league's followers leaking into another's audience.
"""
from __future__ import annotations

import importlib

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import PLATFORM_FOLLOWS_TABLE, PLATFORM_USERS_TABLE


@pytest.fixture
def tables():
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
        dynamodb.create_table(
            TableName=PLATFORM_USERS_TABLE,
            KeySchema=[{"AttributeName": "userId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "userId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield dynamodb


@pytest.fixture
def mod(tables):
    from lambdas.common import platform_follows, platform_users

    importlib.reload(platform_follows)
    importlib.reload(platform_users)
    from lambdas.common import notification_audience

    return importlib.reload(notification_audience)


def user(tables, user_id, email="a@x.com", handle="ahandle", name=None):
    item = {"userId": user_id, "displayName": name or handle}
    if email:
        item["email"] = email
    if handle:
        item["sleeperUsername"] = handle
        item["sleeperUserId"] = f"s-{user_id}"
    tables.Table(PLATFORM_USERS_TABLE).put_item(Item=item)


def follow(mod, user_id, league_id):
    from lambdas.common import platform_follows

    platform_follows.follow(user_id, league_id)


def test_no_follows_means_no_work(mod):
    assert mod.audiences() == []


def test_one_entry_per_followed_league(mod, tables):
    user(tables, "cog-1")
    user(tables, "cog-2", email="b@x.com")
    follow(mod, "cog-1", "L1")
    follow(mod, "cog-2", "L1")
    follow(mod, "cog-2", "L2")

    found = {a.league_id: sorted(r.user_id for r in a.recipients) for a in mod.audiences()}

    assert found == {"L1": ["cog-1", "cog-2"], "L2": ["cog-2"]}


def test_a_follower_with_no_address_is_not_an_audience(mod, tables):
    user(tables, "cog-1", email="")
    follow(mod, "cog-1", "L1")

    # The league drops out entirely rather than returning an Audience a
    # caller would fan out for and then send nothing.
    assert mod.audiences() == []


def test_a_reachable_follower_keeps_the_league(mod, tables):
    user(tables, "cog-1", email="")
    user(tables, "cog-2", email="b@x.com")
    follow(mod, "cog-1", "L1")
    follow(mod, "cog-2", "L1")

    [audience] = mod.audiences()
    assert [r.user_id for r in audience.recipients] == ["cog-2"]


def test_a_follower_with_no_platform_record_is_skipped(mod):
    follow(mod, "ghost", "L1")

    assert mod.audiences() == []


def test_unfollowing_removes_the_league(mod, tables):
    from lambdas.common import platform_follows

    user(tables, "cog-1")
    follow(mod, "cog-1", "L1")
    platform_follows.unfollow("cog-1", "L1")

    assert mod.audiences() == []


def test_recipients_are_keyed_by_sleeper_id_for_roster_matching(mod, tables):
    user(tables, "cog-1")
    follow(mod, "cog-1", "L1")

    [audience] = mod.audiences()

    # Sleeper rosters carry owner_id, so this is the direction a job needs.
    assert audience.by_sleeper_id["s-cog-1"].email == "a@x.com"


def test_someone_without_a_linked_handle_is_not_keyed_on_empty(mod, tables):
    user(tables, "cog-1", handle="")
    follow(mod, "cog-1", "L1")

    [audience] = mod.audiences()

    assert audience.recipients[0].email == "a@x.com"
    assert audience.by_sleeper_id == {}


def test_audience_for_answers_a_single_league(mod, tables):
    user(tables, "cog-1")
    user(tables, "cog-2", email="b@x.com")
    follow(mod, "cog-1", "L1")
    follow(mod, "cog-2", "L2")

    audience = mod.audience_for("L1")

    assert [r.user_id for r in audience.recipients] == ["cog-1"]
    assert mod.audience_for("L2").recipients[0].user_id == "cog-2"
    assert mod.audience_for("nobody-follows-this") is None


def test_display_name_falls_back_to_the_handle(mod, tables):
    tables.Table(PLATFORM_USERS_TABLE).put_item(
        Item={"userId": "cog-1", "email": "a@x.com", "sleeperUsername": "onlyhandle"}
    )
    follow(mod, "cog-1", "L1")

    [audience] = mod.audiences()
    assert audience.recipients[0].display_name == "onlyhandle"


@pytest.fixture
def whitelist(mod, monkeypatch):
    """Stand in for the Supabase whitelist the crons read today."""
    state = {"league": {"league_id": "CLT", "league_name": "Charlotte Dynasty"}, "users": []}
    monkeypatch.setattr(mod, "get_active_whitelisted_league", lambda: state["league"])
    monkeypatch.setattr(mod, "get_active_whitelisted_users", lambda: state["users"])
    return state


def test_the_whitelisted_league_keeps_its_own_recipients(mod, tables, whitelist):
    whitelist["users"] = [{"email": "clt@x.com", "sleeper_user_id": "s-clt"}]
    user(tables, "cog-1")
    follow(mod, "cog-1", "L1")

    jobs = {j.league_id: j for j in mod.jobs()}

    # CLT members never signed up for Xomper; switching them to followers
    # mid-season would silently stop email they already get.
    assert jobs["CLT"].source == "whitelist"
    assert jobs["CLT"].recipients == whitelist["users"]
    assert jobs["L1"].source == "follows"


def test_a_league_in_both_sources_is_emitted_once(mod, tables, whitelist):
    whitelist["users"] = [{"email": "clt@x.com", "sleeper_user_id": "s-clt"}]
    user(tables, "cog-1")
    follow(mod, "cog-1", "CLT")

    jobs = mod.jobs()

    # Otherwise a follower of the whitelisted league is mailed twice.
    assert [j.league_id for j in jobs] == ["CLT"]
    assert jobs[0].source == "whitelist"


def test_follows_recipients_arrive_in_whitelist_shape(mod, tables, whitelist):
    whitelist["league"] = None
    user(tables, "cog-1", email="a@x.com", handle="ahandle")
    follow(mod, "cog-1", "L1")

    [job] = mod.jobs()

    # Every cron indexes recipients by sleeper_user_id and filter_to_admin_only
    # reads the same field, so the shape has to match whatever the source.
    assert job.recipients == [
        {
            "email": "a@x.com",
            "display_name": "ahandle",
            "sleeper_user_id": "s-cog-1",
            "user_id": "cog-1",
        }
    ]


def test_the_league_name_comes_from_the_follow_row(mod, tables, whitelist):
    from lambdas.common import platform_follows

    whitelist["league"] = None
    user(tables, "cog-1")
    platform_follows.follow("cog-1", "L1", name="Sunday Money")

    [job] = mod.jobs()
    assert job.league_name == "Sunday Money"


def test_no_whitelist_league_still_yields_the_followed_ones(mod, tables, whitelist):
    whitelist["league"] = None
    user(tables, "cog-1")
    follow(mod, "cog-1", "L1")

    assert [j.league_id for j in mod.jobs()] == ["L1"]


def test_nothing_anywhere_is_no_jobs(mod, whitelist):
    whitelist["league"] = None

    assert mod.jobs() == []
