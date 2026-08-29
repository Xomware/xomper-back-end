"""
Tests for `lambdas.common.social_store`.

A friendship is stored as two rows so that "my friends" and "who wants to be
my friend" are each one query. That redundancy is the whole design, and it is
only correct if every transition writes both sides — a half-written
friendship shows up for one person and not the other.
"""
from __future__ import annotations

import importlib

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import SOCIAL_TABLE
from lambdas.common.errors import ValidationError

A = "cog-a"
B = "cog-b"


@pytest.fixture
def social_table():
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
        yield dynamodb.Table(SOCIAL_TABLE)


@pytest.fixture
def store(social_table):
    from lambdas.common import social_store

    return importlib.reload(social_store)


def test_no_friends_to_start(store):
    assert store.list_friends(A) == []
    assert store.pending_count(A) == 0


def test_request_writes_both_directions(store):
    store.request_friend(A, B)

    # Without both rows, one side's list is silently wrong.
    assert store.get_friendship(A, B)["state"] == "outgoing"
    assert store.get_friendship(B, A)["state"] == "incoming"


def test_the_recipient_sees_it_pending(store):
    store.request_friend(A, B)

    assert store.pending_count(B) == 1
    assert store.pending_count(A) == 0


def test_accept_flips_both_sides(store):
    store.request_friend(A, B)

    store.accept_friend(B, A)

    assert store.get_friendship(A, B)["state"] == "accepted"
    assert store.get_friendship(B, A)["state"] == "accepted"
    assert store.pending_count(B) == 0


def test_only_the_recipient_can_accept(store):
    store.request_friend(A, B)

    # A asked; A cannot also answer.
    with pytest.raises(ValidationError):
        store.accept_friend(A, B)


def test_accepting_nothing_is_rejected(store):
    with pytest.raises(ValidationError):
        store.accept_friend(A, B)


def test_a_crossing_request_becomes_an_accept(store):
    store.request_friend(A, B)

    # B asks back without having seen the request. Leaving two people staring
    # at pending requests would be the wrong answer.
    result = store.request_friend(B, A)

    assert result["state"] == "accepted"
    assert store.get_friendship(A, B)["state"] == "accepted"


def test_cannot_friend_yourself(store):
    with pytest.raises(ValidationError):
        store.request_friend(A, A)


def test_duplicate_request_is_rejected(store):
    store.request_friend(A, B)

    with pytest.raises(ValidationError):
        store.request_friend(A, B)


def test_requesting_an_existing_friend_is_rejected(store):
    store.request_friend(A, B)
    store.accept_friend(B, A)

    with pytest.raises(ValidationError):
        store.request_friend(A, B)


def test_remove_clears_both_directions(store):
    store.request_friend(A, B)
    store.accept_friend(B, A)

    store.remove_friend(A, B)

    # Decline, cancel and unfriend are the same operation; a one-sided
    # removal would leave a ghost friend on the other list.
    assert store.get_friendship(A, B) is None
    assert store.get_friendship(B, A) is None


def test_remove_is_safe_when_nothing_exists(store):
    store.remove_friend(A, B)

    assert store.list_friends(A) == []


def test_listing_filters_by_state(store):
    store.request_friend(A, B)
    store.request_friend(A, "cog-c")
    store.accept_friend("cog-c", A)

    assert len(store.list_friends(A)) == 2
    assert len(store.list_friends(A, "accepted")) == 1
    assert len(store.list_friends(A, "outgoing")) == 1


def test_friendships_are_per_user(store):
    store.request_friend(A, B)

    assert store.list_friends("cog-unrelated") == []
