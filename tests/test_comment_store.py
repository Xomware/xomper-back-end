"""
Tests for `lambdas.common.comment_store`.

Comments share a table with friendships, separated by key prefix. The
orderings and the ownership check are the parts that would be quietly wrong:
a thread out of order reads as nonsense, and a missing author check lets
anyone delete anyone's words.
"""
from __future__ import annotations

import importlib

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import SOCIAL_TABLE
from lambdas.common.errors import NotFoundError, ValidationError

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
    from lambdas.common import comment_store

    return importlib.reload(comment_store)


def test_empty_thread(store):
    assert store.list_comments("league", "l1") == []


def test_add_and_read_back(store):
    store.add_comment(A, "league", "l1", "Rough week.")

    thread = store.list_comments("league", "l1")
    assert len(thread) == 1
    assert thread[0]["body"] == "Rough week."
    assert thread[0]["authorId"] == A


def test_thread_is_chronological(store):
    store.add_comment(A, "league", "l1", "first")
    store.add_comment(B, "league", "l1", "second")
    store.add_comment(A, "league", "l1", "third")

    bodies = [c["body"] for c in store.list_comments("league", "l1")]

    # Timestamp leads the sort key so order is free; out of order reads as
    # nonsense.
    assert bodies == ["first", "second", "third"]


def test_two_comments_in_the_same_instant_both_survive(store):
    # The uuid suffix exists precisely so these do not collide.
    for _ in range(5):
        store.add_comment(A, "league", "l1", "same ms")

    assert len(store.list_comments("league", "l1")) == 5


def test_threads_are_separate(store):
    store.add_comment(A, "league", "l1", "league talk")
    store.add_comment(A, "player", "4984", "player talk")

    assert len(store.list_comments("league", "l1")) == 1
    assert len(store.list_comments("player", "4984")) == 1


def test_comments_do_not_collide_with_friendships(store):
    from lambdas.common import social_store

    importlib.reload(social_store).request_friend(A, B)
    store.add_comment(A, "league", "l1", "hello")

    # Same table, different prefixes.
    assert len(store.list_comments("league", "l1")) == 1
    assert len(social_store.list_friends(A)) == 1


def test_empty_body_is_rejected(store):
    with pytest.raises(ValidationError):
        store.add_comment(A, "league", "l1", "   ")


def test_over_long_body_is_rejected(store):
    with pytest.raises(ValidationError):
        store.add_comment(A, "league", "l1", "x" * 1001)


def test_body_is_trimmed(store):
    store.add_comment(A, "league", "l1", "  padded  ")

    assert store.list_comments("league", "l1")[0]["body"] == "padded"


def test_unknown_target_type_is_rejected(store):
    with pytest.raises(ValidationError):
        store.add_comment(A, "nonsense", "x", "hi")


def test_mentions_are_stored_as_given(store):
    store.add_comment(A, "league", "l1", "hey @bee", mentions=[B])

    # Subs resolved by the client, not names parsed server-side: display
    # names are not unique.
    assert store.list_comments("league", "l1")[0]["mentions"] == [B]


def test_author_can_delete_their_own(store):
    comment = store.add_comment(A, "league", "l1", "oops")

    store.delete_comment(A, "league", "l1", comment["commentId"])

    assert store.list_comments("league", "l1") == []


def test_others_cannot_delete_it(store):
    comment = store.add_comment(A, "league", "l1", "mine")

    with pytest.raises(ValidationError):
        store.delete_comment(B, "league", "l1", comment["commentId"])

    assert len(store.list_comments("league", "l1")) == 1


def test_deleting_a_missing_comment_is_not_found(store):
    with pytest.raises(NotFoundError):
        store.delete_comment(A, "league", "l1", "no-such-id")


def test_like_and_unlike(store):
    comment = store.add_comment(A, "league", "l1", "good take")
    cid = comment["commentId"]

    store.set_reaction(B, cid, True)
    assert store.reactions_for([cid])[cid] == [B]

    store.set_reaction(B, cid, False)
    assert store.reactions_for([cid])[cid] == []


def test_liking_twice_counts_once(store):
    comment = store.add_comment(A, "league", "l1", "x")
    cid = comment["commentId"]

    store.set_reaction(B, cid, True)
    store.set_reaction(B, cid, True)

    # One row per person per comment; a double-tap must not count twice.
    assert store.reactions_for([cid])[cid] == [B]


def test_unliking_something_never_liked_is_a_no_op(store):
    comment = store.add_comment(A, "league", "l1", "x")

    store.set_reaction(B, comment["commentId"], False)

    assert store.reactions_for([comment["commentId"]])[comment["commentId"]] == []
