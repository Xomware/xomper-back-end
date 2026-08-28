"""
Tests for `lambdas.api_players_list.handler`.

This endpoint replaced a 14.6 MB download from Sleeper with a trimmed map.
Trimming is the whole point and also the whole risk: `PlayerService` falls
back to Sleeper only on a **network error**, so a field this endpoint quietly
omits is not a failure the frontend can detect. It just renders blanks.

That is exactly what happened — `player_id` was dropped because it is also
the map key, and every consumer that does `new PlayerModel(map[id])` got
`player_id: undefined`, which put `.../undefined.jpg` in every headshot URL
across eleven files.
"""
from __future__ import annotations

import importlib
import json

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import PLAYERS_TABLE_NAME

# Fields the Angular source reads off a player. Counted from usage, not
# guessed. A field leaving this list should be a deliberate edit with the
# frontend change that stopped needing it.
REQUIRED_FIELDS = {
    "player_id",
    "first_name",
    "last_name",
    "full_name",
    "position",
    "team",
    "status",
    "injury_status",
    "age",
    "years_exp",
    "number",
    "fantasy_positions",
    "height",
    "weight",
    "college",
    "search_full_name",
}


@pytest.fixture
def players_table():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName=PLAYERS_TABLE_NAME,
            KeySchema=[{"AttributeName": "playerId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "playerId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.put_item(
            Item={
                "playerId": "4984",
                "first_name": "Josh",
                "last_name": "Allen",
                "full_name": "Josh Allen",
                "position": "QB",
                "team": "BUF",
                "status": "Active",
                "injury_status": "Questionable",
                "age": 29,
                "years_exp": 8,
                "number": 17,
                "fantasy_positions": ["QB"],
                "height": "77",
                "weight": "237",
                "college": "Wyoming",
                "search_full_name": "joshallen",
            }
        )
        yield table


@pytest.fixture
def mod(players_table):
    from lambdas.api_players_list import handler as handler_mod

    return importlib.reload(handler_mod)


def body_of(response):
    return json.loads(response["body"])


def test_returns_the_map_keyed_by_player_id(mod):
    payload = body_of(mod.handler({}, None))

    assert payload["count"] == 1
    assert "4984" in payload["players"]


def test_every_record_carries_its_own_player_id(mod):
    player = body_of(mod.handler({}, None))["players"]["4984"]

    # Consumers build PlayerModel from the value alone. Without this every
    # headshot URL resolves to .../undefined.jpg.
    assert player["player_id"] == "4984"


def test_serves_every_field_the_frontend_reads(mod):
    player = body_of(mod.handler({}, None))["players"]["4984"]

    missing = REQUIRED_FIELDS - set(player)
    assert missing == set(), f"frontend reads these but the API omits them: {missing}"


def test_keeps_list_fields_as_lists(mod):
    player = body_of(mod.handler({}, None))["players"]["4984"]

    # str() on the way in would store "['QB']" and break position filtering.
    assert player["fantasy_positions"] == ["QB"]


def test_integral_numbers_do_not_serialise_as_floats(mod):
    player = body_of(mod.handler({}, None))["players"]["4984"]

    # DynamoDB hands numbers back as Decimal; age rendering as 29.0 is wrong.
    assert player["age"] == 29
    assert isinstance(player["age"], int)


def test_drops_the_internal_key_name(mod):
    player = body_of(mod.handler({}, None))["players"]["4984"]

    # `playerId` is the table's key name; the frontend contract is player_id.
    assert "playerId" not in player


def test_empty_table_returns_an_empty_map(mod, players_table):
    players_table.delete_item(Key={"playerId": "4984"})

    payload = body_of(mod.handler({}, None))

    assert payload["count"] == 0
    assert payload["players"] == {}
