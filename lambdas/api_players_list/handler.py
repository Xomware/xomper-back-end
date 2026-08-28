"""
API — Player metadata
=====================
GET /players/list

Returns the slimmed player map keyed by Sleeper player id, in the same shape
the frontend already expects from Sleeper's own endpoint.

Why this exists: `https://api.sleeper.app/v1/players/nfl` is **14.6 MB**, and
the app downloads all of it on every session to resolve names and positions.
This table carries only the fields the Angular source actually reads, for the
positions the app values — roughly 4,300 players instead of every person
Sleeper has ever had on a roster.

The response is deliberately the same {playerId: {...}} map rather than a
list, so PlayerService can swap its source without reshaping anything
downstream.
"""
from decimal import Decimal
from typing import Any

import boto3

from lambdas.common.constants import PLAYERS_TABLE_NAME
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import success_response

HANDLER = "api_players_list"
log = get_logger(HANDLER)


def _scan_all(table: Any) -> list[dict[str, Any]]:
    """Full scan, following pagination.

    A scan is the right call here and not a smell: the endpoint returns the
    whole table by design, the table is ~4,300 small items, and it is rewritten
    wholesale by the nightly ingest. A query would need a partition key that
    exists only to avoid scanning something we always want in full.
    """
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {}
    while True:
        page = table.scan(**kwargs)
        items.extend(page.get("Items", []))
        last = page.get("LastEvaluatedKey")
        if not last:
            return items
        kwargs["ExclusiveStartKey"] = last


def _plain(value: Any) -> Any:
    """DynamoDB hands numbers back as Decimal, which json cannot encode.

    Integral values become int so `age: 25` does not serialise as `25.0`;
    anything fractional stays a float rather than being truncated to an int.
    """
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    table = boto3.resource("dynamodb").Table(PLAYERS_TABLE_NAME)
    items = _scan_all(table)

    players: dict[str, dict[str, Any]] = {}
    for item in items:
        player_id = item.get("playerId")
        if not player_id:
            continue
        record = {
            key: _plain(value)
            for key, value in item.items()
            if key != "playerId"
        }
        # Sleeper's own /players/nfl carries player_id inside each value, and
        # consumers build PlayerModel from the value alone -- `new
        # PlayerModel(map[id])`. Dropping it because it is also the map key
        # left player_id undefined across eleven files: every headshot URL
        # resolved to .../undefined.jpg, and any id-keyed lookup off a model
        # silently missed.
        record["player_id"] = str(player_id)
        players[str(player_id)] = record

    log.info(f"players: returning {len(players)} from {PLAYERS_TABLE_NAME}")
    return success_response({"count": len(players), "players": players})
