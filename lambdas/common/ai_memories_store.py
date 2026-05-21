"""
AI Memories Store
=================
DynamoDB read/write helpers for the `xomper-ai-memories` table that
backs the F3 weekly recap's season-memory loop. Each weekly run:

1. Reads the most-recent N memories via `list_recent_memories(...)`
   and feeds them into the user prompt as context.
2. Writes 3-5 new memories Claude produced from this week via
   `write_memory(...)` so next week's recap can reference them.

Schema
------
- Table: `xomper-ai-memories` (`constants.AI_MEMORIES_TABLE`)
- Primary key:
    - PK (`pk`): `LEAGUE#<league_id>#SEASON#<year>`
      Season lives in the PK so a 2027 season starts as an empty
      query result without manual purge.
    - SK (`sk`): `MEMORY#<week:02d>#<memory_id>`
      Week is zero-padded so lexical SK ordering matches numeric
      week ordering; uuid suffix avoids collisions within a week.
- Attributes:
    - `memory_id` (S): uuid4 hex.
    - `season` (N): 2026, 2027, ...
    - `week` (N): 1-22 (regular season + playoffs).
    - `manager_user_id` (S | absent): Sleeper user_id the memory is
      primarily about. Absent for league-wide memories.
    - `text` (S): one-line memory ("Connor benched LeBron James and
      lost by 0.5").
    - `sentiment` (S): "roast" | "praise" | "lore".
    - `created_at` (S): ISO 8601 UTC string.

Querying newest-first
---------------------
`list_recent_memories(...)` runs a single Query against the base
table with `ScanIndexForward=False` and a `Limit`. SK encodes week
+ uuid, so the most-recent week comes back first and ties (multiple
memories in the same week) break lexically on uuid. The lookback
window is capped via `Limit` so token cost stays predictable.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from lambdas.common.constants import AI_MEMORIES_TABLE, AWS_DEFAULT_REGION
from lambdas.common.errors import MemoryStoreError, ValidationError
from lambdas.common.logger import get_logger

log = get_logger(__file__)

# Allowed sentiment values. Anything outside this set is rejected at
# write time so prompt drift doesn't quietly pollute the store.
SENTIMENTS: tuple[str, ...] = ("roast", "praise", "lore")

# Cached DynamoDB resource — module-level so warm Lambda containers
# don't pay the boto3 init cost per invocation.
_dynamodb = boto3.resource("dynamodb", region_name=AWS_DEFAULT_REGION)


def _table() -> Any:
    return _dynamodb.Table(AI_MEMORIES_TABLE)


def _pk(league_id: str, season: str | int) -> str:
    return f"LEAGUE#{league_id}#SEASON#{season}"


def _sk(week: int, memory_id: str) -> str:
    return f"MEMORY#{int(week):02d}#{memory_id}"


def _validate_sentiment(sentiment: str) -> None:
    if sentiment not in SENTIMENTS:
        raise ValidationError(
            message=(
                f"Invalid sentiment '{sentiment}'. "
                f"Must be one of {SENTIMENTS}."
            ),
            handler="ai_memories_store",
            function="_validate_sentiment",
            field="sentiment",
        )


def write_memory(
    league_id: str,
    season: str | int,
    week: int,
    text: str,
    sentiment: str,
    manager_user_id: str | None = None,
) -> dict[str, Any]:
    """Persist a single season memory. Returns the item that was
    written (handy for tests + logging).

    Args:
        league_id: Sleeper league id.
        season: NFL season year (int or string — both stored
            consistently in the PK).
        week: NFL week (1-22). Zero-padded into the SK.
        text: One-line memory body (~120 char target, ≤200 cap).
        sentiment: One of `"roast" | "praise" | "lore"`.
        manager_user_id: Optional Sleeper user_id this memory is
            primarily about. Absent / None = league-wide.

    Raises:
        ValidationError: invalid inputs (empty league/text, bad
            sentiment).
        MemoryStoreError: on Dynamo failure.
    """
    if not league_id:
        raise ValidationError(
            message="write_memory requires a non-empty league_id",
            handler="ai_memories_store",
            function="write_memory",
            field="league_id",
        )
    if not text or not text.strip():
        raise ValidationError(
            message="write_memory requires non-empty text",
            handler="ai_memories_store",
            function="write_memory",
            field="text",
        )
    _validate_sentiment(sentiment)

    memory_id = uuid.uuid4().hex
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    item: dict[str, Any] = {
        "pk": _pk(league_id, season),
        "sk": _sk(week, memory_id),
        "memory_id": memory_id,
        "season": str(season),
        "week": int(week),
        "text": text.strip(),
        "sentiment": sentiment,
        "created_at": created_at,
    }
    if manager_user_id:
        item["manager_user_id"] = manager_user_id

    try:
        _table().put_item(Item=item)
    except Exception as err:
        raise MemoryStoreError(
            message=f"write_memory failed: {err}",
            function="write_memory",
            table=AI_MEMORIES_TABLE,
        ) from err

    log.info(
        f"ai_memories_store.write_memory: league={league_id} "
        f"season={season} week={week} sentiment={sentiment} "
        f"memory_id={memory_id}"
    )
    return item


def list_recent_memories(
    league_id: str,
    season: str | int,
    limit: int = 6,
) -> list[dict[str, Any]]:
    """Return the N most recent memories for a league + season,
    newest first (by SK lexical order, which is week-desc then
    uuid-desc within the same week).

    Returns an empty list when nothing has been written yet (e.g.
    Week 1's first run, or a fresh 2027 season). Cross-season
    isolation is enforced by the PK — 2026 memories never bleed
    into a 2027 query.

    Args:
        league_id: Sleeper league id.
        season: NFL season year.
        limit: Maximum entries to return (default 6 — matches
            `AI_REVIEW_WEEKLY_MEMORY_LOOKBACK`).

    Raises:
        MemoryStoreError: on Dynamo failure.
    """
    if not league_id:
        return []
    safe_limit = max(1, int(limit or 6))

    try:
        response = _table().query(
            KeyConditionExpression=Key("pk").eq(_pk(league_id, season)),
            ScanIndexForward=False,
            Limit=safe_limit,
        )
    except Exception as err:
        raise MemoryStoreError(
            message=f"list_recent_memories failed: {err}",
            function="list_recent_memories",
            table=AI_MEMORIES_TABLE,
        ) from err

    return response.get("Items") or []


def delete_season_memories(league_id: str, season: str | int) -> int:
    """Delete every memory row for a single league + season. Returns
    the number of rows deleted. Intended as an admin-only purge
    utility for end-of-season cleanup — not called by the cron path.

    Performs a paginated Query then a BatchWriteItem in chunks of 25
    (Dynamo's limit). Errors surface as `MemoryStoreError`.
    """
    if not league_id:
        return 0

    deleted = 0
    table = _table()
    pk_value = _pk(league_id, season)
    last_key: dict[str, Any] | None = None

    try:
        while True:
            kwargs: dict[str, Any] = {
                "KeyConditionExpression": Key("pk").eq(pk_value),
                "ProjectionExpression": "pk, sk",
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            response = table.query(**kwargs)
            items = response.get("Items") or []
            for chunk_start in range(0, len(items), 25):
                chunk = items[chunk_start : chunk_start + 25]
                with table.batch_writer() as batch:
                    for row in chunk:
                        batch.delete_item(Key={"pk": row["pk"], "sk": row["sk"]})
                deleted += len(chunk)
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
    except Exception as err:
        raise MemoryStoreError(
            message=f"delete_season_memories failed: {err}",
            function="delete_season_memories",
            table=AI_MEMORIES_TABLE,
        ) from err

    log.info(
        f"ai_memories_store.delete_season_memories: league={league_id} "
        f"season={season} deleted={deleted}"
    )
    return deleted
