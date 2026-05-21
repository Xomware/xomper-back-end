"""
Tests for `lambdas.common.ai_memories_store`.

Uses `moto` to stand up an in-memory DynamoDB matching the schema
declared in the F3 plan / infra PR #106:
- PK `pk` (S), SK `sk` (S)
- PAY_PER_REQUEST, no GSI in v1

Covers happy-path write, list_recent ordering, cross-season
isolation, schema validation, and the season-purge utility.
"""
from __future__ import annotations

import importlib

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import AI_MEMORIES_TABLE


@pytest.fixture
def memories_table():
    """Spin up the AI memories table inside a moto-mocked DynamoDB
    matching the production schema (PK `pk` + SK `sk`, no GSI)."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName=AI_MEMORIES_TABLE,
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

        # Re-import the store under the active moto context so its
        # module-level `_dynamodb` resource points at the mocked
        # endpoint.
        import lambdas.common.ai_memories_store as store
        importlib.reload(store)
        yield store


class TestWriteMemory:
    def test_writes_with_correct_pk_sk(self, memories_table) -> None:
        store = memories_table
        item = store.write_memory(
            league_id="L1",
            season=2026,
            week=4,
            text="Connor benched LeBron and lost by 0.5",
            sentiment="roast",
            manager_user_id="u1",
        )
        assert item["pk"] == "LEAGUE#L1#SEASON#2026"
        assert item["sk"].startswith("MEMORY#04#")
        assert item["text"] == "Connor benched LeBron and lost by 0.5"
        assert item["sentiment"] == "roast"
        assert item["manager_user_id"] == "u1"
        assert item["week"] == 4
        assert item["season"] == "2026"
        assert item["memory_id"]
        assert item["created_at"].endswith("Z")

    def test_league_wide_memory_omits_manager_user_id(
        self, memories_table
    ) -> None:
        store = memories_table
        item = store.write_memory(
            league_id="L1",
            season=2026,
            week=2,
            text="League average dipped under 100 for the first time",
            sentiment="lore",
        )
        assert "manager_user_id" not in item

    def test_invalid_sentiment_raises(self, memories_table) -> None:
        store = memories_table
        from lambdas.common.errors import ValidationError

        with pytest.raises(ValidationError):
            store.write_memory(
                league_id="L1",
                season=2026,
                week=1,
                text="x",
                sentiment="snark",
            )

    def test_empty_league_id_raises(self, memories_table) -> None:
        store = memories_table
        from lambdas.common.errors import ValidationError

        with pytest.raises(ValidationError):
            store.write_memory(
                league_id="",
                season=2026,
                week=1,
                text="x",
                sentiment="roast",
            )

    def test_empty_text_raises(self, memories_table) -> None:
        store = memories_table
        from lambdas.common.errors import ValidationError

        with pytest.raises(ValidationError):
            store.write_memory(
                league_id="L1",
                season=2026,
                week=1,
                text="   ",
                sentiment="roast",
            )

    def test_text_is_trimmed(self, memories_table) -> None:
        store = memories_table
        item = store.write_memory(
            league_id="L1",
            season=2026,
            week=1,
            text="  surrounded by whitespace  ",
            sentiment="praise",
        )
        assert item["text"] == "surrounded by whitespace"


class TestListRecentMemories:
    def test_returns_newest_first_by_sk(self, memories_table) -> None:
        store = memories_table
        # Insert across weeks 1..5; later weeks should land at the
        # top of the descending-SK query.
        for week in (1, 2, 3, 4, 5):
            store.write_memory(
                league_id="L1",
                season=2026,
                week=week,
                text=f"week {week} memory",
                sentiment="roast",
            )

        rows = store.list_recent_memories("L1", 2026, limit=3)
        assert len(rows) == 3
        weeks = [r["week"] for r in rows]
        assert weeks == [5, 4, 3]

    def test_cross_season_isolation(self, memories_table) -> None:
        store = memories_table
        store.write_memory(
            league_id="L1",
            season=2025,
            week=10,
            text="last year",
            sentiment="lore",
        )
        store.write_memory(
            league_id="L1",
            season=2026,
            week=2,
            text="this year",
            sentiment="lore",
        )
        rows_2026 = store.list_recent_memories("L1", 2026, limit=10)
        assert len(rows_2026) == 1
        assert rows_2026[0]["text"] == "this year"
        rows_2025 = store.list_recent_memories("L1", 2025, limit=10)
        assert len(rows_2025) == 1
        assert rows_2025[0]["text"] == "last year"

    def test_empty_league_returns_empty(self, memories_table) -> None:
        store = memories_table
        assert store.list_recent_memories("", 2026, limit=6) == []

    def test_no_memories_returns_empty(self, memories_table) -> None:
        store = memories_table
        assert store.list_recent_memories("L_FRESH", 2026, limit=6) == []

    def test_limit_caps_returned_rows(self, memories_table) -> None:
        store = memories_table
        for i in range(10):
            store.write_memory(
                league_id="L1",
                season=2026,
                week=i + 1,
                text=f"m{i}",
                sentiment="roast",
            )
        rows = store.list_recent_memories("L1", 2026, limit=6)
        assert len(rows) == 6
        # Newest 6 = weeks 10..5.
        assert [r["week"] for r in rows] == [10, 9, 8, 7, 6, 5]


class TestDeleteSeasonMemories:
    def test_deletes_only_target_season(self, memories_table) -> None:
        store = memories_table
        for season in (2025, 2026, 2027):
            for week in (1, 2):
                store.write_memory(
                    league_id="L1",
                    season=season,
                    week=week,
                    text=f"s{season} w{week}",
                    sentiment="lore",
                )

        deleted = store.delete_season_memories("L1", 2026)
        assert deleted == 2
        # 2025 + 2027 still intact.
        assert (
            len(store.list_recent_memories("L1", 2025, limit=10)) == 2
        )
        assert (
            len(store.list_recent_memories("L1", 2027, limit=10)) == 2
        )
        # 2026 wiped.
        assert store.list_recent_memories("L1", 2026, limit=10) == []

    def test_no_rows_returns_zero(self, memories_table) -> None:
        store = memories_table
        deleted = store.delete_season_memories("L_FRESH", 2026)
        assert deleted == 0

    def test_empty_league_returns_zero(self, memories_table) -> None:
        store = memories_table
        assert store.delete_season_memories("", 2026) == 0
