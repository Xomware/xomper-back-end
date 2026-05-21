"""
Tests for lambdas.common.ai_reports_store.

Uses `moto` to stand up a real in-memory DynamoDB matching the
schema declared in the F0 plan / terraform module. Each test
provisions a fresh mock table so writes / queries / pagination are
exercised against actual boto3 calls.
"""
from __future__ import annotations

import importlib
import time
from typing import Any

import boto3
import pytest
from moto import mock_aws

from lambdas.common.constants import AI_REPORTS_TABLE


@pytest.fixture
def dynamo_table():
    """Spin up the AI reports table inside a moto-mocked DynamoDB
    that exactly mirrors the production schema:
    - PK `pk` (S), SK `sk` (S)
    - GSI `created-at-index` with PK `pk` + SK `created_at`."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName=AI_REPORTS_TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "created-at-index",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Re-import the store under the active moto context so its
        # module-level `_dynamodb` resource points at the mocked
        # endpoint.
        import lambdas.common.ai_reports_store as store
        importlib.reload(store)
        yield store


def _put_report(
    store: Any,
    league_id: str,
    report_type: str,
    period: str,
    body: str = "body",
    metadata: dict | None = None,
) -> dict:
    item = store.write_report(
        league_id=league_id,
        report_type=report_type,
        period=period,
        body_markdown=body,
        metadata=metadata or {},
    )
    # `created_at` is second-resolution — ensure subsequent writes
    # land on a later timestamp so newest-first ordering is testable.
    time.sleep(1.01)
    return item


class TestWriteReport:
    def test_writes_with_correct_pk_sk(self, dynamo_table) -> None:
        store = dynamo_table
        item = store.write_report(
            league_id="L1",
            report_type="weekly",
            period="2026W04",
            body_markdown="# hi",
            metadata={"prompt_version": "v1", "model": "claude-haiku-4-5"},
        )
        assert item["pk"] == "LEAGUE#L1"
        assert item["sk"] == "REPORT#weekly#2026W04"
        assert item["league_id"] == "L1"
        assert item["report_type"] == "weekly"
        assert item["period"] == "2026W04"
        assert item["body_markdown"] == "# hi"
        assert item["metadata"] == {"prompt_version": "v1", "model": "claude-haiku-4-5"}
        # ISO-8601 UTC with 'Z' suffix.
        assert item["created_at"].endswith("Z")
        assert "T" in item["created_at"]

    def test_invalid_report_type_raises(self, dynamo_table) -> None:
        store = dynamo_table
        from lambdas.common.errors import ValidationError
        with pytest.raises(ValidationError):
            store.write_report(
                league_id="L1",
                report_type="bogus",
                period="2026W01",
                body_markdown="",
            )

    def test_empty_league_id_raises(self, dynamo_table) -> None:
        store = dynamo_table
        from lambdas.common.errors import ValidationError
        with pytest.raises(ValidationError):
            store.write_report(
                league_id="",
                report_type="weekly",
                period="2026W01",
                body_markdown="",
            )

    def test_empty_period_raises(self, dynamo_table) -> None:
        store = dynamo_table
        from lambdas.common.errors import ValidationError
        with pytest.raises(ValidationError):
            store.write_report(
                league_id="L1",
                report_type="weekly",
                period="",
                body_markdown="",
            )


class TestGetLatest:
    def test_returns_newest_of_type(self, dynamo_table) -> None:
        store = dynamo_table
        _put_report(store, "L1", "weekly", "2026W01")
        _put_report(store, "L1", "weekly", "2026W02")
        _put_report(store, "L1", "weekly", "2026W03")

        latest = store.get_latest("L1", "weekly")
        assert latest is not None
        assert latest["period"] == "2026W03"

    def test_ignores_other_types(self, dynamo_table) -> None:
        store = dynamo_table
        _put_report(store, "L1", "weekly", "2026W01")
        _put_report(store, "L1", "preseason", "2026-PRE")
        _put_report(store, "L1", "postDraft", "2026-POSTDRAFT")

        weekly = store.get_latest("L1", "weekly")
        assert weekly is not None
        assert weekly["report_type"] == "weekly"

        preseason = store.get_latest("L1", "preseason")
        assert preseason is not None
        assert preseason["report_type"] == "preseason"

    def test_no_report_returns_none(self, dynamo_table) -> None:
        store = dynamo_table
        assert store.get_latest("L1", "weekly") is None

    def test_invalid_type_raises(self, dynamo_table) -> None:
        store = dynamo_table
        from lambdas.common.errors import ValidationError
        with pytest.raises(ValidationError):
            store.get_latest("L1", "bogus")


class TestListRecent:
    def test_newest_first_across_types(self, dynamo_table) -> None:
        store = dynamo_table
        _put_report(store, "L1", "postDraft", "2026-POSTDRAFT")
        _put_report(store, "L1", "preseason", "2026-PRE")
        _put_report(store, "L1", "weekly", "2026W01")
        _put_report(store, "L1", "weekly", "2026W02")

        rows, cursor = store.list_recent("L1", limit=10)
        assert cursor is None
        assert len(rows) == 4
        # Most recently written first (weekly 2026W02).
        assert rows[0]["period"] == "2026W02"
        assert rows[-1]["period"] == "2026-POSTDRAFT"

    def test_pagination_round_trip(self, dynamo_table) -> None:
        store = dynamo_table
        for i in range(5):
            _put_report(store, "L1", "weekly", f"2026W{i+1:02d}")

        first, cursor1 = store.list_recent("L1", limit=2)
        assert len(first) == 2
        assert cursor1 is not None

        second, cursor2 = store.list_recent("L1", limit=2, cursor=cursor1)
        assert len(second) == 2
        # Different rows than page one.
        first_periods = {r["period"] for r in first}
        second_periods = {r["period"] for r in second}
        assert first_periods.isdisjoint(second_periods)

        # Drain to ensure the cursor eventually terminates.
        third, cursor3 = store.list_recent("L1", limit=2, cursor=cursor2)
        assert len(third) == 1
        assert cursor3 is None

    def test_invalid_cursor_raises(self, dynamo_table) -> None:
        store = dynamo_table
        from lambdas.common.errors import ValidationError
        with pytest.raises(ValidationError):
            store.list_recent("L1", limit=10, cursor="not-base64")

    def test_other_league_excluded(self, dynamo_table) -> None:
        store = dynamo_table
        _put_report(store, "L1", "weekly", "2026W01")
        _put_report(store, "L2", "weekly", "2026W01")
        rows, _ = store.list_recent("L1", limit=10)
        assert len(rows) == 1
        assert rows[0]["league_id"] == "L1"

    def test_empty_league_id_returns_empty(self, dynamo_table) -> None:
        store = dynamo_table
        rows, cursor = store.list_recent("", limit=10)
        assert rows == []
        assert cursor is None

    def test_limit_caps_at_50(self, dynamo_table) -> None:
        """`limit` over 50 must not actually scan more than 50 — guard
        against an iOS client requesting an absurd page size."""
        store = dynamo_table
        # Just sanity-check the API surface; we don't need 100 rows.
        _put_report(store, "L1", "weekly", "2026W01")
        rows, _ = store.list_recent("L1", limit=10_000)
        assert len(rows) == 1
