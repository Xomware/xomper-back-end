"""
AI Reports Store
================
DynamoDB read/write helpers for the `xomper-ai-reports` table that
backs the AI Review feature (F1 post-draft / F2 preseason / F3
weekly). All three generator lambdas write through `write_report`;
the two read endpoints (`api_ai_reports_latest`,
`api_ai_reports_list`) query via `get_latest` and `list_recent`.

Schema
------
- Table: `xomper-ai-reports` (`constants.AI_REPORTS_TABLE`)
- Primary key:
    - PK (`pk`): `LEAGUE#<league_id>`
    - SK (`sk`): `REPORT#<report_type>#<period>`
      where `period` is e.g. `2026-POSTDRAFT`, `2026-PRE`,
      `2026W01`...`2026W17`.
- GSI: `created-at-index`
    - GSI PK (`pk`): `LEAGUE#<league_id>` (same as base PK)
    - GSI SK (`created_at`): ISO-8601 UTC string,
      e.g. `2026-05-21T15:42:11Z`.
- Attributes:
    - `body_markdown` (S): the rendered report body.
    - `metadata` (M): structured metadata
      `{prompt_version, model, token_usage_in, token_usage_out,
        source_data_keys, ...}`.
    - `report_type` (S): `postDraft` | `preseason` | `weekly` | `mock`.
    - `period` (S): the same period string used in `sk`.
    - `league_id` (S): convenience copy for read paths.

Querying newest-first
---------------------
- Latest of a single type:
    `Query(pk=LEAGUE#X, sk begins_with REPORT#<type>#,
           ScanIndexForward=False, Limit=1)`.
- Cross-type archive newest-first:
    `Query(IndexName=created-at-index, pk=LEAGUE#X,
           ScanIndexForward=False, ExclusiveStartKey=cursor)`.
"""
from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from lambdas.common.constants import AI_REPORTS_TABLE, AWS_DEFAULT_REGION
from lambdas.common.errors import DynamoDBError, ValidationError
from lambdas.common.logger import get_logger

log = get_logger(__file__)

# Allowed report types. Keep this tight — F1/F2/F3 each use exactly
# one of postDraft/preseason/weekly. `mock` is a read-only surface
# for pre-seeded mock draft reports (iOS Draft tab mock cards); no
# generator lambda writes this type — backfilled directly via boto3.
REPORT_TYPES: tuple[str, ...] = ("postDraft", "preseason", "weekly", "weekPreview", "mock")

GSI_CREATED_AT = "created-at-index"

# Cached DynamoDB resource. Module-level so warm Lambda containers
# don't pay the boto3 init cost per invocation.
_dynamodb = boto3.resource("dynamodb", region_name=AWS_DEFAULT_REGION)


def _table() -> Any:
    return _dynamodb.Table(AI_REPORTS_TABLE)


def _pk(league_id: str) -> str:
    return f"LEAGUE#{league_id}"


def _sk(report_type: str, period: str) -> str:
    return f"REPORT#{report_type}#{period}"


def _validate_report_type(report_type: str) -> None:
    if report_type not in REPORT_TYPES:
        raise ValidationError(
            message=(
                f"Invalid report_type '{report_type}'. "
                f"Must be one of {REPORT_TYPES}."
            ),
            handler="ai_reports_store",
            function="_validate_report_type",
            field="report_type",
        )


def _encode_cursor(last_evaluated_key: dict[str, Any]) -> str:
    """DynamoDB returns a dict cursor; we surface it as a single
    URL-safe string the iOS client can pass back verbatim."""
    raw = json.dumps(last_evaluated_key, default=str, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_cursor(cursor: str) -> dict[str, Any]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        decoded = json.loads(raw)
        if not isinstance(decoded, dict):
            raise ValueError("cursor payload is not an object")
        return decoded
    except Exception as err:
        raise ValidationError(
            message=f"Invalid pagination cursor: {err}",
            handler="ai_reports_store",
            function="_decode_cursor",
            field="cursor",
        ) from err


def write_report(
    league_id: str,
    report_type: str,
    period: str,
    body_markdown: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist a generated report. Overwrites any existing report
    with the same (league_id, report_type, period) — F1/F2/F3 may
    re-run a generation, and we always want the most recent body.

    Returns the item that was written (handy for tests + logging).
    """
    _validate_report_type(report_type)
    if not league_id:
        raise ValidationError(
            message="write_report requires a non-empty league_id",
            handler="ai_reports_store",
            function="write_report",
            field="league_id",
        )
    if not period:
        raise ValidationError(
            message="write_report requires a non-empty period",
            handler="ai_reports_store",
            function="write_report",
            field="period",
        )

    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    item: dict[str, Any] = {
        "pk": _pk(league_id),
        "sk": _sk(report_type, period),
        "league_id": league_id,
        "report_type": report_type,
        "period": period,
        "body_markdown": body_markdown or "",
        "metadata": metadata or {},
        "created_at": created_at,
    }

    try:
        _table().put_item(Item=item)
    except Exception as err:
        raise DynamoDBError(
            message=f"write_report failed: {err}",
            function="write_report",
            table=AI_REPORTS_TABLE,
        ) from err

    log.info(
        f"ai_reports_store.write_report: league={league_id} "
        f"type={report_type} period={period} created_at={created_at}"
    )
    return item


def get_report(
    league_id: str,
    report_type: str,
    period: str,
) -> dict[str, Any] | None:
    """Return a single report by its exact composite key, or `None` if
    no row exists for that `(league_id, report_type, period)`.

    Used by read-only surfaces that need to look up a specific report
    without scanning (e.g. admin-portal F1's test-email handler, which
    accepts a `report_id = "{pk}|{sk}"` from the iOS client and fans
    a single rendered email out to one whitelisted user).

    This is a `GetItem` — exactly one Dynamo round-trip, no Query
    overhead.
    """
    _validate_report_type(report_type)
    if not league_id or not period:
        return None

    try:
        response = _table().get_item(
            Key={"pk": _pk(league_id), "sk": _sk(report_type, period)},
        )
    except Exception as err:
        raise DynamoDBError(
            message=f"get_report failed: {err}",
            function="get_report",
            table=AI_REPORTS_TABLE,
        ) from err

    return response.get("Item")


def get_latest(league_id: str, report_type: str) -> dict[str, Any] | None:
    """Return the most recent report for the given league + type, or
    None if no report exists yet.

    Uses the base table's `pk` + `sk` schema directly:
        Query(pk = LEAGUE#X, sk begins_with REPORT#<type>#)
    with `ScanIndexForward=False` so the newest SK (highest period
    string) comes first.
    """
    _validate_report_type(report_type)
    if not league_id:
        return None

    try:
        response = _table().query(
            KeyConditionExpression=(
                Key("pk").eq(_pk(league_id))
                & Key("sk").begins_with(f"REPORT#{report_type}#")
            ),
            ScanIndexForward=False,
            Limit=1,
        )
    except Exception as err:
        raise DynamoDBError(
            message=f"get_latest failed: {err}",
            function="get_latest",
            table=AI_REPORTS_TABLE,
        ) from err

    items = response.get("Items") or []
    return items[0] if items else None


def update_metadata(
    league_id: str,
    report_type: str,
    period: str,
    partial: dict[str, Any],
) -> dict[str, Any]:
    """Merge `partial` into the existing item's `metadata` map without
    overwriting unrelated fields. Used by F1's post-broadcast hook to
    stamp `metadata.broadcast_at` once the league fan-out completes.

    Raises:
        ValidationError: invalid report_type / empty inputs.
        DynamoDBError: on Dynamo failure.

    Returns:
        The updated item.
    """
    _validate_report_type(report_type)
    if not league_id:
        raise ValidationError(
            message="update_metadata requires a non-empty league_id",
            handler="ai_reports_store",
            function="update_metadata",
            field="league_id",
        )
    if not period:
        raise ValidationError(
            message="update_metadata requires a non-empty period",
            handler="ai_reports_store",
            function="update_metadata",
            field="period",
        )
    if not isinstance(partial, dict) or not partial:
        raise ValidationError(
            message="update_metadata requires a non-empty partial dict",
            handler="ai_reports_store",
            function="update_metadata",
            field="partial",
        )

    # Build a SET ... statement that touches only the keys passed in.
    # Each key becomes `metadata.#k_i = :v_i` with ExpressionAttributeNames
    # so reserved words / dotted keys don't clash with DynamoDB syntax.
    sets: list[str] = []
    names: dict[str, str] = {"#meta": "metadata"}
    values: dict[str, Any] = {}
    for idx, (key, value) in enumerate(partial.items()):
        name_placeholder = f"#k{idx}"
        value_placeholder = f":v{idx}"
        sets.append(f"#meta.{name_placeholder} = {value_placeholder}")
        names[name_placeholder] = key
        values[value_placeholder] = value

    update_expression = "SET " + ", ".join(sets)

    try:
        response = _table().update_item(
            Key={"pk": _pk(league_id), "sk": _sk(report_type, period)},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )
    except Exception as err:
        raise DynamoDBError(
            message=f"update_metadata failed: {err}",
            function="update_metadata",
            table=AI_REPORTS_TABLE,
        ) from err

    log.info(
        f"ai_reports_store.update_metadata: league={league_id} "
        f"type={report_type} period={period} keys={list(partial.keys())}"
    )
    return response.get("Attributes", {})


def stamp_broadcast_at(
    league_id: str,
    report_type: str,
    period: str,
) -> dict[str, Any]:
    """Stamp `metadata.broadcast_at = <utc iso8601>` on the report row.

    Canonical single-write helper for the broadcast-success path —
    called by all three orchestrators (postdraft, preseason, weekly)
    after `send_emails_concurrently` returns. Keeps the timestamp
    format identical across report types so the iOS surface can
    parse a single shape.

    Returns:
        The updated item (mirrors `update_metadata`).

    Raises:
        ValidationError: invalid report_type / empty inputs.
        DynamoDBError: on Dynamo failure.
    """
    broadcast_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return update_metadata(
        league_id=league_id,
        report_type=report_type,
        period=period,
        partial={"broadcast_at": broadcast_at},
    )


def list_recent(
    league_id: str,
    limit: int = 20,
    cursor: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Paginated archive listing across all report types, newest first.

    Queries the `created-at-index` GSI so a single Dynamo round-trip
    returns mixed-type rows ordered by `created_at` descending.

    Args:
        league_id: Sleeper league id.
        limit: Max rows to return (default 20, capped to 50).
        cursor: Opaque cursor returned from a previous call.

    Returns:
        Tuple of `(items, next_cursor)`. `next_cursor` is None when
        there are no more pages.
    """
    if not league_id:
        return [], None

    safe_limit = max(1, min(int(limit or 20), 50))

    kwargs: dict[str, Any] = {
        "IndexName": GSI_CREATED_AT,
        "KeyConditionExpression": Key("pk").eq(_pk(league_id)),
        "ScanIndexForward": False,
        "Limit": safe_limit,
    }
    if cursor:
        kwargs["ExclusiveStartKey"] = _decode_cursor(cursor)

    try:
        response = _table().query(**kwargs)
    except Exception as err:
        raise DynamoDBError(
            message=f"list_recent failed: {err}",
            function="list_recent",
            table=AI_REPORTS_TABLE,
        ) from err

    items = response.get("Items") or []
    last_key = response.get("LastEvaluatedKey")
    next_cursor = _encode_cursor(last_key) if last_key else None
    return items, next_cursor
