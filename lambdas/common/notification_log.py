"""
Notification activity log
=========================
Persists every push + email send (success or failure) to the
`xomper-notification-log` DynamoDB table. Drives the iOS Admin
portal's activity feed.

Schema:
- PK: day (S, "YYYY-MM-DD") — partitions by day so a single
  user's day is queryable in O(1) and scrolling backward across
  days needs only N additional queries (one per day).
- SK: id (S, "{epoch_ms}#{uuid}") — sortable by time inside the
  partition, unique across concurrent sends.
- GSI: kind-index keyed on `kind` for filtering ("push" / "email")
  with `id` as range so we can scope the feed to a channel.

Logging is best-effort. A DynamoDB outage must NOT prevent a push
or email from going out — failures here are swallowed with a
warning log.
"""
from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import boto3

from lambdas.common.logger import get_logger

log = get_logger(__file__)

LOG_TABLE = os.environ.get("APP_NAME", "xomper") + "-notification-log"
KIND_PUSH = "push"
KIND_EMAIL = "email"
STATUS_SUCCESS = "success"
STATUS_FAILURE = "failure"

_dynamodb = boto3.resource("dynamodb")


def _now_parts() -> tuple[str, int, str]:
    """Return (day_yyyy_mm_dd, epoch_ms, uuid_short)."""
    now = datetime.now(timezone.utc)
    day = now.strftime("%Y-%m-%d")
    epoch_ms = int(now.timestamp() * 1000)
    short = uuid.uuid4().hex[:8]
    return day, epoch_ms, short


def log_push(
    *,
    user_id: str,
    title: str,
    body: str,
    category: Optional[str],
    success: bool,
    handler: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    """Record a single push send attempt. Called from
    `sns_helper.send_push_to_users` after each per-endpoint result.
    """
    day, epoch_ms, short = _now_parts()
    item: dict[str, Any] = {
        "day": day,
        "id": f"{epoch_ms:013d}#{short}",
        "epoch_ms": epoch_ms,
        "kind": KIND_PUSH,
        "channel": KIND_PUSH,
        "status": STATUS_SUCCESS if success else STATUS_FAILURE,
        "user_id": user_id or "",
        "title": title or "",
        "body": body or "",
    }
    if category:
        item["category"] = category
    if handler:
        item["handler"] = handler
    if error:
        item["error"] = error
    _put(item)


def log_email(
    *,
    recipient: str,
    subject: str,
    success: bool,
    handler: Optional[str] = None,
    body_snippet: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    """Record a single email send attempt. Called from
    `ses_helper.send_email` after each result.
    """
    day, epoch_ms, short = _now_parts()
    item: dict[str, Any] = {
        "day": day,
        "id": f"{epoch_ms:013d}#{short}",
        "epoch_ms": epoch_ms,
        "kind": KIND_EMAIL,
        "channel": KIND_EMAIL,
        "status": STATUS_SUCCESS if success else STATUS_FAILURE,
        "recipient": recipient or "",
        "subject": subject or "",
    }
    if body_snippet:
        # Truncate so the log row stays small. 240 chars is enough
        # for the admin feed preview without bloating DynamoDB.
        item["body_snippet"] = body_snippet[:240]
    if handler:
        item["handler"] = handler
    if error:
        item["error"] = error
    _put(item)


def _put(item: dict[str, Any]) -> None:
    """Write the log entry. Swallow failures so logging never
    blocks the actual notification delivery."""
    try:
        _dynamodb.Table(LOG_TABLE).put_item(Item=item)
    except Exception as err:  # pragma: no cover — defensive
        log.warning(f"notification_log write failed: {err}")


def query_recent(
    *,
    days_back: int = 7,
    kind: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Read recent log entries across the last `days_back` days,
    newest first. Filters applied in-process — DynamoDB returns
    each day's partition fully, then we filter + slice. Fine for
    the Xomper admin scale (handful of rows per day).
    """
    table = _dynamodb.Table(LOG_TABLE)
    rows: list[dict[str, Any]] = []
    today = datetime.now(timezone.utc)
    for offset in range(days_back):
        day = (today - _delta_days(offset)).strftime("%Y-%m-%d")
        try:
            resp = table.query(
                KeyConditionExpression="#d = :d",
                ExpressionAttributeNames={"#d": "day"},
                ExpressionAttributeValues={":d": day},
                ScanIndexForward=False,  # newest first within partition
                Limit=200,
            )
            rows.extend(resp.get("Items", []))
        except Exception as err:
            log.warning(f"notification_log query failed for {day}: {err}")
        if len(rows) >= limit * 3:  # plenty of headroom for filtering
            break

    if kind:
        rows = [r for r in rows if r.get("kind") == kind]
    if status:
        rows = [r for r in rows if r.get("status") == status]

    rows.sort(key=lambda r: r.get("epoch_ms", 0), reverse=True)
    return rows[:limit]


def _delta_days(n: int):
    """timedelta(days=n) without importing timedelta directly so the
    helper module's surface stays tight."""
    from datetime import timedelta
    return timedelta(days=n)
