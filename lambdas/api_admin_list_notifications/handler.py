"""
GET /api/admin/notifications
============================
Returns the most recent push + email send activity from the
xomper-notification-log DynamoDB table. Drives the iOS Admin
portal's activity feed.

Query params:
- days_back (int, default 7) — how far back to scan
- kind ("push" | "email", optional) — filter to a single channel
- status ("success" | "failure", optional) — filter by outcome
- limit (int, default 100) — max rows to return
- sleeper_user_id (string, required) — caller identity (admin gate)

The lambda authorizer already enforces a valid JWT; this lambda
adds role enforcement via the `is_admin` flag on
whitelisted_users.
"""
from typing import Any

from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response
from lambdas.common.admin_gate import require_admin, NotAdmin
from lambdas.common.notification_log import query_recent

log = get_logger(__file__)
HANDLER = "api_admin_list_notifications"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin list-notifications request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    qs = event.get("queryStringParameters") or {}

    def _int(key: str, default: int) -> int:
        try:
            return int(qs.get(key)) if qs.get(key) is not None else default
        except (TypeError, ValueError):
            return default

    days_back = _int("days_back", 7)
    limit = _int("limit", 100)
    kind = qs.get("kind") or None
    status = qs.get("status") or None

    rows = query_recent(
        days_back=days_back,
        kind=kind,
        status=status,
        limit=limit,
    )

    # Coerce DynamoDB Decimal-typed numbers to plain ints for JSON.
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        cleaned.append({
            **row,
            "epoch_ms": int(row.get("epoch_ms", 0)),
        })

    return success_response({
        "rows": cleaned,
        "count": len(cleaned),
        "filters": {
            "days_back": days_back,
            "kind": kind,
            "status": status,
            "limit": limit,
        },
    })
