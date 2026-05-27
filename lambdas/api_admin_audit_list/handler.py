"""
GET /admin/audit-list
=====================
Admin-only endpoint that returns the paginated audit feed from the
`admin_audit` Supabase table (admin-portal F4).

Query params:
- sleeper_user_id  (required) — caller identity (admin gate).
- limit            (optional) — default 50, clamped to [1, 200].
- cursor           (optional) — `created_at` timestamp of the last row
                                 of the previous page; next page is
                                 strictly older than this.
- action           (optional) — filter to a single action name (e.g.
                                 "users.update").
- actor_user_id    (optional) — filter to a single actor.

Response:
{
    "Success":     true,
    "count":       N,
    "rows":        [ ... admin_audit rows ... ],
    "next_cursor": "2026-05-27T...",  // or null when at the tail
    "table_missing": false             // true ONLY when the Supabase
                                       //   migration hasn't been applied
                                       //   (graceful empty-state fallback)
}

GRACEFUL FALLBACK — table_missing:
    If the `admin_audit` Supabase table doesn't exist yet (the manual
    SQL migration hasn't been applied), this endpoint returns
    `{ Success: true, count: 0, rows: [], next_cursor: null,
       table_missing: true }` with HTTP 200. iOS renders an empty
    state rather than an error. The lambda logs the missing-table
    case at ERROR so CloudWatch surfaces the misconfiguration.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import list_rows
from lambdas.common.utility_helpers import get_query_params, success_response

log = get_logger(__file__)
HANDLER = "api_admin_audit_list"

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin audit-list request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    qs = get_query_params(event)

    limit = _parse_limit(qs.get("limit"))
    cursor = (qs.get("cursor") or "").strip() or None
    action = (qs.get("action") or "").strip() or None
    actor = (qs.get("actor_user_id") or "").strip() or None

    filters: dict[str, str] = {}
    if action:
        filters["action"] = action
    if actor:
        filters["actor_user_id"] = actor

    # Graceful table-missing fallback: if the Supabase migration hasn't
    # been applied yet, return an empty page with `table_missing: true`
    # so the iOS audit feed renders an empty state rather than erroring.
    try:
        rows, next_cursor = list_rows(
            "admin_audit",
            filters=filters or None,
            limit=limit,
            cursor=cursor,
            order_by="created_at.desc",
        )
    except Exception as err:  # noqa: BLE001 — graceful fallback
        log.error(
            f"audit-list: admin_audit fetch failed — returning empty "
            f"page with table_missing=true. error={err}"
        )
        return success_response(
            {
                "Success": True,
                "count": 0,
                "rows": [],
                "next_cursor": None,
                "table_missing": True,
            }
        )

    return success_response(
        {
            "Success": True,
            "count": len(rows or []),
            "rows": rows or [],
            "next_cursor": next_cursor,
            "table_missing": False,
        }
    )


def _parse_limit(raw: Any) -> int:
    """Parse the `limit` query param. Defaults to 50; clamps to
    [1, MAX_LIMIT]. Invalid values fall back to the default."""
    if raw is None or raw == "":
        return DEFAULT_LIMIT
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if value < 1:
        return 1
    if value > MAX_LIMIT:
        return MAX_LIMIT
    return value
