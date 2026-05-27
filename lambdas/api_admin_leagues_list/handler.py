"""
GET /admin/leagues-list
=======================
Admin-only endpoint that returns ALL rows from `whitelisted_leagues`
(active + inactive). The iOS LeaguesListView surfaces inactive leagues
so the admin can re-activate them — unlike runtime read paths which
filter to `is_active=true`.

Query params:
- sleeper_user_id (required) — caller identity (admin gate). The base
  authorizer already enforces a valid JWT; this lambda adds `is_admin`
  role enforcement on top.

Response:
{
    "Success": true,
    "count":   N,
    "leagues": [
        {
            "sleeper_league_id": "...",
            "league_name":       "...",
            "is_active":         true,
            "is_dynasty":        true,
            "has_taxi":          false,
            "created_at":        "2026-..."
        },
        ...
    ]
}
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import list_rows
from lambdas.common.utility_helpers import success_response

log = get_logger(__file__)
HANDLER = "api_admin_leagues_list"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin leagues-list request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    # No cursor pagination — the league count is tiny (1 in v1, max
    # realistically ~20). Limit=200 covers any plausible growth; if
    # we cross 50 we revisit per Open Q3 in the plan.
    rows, _ = list_rows(
        "whitelisted_leagues",
        filters=None,
        limit=200,
        cursor=None,
        order_by="created_at.desc",
    )

    return success_response(
        {
            "Success": True,
            "count": len(rows or []),
            "leagues": rows or [],
        }
    )
