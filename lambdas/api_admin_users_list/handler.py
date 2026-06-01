"""
GET /admin/users-list
=====================
Admin-only endpoint that returns ALL rows from `whitelisted_users`
(active + inactive). The iOS UsersListView surfaces inactive users
so the admin can re-activate them — unlike runtime read paths which
filter to `is_active=true`.

Auth:
- JWT-gated by the existing API Gateway authorizer.
- `require_admin` adds the `is_admin` role check on top.

Response:
{
    "Success": true,
    "count":   N,
    "users":   [
        {
            "id":               "...",
            "email":            "...",
            "sleeper_username": "...",
            "sleeper_user_id":  "...",
            "display_name":     "...",
            "role":             "...",
            "is_active":        true,
            "is_admin":         false,
            "created_at":       "2026-..."
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
HANDLER = "api_admin_users_list"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin users-list request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    rows, _ = list_rows(
        "whitelisted_users",
        filters=None,
        limit=200,
        cursor=None,
        order_by="created_at.desc",
    )

    return success_response(
        {
            "Success": True,
            "count": len(rows or []),
            "users": rows or [],
        }
    )
