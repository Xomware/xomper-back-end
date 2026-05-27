"""
GET /admin/email-test-recipients
================================
Admin-only endpoint that returns the list of currently active
whitelisted users — drives the recipient picker on the iOS Test
Email screen (admin-portal F1).

Read-only against Supabase. No mutation, no Dynamo writes.

Query params:
- sleeper_user_id (required) — caller identity (admin gate). The
  base authorizer already enforces a valid JWT; this lambda adds
  `is_admin` role enforcement on top.

Response:
{
    "Success":  true,
    "count":    N,
    "recipients": [
        {
            "user_id":      "u7",
            "display_name": "ManagerSeven",
            "email":        "seven@example.com",
            "is_admin":     false
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
from lambdas.common.supabase_helper import get_active_whitelisted_users
from lambdas.common.utility_helpers import success_response

log = get_logger(__file__)
HANDLER = "api_admin_email_test_recipients"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin email-test recipients request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    rows = get_active_whitelisted_users()

    recipients: list[dict[str, Any]] = []
    for row in rows or []:
        sleeper_id = row.get("sleeper_user_id")
        email = row.get("email")
        # Skip rows missing the data the iOS picker needs to render +
        # POST against /admin/email-test. Leaves a clean payload.
        if not sleeper_id or not email:
            continue
        recipients.append(
            {
                "user_id": str(sleeper_id),
                "display_name": row.get("display_name") or row.get("sleeper_username") or "",
                "email": email,
                "is_admin": bool(row.get("is_admin")),
            }
        )

    return success_response(
        {
            "Success": True,
            "count": len(recipients),
            "recipients": recipients,
        }
    )
