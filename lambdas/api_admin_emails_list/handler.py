"""
GET /admin/emails-list
======================
Admin-only paginated list of emails archived via Supabase
`email_archive`. Returns newest-first metadata for the admin Email
Archive screen. The HTML body is omitted from the list payload to
keep it lean — fetch the single-row detail via
`GET /admin/emails-detail?id=<uuid>` to load the rendered content.

Query params:
- `limit` (optional, default 25, max 100)
- `cursor` (optional ISO-8601 `sent_at` of the last row from the
  previous page; rows older than `cursor` are returned next)
- `recipient` (optional email filter)
- `template` (optional template-name filter)

Response:
{
    "Success": true,
    "count": N,
    "next_cursor": "2026-06-03T..." | null,
    "rows": [
        {
            "id":              "uuid",
            "sent_at":         "2026-06-03T15:42:11Z",
            "template":        "weekly_recap",
            "subject":         "Week 12 CLT DYNASTY recap",
            "recipient_email": "...",
            "message_id":      "ses-mid-..."
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
from lambdas.common.supabase_helper import _get  # type: ignore[attr-defined]
from lambdas.common.utility_helpers import success_response

log = get_logger(__file__)
HANDLER = "api_admin_emails_list"

_DEFAULT_LIMIT = 25
_MAX_LIMIT = 100


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin emails-list request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    qs = event.get("queryStringParameters") or {}
    raw_limit = qs.get("limit")
    try:
        limit = max(1, min(int(raw_limit) if raw_limit else _DEFAULT_LIMIT, _MAX_LIMIT))
    except (TypeError, ValueError):
        limit = _DEFAULT_LIMIT

    cursor = (qs.get("cursor") or "").strip()
    recipient = (qs.get("recipient") or "").strip()
    template = (qs.get("template") or "").strip()

    params: dict[str, str] = {
        "select": "id,sent_at,template,subject,recipient_email,message_id",
        "order": "sent_at.desc",
        # +1 so we can detect the next-cursor without a second query.
        "limit": str(limit + 1),
    }
    if cursor:
        params["sent_at"] = f"lt.{cursor}"
    if recipient:
        params["recipient_email"] = f"eq.{recipient}"
    if template:
        params["template"] = f"eq.{template}"

    rows = _get("email_archive", params, "List email archive") or []

    next_cursor: str | None = None
    if len(rows) > limit:
        next_cursor = rows[limit - 1].get("sent_at")
        rows = rows[:limit]

    return success_response(
        {
            "Success": True,
            "count": len(rows),
            "next_cursor": next_cursor,
            "rows": rows,
        }
    )
