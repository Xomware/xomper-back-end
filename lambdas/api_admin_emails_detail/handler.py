"""
GET /admin/emails-detail?id=<uuid>
==================================
Admin-only fetch of a single email_archive row INCLUDING the HTML +
text bodies. Separate from `/admin/emails-list` so the list payload
stays lean (HTML bodies are 10-50 KB each).

Response:
{
    "Success": true,
    "row": {
        "id":              "uuid",
        "sent_at":         "2026-06-03T15:42:11Z",
        "template":        "weekly_recap",
        "subject":         "...",
        "recipient_email": "...",
        "html_body":       "<html>...</html>",
        "text_body":       "Plain text...",
        "message_id":      "ses-mid-...",
        "metadata":        {}
    }
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
HANDLER = "api_admin_emails_detail"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin emails-detail request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    qs = event.get("queryStringParameters") or {}
    archive_id = (qs.get("id") or "").strip()
    if not archive_id:
        return success_response(
            {"Success": False, "Message": "id is required"},
            status_code=400,
        )

    rows = _get(
        "email_archive",
        {
            "select": "id,sent_at,template,subject,recipient_email,html_body,text_body,message_id,metadata",
            "id": f"eq.{archive_id}",
            "limit": "1",
        },
        "Fetch email archive row",
    ) or []

    if not rows:
        return success_response(
            {"Success": False, "Message": "email_archive row not found"},
            status_code=404,
        )

    return success_response({"Success": True, "row": rows[0]})
