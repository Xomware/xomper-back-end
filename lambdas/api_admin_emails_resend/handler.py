"""
POST /admin/emails-resend
=========================
Admin-only: load an `email_archive` row by id and re-fire it to a new
recipient via SES. The resend uses the EXACT html + text bodies from
the original send — no re-rendering — so the admin can repeat a
production-ready email to a one-off address (e.g. a manager who lost
their original send, or a QA inbox).

Body:
{
    "sleeper_user_id": "abc",         // admin gate identity
    "email":           "admin@...",   // admin identity fallback
    "id":              "uuid",        // required — email_archive row id
    "to_email":        "..."          // required — new recipient
}

Behavior:
- 200 + send receipt on success.
- 400 on missing fields.
- 403 when caller is not an admin.
- 404 when the source row doesn't exist.
- 500 on SES failure.

The new send is itself archived (ses_helper.send_email writes a row
unconditionally), so resends accumulate in the archive next to the
original.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.audit_log import write_audit
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.ses_helper import send_email
from lambdas.common.supabase_helper import _get  # type: ignore[attr-defined]
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_emails_resend"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin emails-resend request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    archive_id = (body.get("id") or "").strip()
    to_email = (body.get("to_email") or "").strip()
    if not archive_id:
        return success_response(
            {"Success": False, "Message": "id is required"},
            status_code=400,
        )
    if not to_email or "@" not in to_email:
        return success_response(
            {"Success": False, "Message": "to_email must be a valid email"},
            status_code=400,
        )

    rows = _get(
        "email_archive",
        {
            "select": "id,template,subject,html_body,text_body",
            "id": f"eq.{archive_id}",
            "limit": "1",
        },
        "Fetch email archive row for resend",
    ) or []
    if not rows:
        return success_response(
            {"Success": False, "Message": "email_archive row not found"},
            status_code=404,
        )

    src = rows[0]
    template_name = src.get("template") or "resend"
    subject = src.get("subject") or "(no subject)"

    ses_result = send_email(
        to_email=to_email,
        subject=subject,
        html_body=src.get("html_body") or "",
        text_body=src.get("text_body") or "",
        template=f"{template_name}_resend",
    )
    if not ses_result.get("success"):
        return success_response(
            {
                "Success": False,
                "Message": f"SES failure: {ses_result.get('error') or 'unknown'}",
            },
            status_code=500,
        )

    sent_at = datetime.now(timezone.utc).isoformat()
    actor_id = admin_user.get("sleeper_user_id") or admin_user.get("email") or ""
    write_audit(
        actor_user_id=actor_id,
        action="email.resend",
        target_table="email_archive",
        target_id=archive_id,
        before=None,
        after={
            "to_email": to_email,
            "subject": subject,
            "template": template_name,
            "message_id": ses_result.get("message_id"),
        },
    )

    return success_response(
        {
            "Success": True,
            "source_id": archive_id,
            "recipient_email": to_email,
            "subject": subject,
            "template": template_name,
            "message_id": ses_result.get("message_id"),
            "sent_at": sent_at,
        }
    )
