"""
POST /admin/announcements-delete
================================
Admin-only endpoint that SOFT-DELETES a row in `league_announcements`
by flipping `is_active=false`. Writes one row into `admin_audit`.

Soft delete only — hard delete is a Supabase dashboard chore so the
audit trail stays intact. The schema doesn't even expose a hard-delete
path through the API.

Body:
{
    "sleeper_user_id": "abc123",        // required — admin gate identity
    "email":           "user@..",       // optional fallback identity
    "id":              "<uuid>"         // required — row to soft-delete
}

Behavior:
- 200 + Success=true on a successful soft delete. Response carries the
  updated row + audit_id.
- 400 on missing `id`.
- 403 when the caller is not an admin.
- 404 when the row isn't in `league_announcements`.
- 500 on Supabase / unexpected failure.

Idempotency note:
    Soft-deleting an already-inactive row succeeds (the patch is a
    no-op write). The audit row still fires so the trail records the
    intent. This matches how `is_active=false` toggles work elsewhere
    in admin-portal F4.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.announcements_store import delete as delete_announcement
from lambdas.common.audit_log import write_audit
from lambdas.common.errors import NotFoundError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import get_row
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_announcements_delete"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin announcements-delete request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    announcement_id = (body.get("id") or "").strip()
    if not announcement_id:
        return success_response(
            {"Success": False, "Message": "Missing 'id' in body"},
            status_code=400,
        )

    # Snapshot before so the audit row records the prior is_active flag.
    existing = get_row("league_announcements", "id", announcement_id)
    if not existing:
        return success_response(
            {
                "Success": False,
                "Message": f"Announcement not found: {announcement_id}",
            },
            status_code=404,
        )

    before = {"is_active": existing.get("is_active")}
    after = {"is_active": False}

    try:
        updated = delete_announcement(announcement_id)
    except NotFoundError as err:
        # Race: row vanished between the snapshot fetch and the soft
        # delete. Surface as 404.
        return success_response(
            {"Success": False, "Message": str(err.message)},
            status_code=404,
        )

    # Best-effort audit write — never fails the parent action.
    actor = admin_user.get("sleeper_user_id") or admin_user.get("id") or "unknown"
    audit_row = write_audit(
        actor_user_id=str(actor),
        action="announcements.delete",
        target_table="league_announcements",
        target_id=announcement_id,
        before=before,
        after=after,
    )
    audit_id = (audit_row or {}).get("id") if audit_row else None

    return success_response(
        {
            "Success": True,
            "id": announcement_id,
            "row": updated or existing,
            "before": before,
            "after": after,
            "audit_id": audit_id,
        }
    )
