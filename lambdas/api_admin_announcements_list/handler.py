"""
GET /admin/announcements-list
=============================
Admin-only endpoint that returns EVERY row from `league_announcements`
including inactive (soft-deleted) + expired entries so the admin UI
can manage them. Powers the iOS AnnouncementsListView.

Response:
{
    "Success":       true,
    "count":         N,
    "rows":          [ ... league_announcements rows ... ],
    "table_missing": false   // true ONLY when the Supabase migration
                             //   hasn't been applied yet (graceful
                             //   empty-state fallback for iOS)
}

GRACEFUL FALLBACK — table_missing:
    Mirrors api_admin_audit_list + api_admin_cron_settings_list. If the
    `league_announcements` Supabase table doesn't exist yet (the manual
    SQL migration hasn't been applied), `list_all` degrades to []. The
    handler turns the empty result into `table_missing: true` for iOS
    so it renders an empty-state instead of a generic error.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.announcements_store import list_all
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import success_response

log = get_logger(__file__)
HANDLER = "api_admin_announcements_list"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin announcements-list request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    rows = list_all()

    # An empty list could mean "table missing" or "all rows hard-deleted"
    # — but the migration seeds 3 rows and the API only soft-deletes, so
    # in practice empty == table_missing. Surface the flag for iOS.
    table_missing = len(rows) == 0

    return success_response(
        {
            "Success": True,
            "count": len(rows),
            "rows": rows,
            "table_missing": table_missing,
        }
    )
