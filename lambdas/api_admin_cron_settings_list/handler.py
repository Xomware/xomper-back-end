"""
GET /admin/cron-settings-list
=============================
Admin-only endpoint that returns every row from `admin_cron_settings`
(admin-cron-settings F2). Powers the iOS CronSettingsView list.

Query params:
- sleeper_user_id  (required) — caller identity (admin gate).

Response:
{
    "Success":       true,
    "count":         N,
    "rows":          [ ... admin_cron_settings rows ... ],
    "table_missing": false  // true ONLY when the Supabase migration
                            //   hasn't been applied yet (graceful
                            //   empty-state fallback for iOS)
}

GRACEFUL FALLBACK — table_missing:
    Mirrors api_admin_audit_list (F4). If the `admin_cron_settings`
    Supabase table doesn't exist yet (the manual SQL migration hasn't
    been applied), this endpoint returns `count=0, rows=[],
    table_missing=true` with HTTP 200. The cron_settings helper logs
    the failure at ERROR so CloudWatch surfaces the misconfiguration.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.cron_settings import list_cron_settings
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import success_response

log = get_logger(__file__)
HANDLER = "api_admin_cron_settings_list"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin cron-settings-list request")

    try:
        require_admin(event)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    rows = list_cron_settings()

    # An empty list could mean "table missing" or "no rows yet"
    # — but the migration seeds 5 rows so in practice an empty result
    # is always the table_missing case. Mark it so iOS renders the
    # right empty state.
    table_missing = len(rows) == 0

    return success_response(
        {
            "Success": True,
            "count": len(rows),
            "rows": rows,
            "table_missing": table_missing,
        }
    )
