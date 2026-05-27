"""
GET /api/ai-reports/latest
==========================
Returns the most recent AI Review report for the active whitelisted
league, filtered by `type`.

Query params:
- `type` (required) — one of `postDraft` | `preseason` | `weekly`.

Auth:
- The base JWT authorizer (`lambdas/authorizer/`) gates this route
  at API Gateway. Any signed-in league member can read — no admin
  enforcement (these are league-member reads, not admin actions).

Response shape:
- 200 with `{"report": {...} | null}`.
- 400 on an unknown / missing `type`.
- 404 when the league has no reports of the requested type yet
  (we return 200 + `null` to keep the iOS empty-state path simple;
  the 404 path is reserved for the no-active-league case).
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import is_admin
from lambdas.common.ai_reports_store import REPORT_TYPES, get_latest
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import get_active_whitelisted_league
from lambdas.common.utility_helpers import success_response

log = get_logger(__file__)
HANDLER = "api_ai_reports_latest"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("AI reports latest request")

    qs = event.get("queryStringParameters") or {}
    report_type = (qs.get("type") or "").strip()

    if not report_type:
        return success_response(
            {"Success": False, "Message": "Missing required query param 'type'"},
            status_code=400,
        )

    if report_type not in REPORT_TYPES:
        return success_response(
            {
                "Success": False,
                "Message": (
                    f"Invalid type '{report_type}'. Must be one of {list(REPORT_TYPES)}."
                ),
            },
            status_code=400,
        )

    league = get_active_whitelisted_league()
    if not league:
        return success_response(
            {"Success": False, "Message": "No active whitelisted league configured"},
            status_code=404,
        )

    league_id = league.get("sleeper_league_id") or league.get("league_id")
    if not league_id:
        return success_response(
            {"Success": False, "Message": "Active league row is missing sleeper_league_id"},
            status_code=500,
        )

    report = get_latest(league_id, report_type)

    # Admin-portal F3: hide redacted reports from non-admin callers.
    # Admins still see the row (the iOS archive surfaces a "Show
    # redacted" toggle so they can un-redact). Treat the redacted-
    # for-non-admin case as "no report" so the iOS empty-state path
    # is identical to the no-row case — no info leak about the row's
    # existence.
    if report and not is_admin(event):
        metadata = report.get("metadata") or {}
        if str(metadata.get("is_redacted", "")).lower() == "true":
            report = None

    return success_response({"report": report})
