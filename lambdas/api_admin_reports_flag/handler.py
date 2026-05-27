"""
POST /admin/reports-flag
========================
Admin-only endpoint that toggles one of the broadcast-safety flags
on an AI Review report row.

NOTE on the path: the infra module flattens nested REST routes to a
single `path_part` segment under `/admin`, so what the plan refers
to as `/admin/reports/{league_id}/{report_type}/{period}/flag` is
deployed at `/admin/reports-flag` (infra PR #109). Same admin gate,
identifiers live in the body.

Body:
{
    "sleeper_user_id": "abc123",          // required — admin gate identity
    "email":           "user@example.com", // optional fallback identity
    "league_id":       "LEAGUE_ID",        // required — Sleeper league id
    "report_type":     "weekly",           // required — postDraft | preseason | weekly | mock
    "period":          "2026W04",          // required — period string
    "flag":            "is_redacted",      // required — "is_redacted" | "do_not_broadcast"
    "value":           true                // required — bool
}

Behavior:
- 200 + Success=true on a happy-path toggle. Response carries the
  FULL updated metadata dict so iOS can refresh local state in place.
- 400 on bad input (missing field, invalid flag name, non-bool value).
- 403 when the caller is not an admin.
- 404 when the targeted report doesn't exist.
- 500 on Dynamo / unexpected failures.

Response:
{
    "Success":     true,
    "league_id":   "LEAGUE_ID",
    "report_type": "weekly",
    "period":      "2026W04",
    "flag":        "is_redacted",
    "value":       true,
    "metadata":    { ... full updated metadata map ... }
}

Storage: writes through `ai_reports_store.update_metadata` with
`{flag: "true" | "false"}` — booleans persist as strings because the
flat-map accessor on iOS (`AIReport.metadata: [String: String]`)
collapses Dynamo BOOL to "true"/"false" already.
"""
from __future__ import annotations

from typing import Any

from lambdas.common import ai_reports_store
from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.audit_log import write_audit
from lambdas.common.errors import XomperError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_reports_flag"

ALLOWED_FLAGS: tuple[str, ...] = ("is_redacted", "do_not_broadcast")


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin reports-flag request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    league_id = (body.get("league_id") or "").strip()
    report_type = (body.get("report_type") or "").strip()
    period = (body.get("period") or "").strip()
    flag = (body.get("flag") or "").strip()
    raw_value = body.get("value")

    # --- input validation ---------------------------------------------------
    if not league_id:
        return success_response(
            {"Success": False, "Message": "Missing 'league_id' in body"},
            status_code=400,
        )
    if not report_type:
        return success_response(
            {"Success": False, "Message": "Missing 'report_type' in body"},
            status_code=400,
        )
    if not period:
        return success_response(
            {"Success": False, "Message": "Missing 'period' in body"},
            status_code=400,
        )
    if not flag:
        return success_response(
            {"Success": False, "Message": "Missing 'flag' in body"},
            status_code=400,
        )
    if flag not in ALLOWED_FLAGS:
        return success_response(
            {
                "Success": False,
                "Message": (
                    f"Invalid flag '{flag}'. "
                    f"Must be one of {list(ALLOWED_FLAGS)}."
                ),
            },
            status_code=400,
        )
    if not isinstance(raw_value, bool):
        return success_response(
            {
                "Success": False,
                "Message": "'value' must be a boolean (true/false)",
            },
            status_code=400,
        )

    # --- existence check ----------------------------------------------------
    # 404 immediately rather than blind-writing a metadata patch on a
    # nonexistent row (UpdateItem would otherwise create a partial row
    # with only `metadata.<flag>` set, leaving an inconsistent record).
    try:
        report = ai_reports_store.get_report(
            league_id=league_id,
            report_type=report_type,
            period=period,
        )
    except XomperError as err:
        err.log_error()
        return err.to_response()

    if not report:
        return success_response(
            {
                "Success": False,
                "Message": (
                    f"Report not found for league={league_id} "
                    f"type={report_type} period={period}"
                ),
            },
            status_code=404,
        )

    # Capture the previous flag value BEFORE mutating so the audit
    # `before` blob records the actual transition (not just a snapshot
    # of the post-write state). Existing metadata persists flags as
    # "true"/"false" strings to match the iOS flat-map accessor.
    existing_metadata = report.get("metadata") if isinstance(report, dict) else None
    if isinstance(existing_metadata, dict):
        old_value_str = existing_metadata.get(flag)
    else:
        old_value_str = None

    # --- mutate -------------------------------------------------------------
    value_str = "true" if raw_value else "false"
    try:
        updated_item = ai_reports_store.update_metadata(
            league_id=league_id,
            report_type=report_type,
            period=period,
            partial={flag: value_str},
        )
    except XomperError as err:
        err.log_error()
        return err.to_response()

    metadata = updated_item.get("metadata") if isinstance(updated_item, dict) else None

    # F4 audit retrofit. Best-effort — audit failures NEVER fail the
    # parent action (the flag is already toggled in Dynamo). The
    # `before` / `after` JSONB blobs capture the actual transition on
    # the flag key only — keeping the audit row compact and easy to
    # diff in the iOS audit feed.
    actor = admin_user.get("sleeper_user_id") or admin_user.get("id") or "unknown"
    write_audit(
        actor_user_id=str(actor),
        action="reports.flag",
        target_table="xomper-ai-reports",
        target_id=f"{league_id}|{report_type}|{period}",
        before={flag: old_value_str},
        after={flag: value_str},
    )
    log.info(
        f"admin_reports_flag: actor={actor} league={league_id} "
        f"type={report_type} period={period} flag={flag} value={raw_value}"
    )

    return success_response(
        {
            "Success": True,
            "league_id": league_id,
            "report_type": report_type,
            "period": period,
            "flag": flag,
            "value": raw_value,
            "metadata": metadata or {},
        }
    )
