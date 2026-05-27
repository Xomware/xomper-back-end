"""
POST /admin/email-test
======================
Admin-only endpoint that delivers an existing AI Review report to a
single whitelisted user — used by the iOS Test Email screen to
iterate on report copy without polluting the production broadcast
state.

CRITICAL invariant: this handler is **read-only** against the
`xomper-ai-reports` table. It never calls `ai_reports_store.update_metadata`
and never writes a `broadcast_at` flag. The unit test suite locks
this down by patching `update_metadata` to raise on call and
asserting the happy path still completes.

Body:
{
    "sleeper_user_id":       "abc123",            // required — admin gate identity
    "email":                 "user@example.com",  // optional fallback identity
    "recipient_user_id":     "u7",                // required — Sleeper user_id of recipient
    "report_id":             "LEAGUE#X|REPORT#weekly#2026W04",
                                                   // composite "pk|sk" from AIReport
    "report_type":           "weekly",            // alt to report_id: report_type + period
    "report_period":         "2026W04"
}

Either `report_id` (composite "pk|sk") OR (`report_type` + `report_period`)
is required — the handler resolves both shapes against the active
whitelisted league.

Behavior:
- 200 + send receipt on success.
- 400 on bad input (missing recipient / missing report identifier).
- 403 when the caller is not an admin.
- 404 when the recipient is unknown / inactive, or the report doesn't exist.
- 500 on any SES / Dynamo failure that isn't a known XomperError.

Response:
{
    "Success": true,
    "recipient_email":  "user@example.com",
    "recipient_user_id": "u7",
    "message_id":       "ses-mid-...",
    "sent_at":          "2026-05-26T17:42:33Z",
    "template":         "ai_review_test",
    "report_type":      "weekly",
    "report_period":    "2026W04"
}
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from lambdas.common import ai_reports_store
from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.email_templates.ai_review import build_email_payload
from lambdas.common.errors import XomperError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.ses_helper import send_email
from lambdas.common.supabase_helper import (
    get_active_whitelisted_league,
    get_whitelisted_user_by_sleeper_id,
)
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_email_test"
TEMPLATE_NAME = "ai_review_test"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin email-test request")

    body = parse_body(event)

    try:
        require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    recipient_user_id = (body.get("recipient_user_id") or "").strip()
    if not recipient_user_id:
        return success_response(
            {"Success": False, "Message": "Missing 'recipient_user_id' in body"},
            status_code=400,
        )

    # Resolve recipient against whitelisted_users.
    recipient = get_whitelisted_user_by_sleeper_id(recipient_user_id)
    if not recipient:
        # Fall back to the active-users list in case the helper rejects
        # users whose `is_active` got toggled but who were intended for
        # a re-send. Single Sleeper id lookup is the canonical path.
        log.warning(f"email-test: recipient not found for sleeper_user_id={recipient_user_id}")
        return success_response(
            {
                "Success": False,
                "Message": f"Recipient not found: {recipient_user_id}",
            },
            status_code=404,
        )

    recipient_email = recipient.get("email")
    if not recipient_email:
        return success_response(
            {
                "Success": False,
                "Message": "Recipient is missing an email address",
            },
            status_code=400,
        )

    # Resolve the report — accept either composite report_id OR
    # explicit report_type + report_period.
    try:
        league_id, report_type, period = _resolve_report_identifiers(body)
    except ValueError as err:
        return success_response(
            {"Success": False, "Message": str(err)},
            status_code=400,
        )

    # CRITICAL: read-only lookup against ai-reports. We intentionally
    # never import `update_metadata` here — the unit test asserts.
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

    body_markdown = report.get("body_markdown") or ""
    league_name = _resolve_league_name(league_id)
    first_name = _first_name(recipient)
    period_label = _period_label(report_type, period)

    payload = build_email_payload(
        user_email=recipient_email,
        user_first_name=first_name,
        report_type=report_type,
        body_markdown=body_markdown,
        period_label=period_label,
        league_name=league_name,
    )

    # Single-recipient send so we can return the SES MessageId. Tagged
    # with `template="ai_review_test"` so the notification_log row is
    # distinguishable from production broadcasts.
    result = send_email(
        to_email=payload["recipient"],
        subject=payload["subject"],
        html_body=payload["html_body"],
        text_body=payload["text_body"],
        template=TEMPLATE_NAME,
    )

    if not result.get("success"):
        return success_response(
            {
                "Success": False,
                "Message": result.get("error") or "SES delivery failed",
                "recipient_email": recipient_email,
                "template": TEMPLATE_NAME,
            },
            status_code=502,
        )

    sent_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return success_response(
        {
            "Success": True,
            "recipient_email": recipient_email,
            "recipient_user_id": recipient_user_id,
            "message_id": result.get("message_id"),
            "sent_at": sent_at,
            "template": TEMPLATE_NAME,
            "report_type": report_type,
            "report_period": period,
        }
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_report_identifiers(body: dict[str, Any]) -> tuple[str, str, str]:
    """Resolve `(league_id, report_type, period)` from the request body.

    Accepts either:
      - `report_id`: composite "pk|sk" where pk is `LEAGUE#<id>` and
        sk is `REPORT#<type>#<period>`. The iOS `AIReport.id` field
        sends this shape.
      - `report_type` + `report_period`: explicit pair, resolved
        against the active whitelisted league's id.

    Raises `ValueError` with a user-facing message on bad input.
    """
    report_id = (body.get("report_id") or "").strip()
    report_type_in = (body.get("report_type") or "").strip()
    period_in = (body.get("report_period") or "").strip()

    if report_id:
        league_id, report_type, period = _parse_composite_report_id(report_id)
        return league_id, report_type, period

    if report_type_in and period_in:
        active_league = get_active_whitelisted_league()
        if not active_league:
            raise ValueError("No active whitelisted league configured")
        league_id = active_league.get("sleeper_league_id") or active_league.get("league_id")
        if not league_id:
            raise ValueError("Active league row is missing sleeper_league_id")
        return str(league_id), report_type_in, period_in

    raise ValueError(
        "Missing report identifier: provide 'report_id' (pk|sk) "
        "or 'report_type' + 'report_period'"
    )


def _parse_composite_report_id(report_id: str) -> tuple[str, str, str]:
    """Split `LEAGUE#<id>|REPORT#<type>#<period>` into (league_id, type, period)."""
    if "|" not in report_id:
        raise ValueError(
            f"Invalid report_id '{report_id}': expected 'LEAGUE#X|REPORT#type#period'"
        )
    pk_part, sk_part = report_id.split("|", 1)
    if not pk_part.startswith("LEAGUE#"):
        raise ValueError(f"Invalid report_id pk: '{pk_part}' must start with 'LEAGUE#'")
    if not sk_part.startswith("REPORT#"):
        raise ValueError(f"Invalid report_id sk: '{sk_part}' must start with 'REPORT#'")
    league_id = pk_part[len("LEAGUE#"):]
    sk_inner = sk_part[len("REPORT#"):]
    if "#" not in sk_inner:
        raise ValueError(
            f"Invalid report_id sk: '{sk_part}' missing '#' between type + period"
        )
    report_type, period = sk_inner.split("#", 1)
    if not league_id or not report_type or not period:
        raise ValueError(f"Invalid report_id '{report_id}': empty component")
    return league_id, report_type, period


def _resolve_league_name(league_id: str) -> Optional[str]:
    """Look up the league name from the active whitelisted league row,
    or fall back to None. Best-effort — a missing name renders the
    email without the league badge, not as a failure."""
    try:
        active = get_active_whitelisted_league()
    except Exception as err:  # pragma: no cover — defensive
        log.warning(f"email-test: failed to resolve league name: {err}")
        return None
    if not active:
        return None
    active_id = active.get("sleeper_league_id") or active.get("league_id")
    if active_id and str(active_id) == str(league_id):
        return active.get("league_name") or active.get("name")
    return None


def _first_name(recipient: dict[str, Any]) -> str:
    """Best-effort first-name extraction for the greeting line."""
    display = recipient.get("display_name") or recipient.get("sleeper_username") or ""
    if not display:
        return ""
    # Use only the leading whitespace-delimited token so display_names
    # like "First Last" render naturally in the subject.
    return display.split()[0]


def _period_label(report_type: str, period: str) -> str:
    """Human-readable period label for the email subject + section title."""
    if report_type == "postDraft":
        return f"Post-Draft {period}"
    if report_type == "preseason":
        return f"Preseason {period}"
    if report_type == "weekly":
        return f"Week {period}"
    return period
