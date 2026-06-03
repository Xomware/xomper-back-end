"""
POST /admin/ai-review-week-preview-trigger
==========================================
Admin-only endpoint that fires the Wednesday Week Preview pipeline
manually. Mirrors the F3 weekly trigger (same admin gate, same body
shape, same response envelope).

Body:
{
    "sleeper_user_id": "abc123",     // required — admin gate identity
    "email": "user@example.com",     // optional fallback identity
    "week": 4,                        // optional — defaults to upcoming
    "dry_run": true,                 // optional, default true
    "force": false                   // optional, default false
}

Behavior:
- 200 + body on success.
- 400 on bad input.
- 403 when the caller is not an admin.
- 409 when a preview already exists for the resolved period and
  `force=false`.
- 502 when Anthropic / Sleeper external calls fail terminally.
- 500 on everything else.

The orchestrator lives in `lambdas.common.week_preview_orchestrator`
so both this handler and `notif_week_preview` share one pipeline.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.audit_log import write_audit
from lambdas.common.errors import (
    NotFoundError,
    ReportAlreadyExistsError,
    ValidationError,
    handle_errors,
)
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import parse_body, success_response
from lambdas.common.week_preview_orchestrator import run_week_preview

log = get_logger(__file__)
HANDLER = "api_admin_ai_review_week_preview_trigger"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin week-preview trigger request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    week_override = body.get("week")
    dry_run = bool(body.get("dry_run", True))
    force = bool(body.get("force", False))
    try:
        seasons_back = int(body.get("seasons_back", 0) or 0)
    except (TypeError, ValueError):
        seasons_back = 0
    actor_id = admin_user.get("sleeper_user_id") or admin_user.get("email") or ""

    try:
        result = run_week_preview(
            week=week_override,
            dry_run=dry_run,
            force=force,
            seasons_back=seasons_back,
            created_by_user_id=admin_user.get("sleeper_user_id"),
        )
    except ReportAlreadyExistsError as e:
        return success_response(
            {
                "Success": False,
                "Message": e.message,
                "existing": e.existing,
            },
            status_code=409,
        )
    except ValidationError as e:
        return success_response(
            {"Success": False, "Message": e.message},
            status_code=400,
        )
    except NotFoundError as e:
        return success_response(
            {"Success": False, "Message": e.message},
            status_code=404,
        )

    write_audit(
        actor_user_id=actor_id,
        action="ai_review.week_preview_trigger",
        target_table="xomper-ai-reports",
        target_id=result.get("report_id") or result.get("period"),
        before=None,
        after={
            "period": result.get("period"),
            "dry_run": result.get("dry_run"),
            "email_sent": result.get("email_sent"),
            "envelope_parsed": result.get("envelope_parsed"),
        },
    )

    return success_response({"Success": True, **result})
