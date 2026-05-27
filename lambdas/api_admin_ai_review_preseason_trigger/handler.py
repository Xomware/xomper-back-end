"""
POST /admin/ai-review-preseason-trigger
=======================================
Admin-only endpoint that fires the F2 preseason AI Review pipeline.

Mirrors the F1 post-draft trigger (see
`api_admin_ai_review_postdraft_trigger.handler`). Same admin gate,
same body shape, same response shape. The only differences are the
pre-flight check (NFL state must be in `pre`/`off` season — i.e. the
regular season hasn't started yet) and the period string
(`"2026-PRESEASON"` vs F1's `"2026"`).

Body:
{
    "sleeper_user_id": "abc123",     // required — admin gate identity
    "email": "user@example.com",     // optional fallback identity
    "dry_run": true,                 // optional, default true
    "force": false                   // optional, default false
}

Behavior:
- 200 + body on success (delivery counts + token usage + report row).
- 400 on bad input.
- 403 when the caller is not an admin.
- 409 when a report already exists and `force=false`.
- 412 when the regular season has already started (preseason window
  has closed).
- 502 when Anthropic / Sleeper external calls fail terminally.
- 500 on everything else.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.errors import (
    PreseasonWindowPassedError,
    ReportAlreadyExistsError,
    XomperError,
    handle_errors,
)
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import parse_body, success_response

from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

log = get_logger(__file__)
HANDLER = "api_admin_ai_review_preseason_trigger"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin AI Review preseason trigger request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    dry_run = _coerce_bool(body.get("dry_run"), default=True)
    force = _coerce_bool(body.get("force"), default=False)
    created_by = admin_user.get("sleeper_user_id") or admin_user.get("id")

    try:
        result = run(
            dry_run=dry_run,
            force=force,
            created_by_user_id=created_by,
        )
    except ReportAlreadyExistsError as err:
        err.log_error()
        return success_response(
            {
                "Success": False,
                "error": "already_generated",
                "Message": err.message,
                "existing": err.details.get("existing"),
            },
            status_code=409,
        )
    except PreseasonWindowPassedError as err:
        err.log_error()
        return success_response(
            {
                "Success": False,
                "error": "preseason_window_passed",
                "Message": err.message,
                "nfl_season": err.details.get("nfl_season"),
                "season_type": err.details.get("season_type"),
            },
            status_code=412,
        )
    except XomperError as err:
        # ClaudeAPIError / SleeperAPIError / DynamoDBError /
        # ValidationError / NotFoundError all surface through here.
        err.log_error()
        return err.to_response()

    return success_response(
        {
            "Success": True,
            "status": result["status"],
            "report": result.get("report"),
            "dry_run": result["dry_run"],
            "delivery_count": result["delivery_count"],
            "model": result["model"],
            "token_usage": result["token_usage"],
            # Admin Portal F2 — present on dry-run, None on broadcast.
            "previews": result.get("previews"),
        }
    )


def _coerce_bool(value: Any, *, default: bool) -> bool:
    """Map the API GW body's `bool | "true" | "false" | 1 | 0` shapes
    to a Python bool. Anything else falls back to `default`."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        if value.strip().lower() in {"true", "1", "yes"}:
            return True
        if value.strip().lower() in {"false", "0", "no"}:
            return False
    return default
