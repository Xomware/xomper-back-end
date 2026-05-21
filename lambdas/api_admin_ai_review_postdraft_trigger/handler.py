"""
POST /admin/ai-review-postdraft-trigger
=======================================
Admin-only endpoint that fires the F1 post-draft AI Review pipeline.

NOTE on the path: the infra module flattens the route to a single
`path_part` (max 2 segments under `/admin`), so what the plan calls
`/admin/ai-review/post-draft/trigger` is actually deployed at
`/admin/ai-review-postdraft-trigger`. Same admin gate, same payload.

Body:
{
    "sleeper_user_id": "abc123",     // required — admin gate identity
    "email": "user@example.com",     // optional fallback identity
    "dry_run": true,                 // optional, default true
    "force": false,                  // optional, default false
    "target_year": 2024              // optional — backfill mode: walk
                                     //   previous_league_id from active
                                     //   league until season == str(year),
                                     //   then generate against THAT draft.
                                     //   Default None = current draft.
}

Behavior:
- 200 + body on success (delivery counts + token usage + report row).
- 400 on bad input.
- 403 when the caller is not an admin.
- 404 when target_year is set but no historical league with that
  season is reachable via previous_league_id.
- 409 when a report already exists and `force=false`.
- 412 when the Sleeper draft is not yet complete.
- 502 when Anthropic / Sleeper external calls fail terminally.
- 500 on everything else.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.errors import (
    DraftNotCompleteError,
    ReportAlreadyExistsError,
    XomperError,
    handle_errors,
)
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import parse_body, success_response

from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

log = get_logger(__file__)
HANDLER = "api_admin_ai_review_postdraft_trigger"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin AI Review post-draft trigger request")

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
    target_year = _coerce_int(body.get("target_year"))
    created_by = admin_user.get("sleeper_user_id") or admin_user.get("id")

    try:
        result = run(
            dry_run=dry_run,
            force=force,
            created_by_user_id=created_by,
            target_year=target_year,
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
    except DraftNotCompleteError as err:
        err.log_error()
        return success_response(
            {
                "Success": False,
                "error": "draft_not_complete",
                "Message": err.message,
                "draft_status": err.details.get("draft_status"),
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
            "period": result.get("period"),
            "target_year": result.get("target_year"),
        }
    )


def _coerce_int(value: Any) -> int | None:
    """Cast API GW body integers (which sometimes arrive as strings)
    to int. Returns None on failure or when omitted."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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
