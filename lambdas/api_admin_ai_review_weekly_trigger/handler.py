"""
POST /admin/ai-review-weekly-trigger
====================================
Admin-only endpoint that fires the F3 weekly AI Review pipeline
manually. Mirrors the F1 post-draft + F2 preseason triggers (same
admin gate, same body shape, same response envelope) — the only
delta is the optional `week` override and the optional
`use_previous_season` flag for dry-run calibration against last
year's matchup data.

Body:
{
    "sleeper_user_id": "abc123",     // required — admin gate identity
    "email": "user@example.com",     // optional fallback identity
    "week": 4,                        // optional — defaults to current
    "dry_run": true,                 // optional, default true
    "force": false,                  // optional, default false
    "seasons_back": 0,               // optional, default 0 — walk
                                     //   previous_league_id N times
                                     //   from the active league
                                     //   before fetching matchups.
                                     //   1 = prior season, 2 = two
                                     //   seasons ago, etc.
    "use_previous_season": false     // DEPRECATED — alias for
                                     //   seasons_back=1. Logs a
                                     //   warning when used.
}

Behavior:
- 200 + body on success (delivery counts + token usage + report row).
- 400 on bad input.
- 403 when the caller is not an admin.
- 409 when a report already exists for the resolved period and
  `force=false`.
- 502 when Anthropic / Sleeper external calls fail terminally.
- 500 on everything else.

The orchestrator lives in `lambdas.common.weekly_orchestrator` so
both this lambda and the EventBridge cron lambda
(`notif_ai_review_weekly`) share the exact same pipeline. The
deploy script zips each lambda dir in isolation; cross-package
imports would not survive the layer-only common deploy.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.errors import (
    ReportAlreadyExistsError,
    XomperError,
    handle_errors,
)
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import parse_body, success_response
from lambdas.common.weekly_orchestrator import run_weekly

log = get_logger(__file__)
HANDLER = "api_admin_ai_review_weekly_trigger"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin AI Review weekly trigger request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    week = _coerce_int(body.get("week"))
    dry_run = _coerce_bool(body.get("dry_run"), default=True)
    force = _coerce_bool(body.get("force"), default=False)
    seasons_back_raw = _coerce_int(body.get("seasons_back"))
    seasons_back = seasons_back_raw if seasons_back_raw is not None else 0
    # Back-compat alias — forwarded to the orchestrator which logs
    # a deprecation warning when truthy AND seasons_back==0.
    use_previous_season = _coerce_bool(
        body.get("use_previous_season"), default=False
    )
    created_by = admin_user.get("sleeper_user_id") or admin_user.get("id")

    try:
        result = run_weekly(
            week=week,
            dry_run=dry_run,
            force=force,
            seasons_back=seasons_back,
            use_previous_season=use_previous_season,
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
    except XomperError as err:
        # ClaudeAPIError / SleeperAPIError / DynamoDBError /
        # ValidationError / NotFoundError / MemoryStoreError all
        # surface through here.
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
            "week": result.get("week"),
            "period": result.get("period"),
            "memory_count_in": result.get("memory_count_in"),
            "memory_count_out": result.get("memory_count_out"),
            "envelope_parsed": result.get("envelope_parsed"),
            "seasons_back": result.get("seasons_back"),
            "use_previous_season": result.get("use_previous_season"),
            # Admin Portal F2 — present on dry-run, None on broadcast.
            "previews": result.get("previews"),
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
