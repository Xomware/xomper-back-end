"""
Notification — Week Preview (scheduled)
========================================
EventBridge cron entry point for the Wednesday-morning Week Preview
newsletter. Fires Wed 9am ET during the regular + post season; runs
through the shared `week_preview_orchestrator.run_week_preview(...)`
pipeline.

Triggered by EventBridge scheduled event (no API Gateway). The
orchestrator owns the pre-flight gate (no-op outside regular/post
season), idempotency, data fetch, Claude generation, persistence, and
email fan-out.

Behavior contract:
- Real cron fires set no body — defaults flow (current week,
  broadcast, not forced).
- Off-season fires return `skipped_offseason` (NOT an exception —
  EventBridge would retry on raise).
- Claude / Sleeper / Dynamo failures are caught here and logged
  WITHOUT re-raising, because EventBridge retry on scheduled events
  is rarely what we want.
- For testing / backfill, the event may carry `{"week": N,
  "dry_run": true, "force": true}` — same shape the admin trigger
  accepts.

Cron-settings gate (admin-cron-settings):
- `enabled=false` → no-op skip.
- `test_mode=true` → forward as `dry_run=True` to the orchestrator
  (admin-only delivery).
"""
from __future__ import annotations

from typing import Any

from lambdas.common.cron_settings import get_cron_setting
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import success_response
from lambdas.common.week_preview_orchestrator import run_week_preview

log = get_logger(__file__)
HANDLER = "notif_week_preview"
LAMBDA_CRON_KEY = "notif_week_preview"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Starting Week Preview notification...")

    cron_setting = get_cron_setting(LAMBDA_CRON_KEY)
    if not cron_setting["enabled"]:
        log.info(
            f"{LAMBDA_CRON_KEY} disabled via admin_cron_settings — skipping"
        )
        return success_response(
            {"Success": True, "skipped": True, "reason": "disabled"},
            is_api=False,
        )

    cron_test_mode = bool(cron_setting["test_mode"])
    event = event or {}

    # Caller-provided overrides (admin trigger / backfill) take precedence,
    # but test_mode acts as a floor — if the admin flipped test_mode on,
    # never broadcast to the full league from a cron fire.
    week_override = event.get("week")
    dry_run = bool(event.get("dry_run", False)) or cron_test_mode
    force = bool(event.get("force", False))
    try:
        seasons_back = int(event.get("seasons_back", 0) or 0)
    except (TypeError, ValueError):
        seasons_back = 0

    try:
        result = run_week_preview(
            week=week_override,
            dry_run=dry_run,
            force=force,
            seasons_back=seasons_back,
        )
    except Exception as e:
        # Swallow + log so EventBridge doesn't retry. Recovery is via
        # the admin trigger.
        log.exception(f"week_preview orchestrator error: {e}")
        return success_response(
            {
                "Success": False,
                "Message": str(e),
                "handler": HANDLER,
            },
            is_api=False,
        )

    return success_response(result, is_api=False)
