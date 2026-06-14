"""
season_guard helper
====================
Shared offseason guard for the game-dependent scheduled notif lambdas.

EventBridge fires the weekly recap, close-game alert, lineup-not-set,
and world-cup movement notifs on fixed cron expressions year-round.
None of them make sense once the NFL regular season ends — recapping a
week that was never played, alerting on lineups for games that don't
exist. Before this guard, the only thing stopping an offseason send was
the admin cron toggle, which means a human had to remember to flip every
notif off in February and back on in September. They didn't, so a
"Week 1 recap" went out in June.

This guard makes offseason suppression automatic, mirroring the
pre-flight already baked into `weekly_orchestrator` /
`week_preview_orchestrator` (the AI-newsletter path). Each handler calls
`offseason_skip(nfl_state, LAMBDA_CRON_KEY, force=...)` immediately after
fetching nfl_state and returns the result if it's truthy.

Safety posture matches `cron_settings`: best-effort. If `season_type` is
blank/unknown we DO NOT skip — better to send than to wrongly suppress a
real in-season notification on a transient Sleeper hiccup. `force=True`
(manual/backfill invokes carrying an explicit week or `force` flag)
bypasses the guard entirely so testing + historical backfill still work.
"""
from __future__ import annotations

from typing import Any, Optional

from lambdas.common.constants import AI_REVIEW_WEEKLY_OK_SEASON_TYPES
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import success_response

log = get_logger(__file__)


def is_offseason(nfl_state: dict[str, Any]) -> bool:
    """True when NFL `season_type` is known AND not regular/post.

    Blank/unknown season_type returns False (don't suppress on a
    transient read failure) — same posture as `cron_settings`.
    """
    season_type = (nfl_state.get("season_type") or "").lower()
    return bool(season_type) and season_type not in AI_REVIEW_WEEKLY_OK_SEASON_TYPES


def offseason_skip(
    nfl_state: dict[str, Any],
    handler_name: str,
    *,
    force: bool = False,
) -> Optional[dict[str, Any]]:
    """Return a `skipped` success_response when out of season, else None.

    - `force=True` bypasses the guard (manual/backfill invokes).
    - Logs the skip at info so the absence of an offseason send is
      visible in CloudWatch without looking like an error.
    """
    if force:
        return None
    if not is_offseason(nfl_state):
        return None

    season_type = (nfl_state.get("season_type") or "").lower()
    log.info(
        f"{handler_name}: season_type={season_type!r} not in "
        f"{AI_REVIEW_WEEKLY_OK_SEASON_TYPES} — skipping offseason fire"
    )
    return success_response(
        {
            "Success": True,
            "skipped": True,
            "reason": "offseason",
            "season_type": season_type,
        },
        is_api=False,
    )
