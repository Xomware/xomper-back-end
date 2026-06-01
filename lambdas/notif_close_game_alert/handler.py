"""
Notification — Close Game Alert (scheduled, Sunday + Monday primetime)
======================================================================
At Sun 8pm ET and Mon 8pm ET, scans live matchup scores for the
active league. For any matchup where the score is within 10 points,
pushes both managers an alert.

Schedule:
- Sun 20:00 ET → cron(0 0 ? * MON *)
- Mon 20:00 ET → cron(0 1 ? * TUE *)

Idempotency: dedupe by (league_id-week-matchup_id-slot) in
DynamoDB to avoid double-firing on retries. The slot is derived from
the current weekday (`sun` / `mon`).

For the v1 stub the dedupe table is *not* required — both schedules
fire once and only once per primetime. Add when retries become a real
concern.
"""
from datetime import datetime, timezone
from typing import Any

from lambdas.common.admin_only_filter import filter_to_admin_only
from lambdas.common.cron_settings import get_cron_setting
from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response
from lambdas.common.supabase_helper import (
    get_active_whitelisted_league,
    get_active_whitelisted_users,
)
from lambdas.common.sleeper_helper import (
    get_sleeper_league_rosters,
    get_sleeper_league_users,
    get_sleeper_league_matchups,
    get_nfl_state,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.push_templates import close_game_push

log = get_logger(__file__)
HANDLER = "notif_close_game_alert"
LAMBDA_CRON_KEY = "notif_close_game_alert"

CLOSE_GAME_THRESHOLD = 10.0  # points


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Starting Close Game Alert notification...")

    # Admin cron settings gate (admin-cron-settings).
    cron_setting = get_cron_setting(LAMBDA_CRON_KEY)
    if not cron_setting["enabled"]:
        log.info(
            f"{LAMBDA_CRON_KEY} disabled via admin_cron_settings — skipping"
        )
        return success_response(
            {"Success": True, "skipped": True, "reason": "disabled"},
            is_api=False,
        )
    test_mode = bool(cron_setting["test_mode"])

    league_row = get_active_whitelisted_league()
    if not league_row:
        return success_response({"sent": 0, "reason": "no active league"}, is_api=False)

    league_id = league_row["league_id"]
    nfl_state = get_nfl_state()
    week = int(event.get("week") if isinstance(event, dict) and event.get("week") else nfl_state.get("week", 1))

    log.info(f"Scanning live scores for week {week} in league {league_id}")

    rosters = get_sleeper_league_rosters(league_id)
    users = get_sleeper_league_users(league_id)
    matchups = get_sleeper_league_matchups(league_id, week)

    if not matchups:
        return success_response({"sent": 0, "reason": "no matchup data"}, is_api=False)

    user_by_id = {u["user_id"]: u for u in users}
    roster_by_id = {r["roster_id"]: r for r in rosters}

    whitelisted = get_active_whitelisted_users()
    if test_mode:
        whitelisted = filter_to_admin_only(whitelisted)
        log.info(
            f"{LAMBDA_CRON_KEY} test_mode=true — restricting recipients to "
            f"admin only ({len(whitelisted)} user(s))"
        )
    sleeper_id_to_email = {
        w.get("sleeper_user_id"): w.get("email")
        for w in whitelisted
        if w.get("sleeper_user_id")
    }

    def team_name_for_roster(roster_id: int) -> str:
        roster = roster_by_id.get(roster_id) or {}
        owner_id = roster.get("owner_id")
        user = user_by_id.get(owner_id, {}) if owner_id else {}
        team_name = (user.get("metadata") or {}).get("team_name")
        return team_name or user.get("display_name") or "Unknown"

    by_matchup: dict[int, list[dict[str, Any]]] = {}
    for m in matchups:
        mid = m.get("matchup_id")
        if mid is None:
            continue
        by_matchup.setdefault(mid, []).append(m)

    sent = 0
    for mid, pair in by_matchup.items():
        if len(pair) != 2:
            continue
        a, b = pair
        a_pts = float(a.get("points", 0.0))
        b_pts = float(b.get("points", 0.0))
        diff = abs(a_pts - b_pts)
        # Skip both-zero matchups (game hasn't started). Skip diffs over
        # the threshold. Skip exact ties at 0 (no signal).
        if a_pts == 0 and b_pts == 0:
            continue
        if diff > CLOSE_GAME_THRESHOLD:
            continue

        for me, opp, me_pts, opp_pts in [
            (a, b, a_pts, b_pts),
            (b, a, b_pts, a_pts),
        ]:
            roster = roster_by_id.get(me["roster_id"]) or {}
            owner_id = roster.get("owner_id")
            if not owner_id or owner_id not in sleeper_id_to_email:
                continue

            user_team_name = team_name_for_roster(me["roster_id"])
            opp_team_name = team_name_for_roster(opp["roster_id"])

            title, body, category, data = close_game_push(
                user_team_name=user_team_name,
                user_pts=me_pts,
                opp_team_name=opp_team_name,
                opp_pts=opp_pts,
            )
            send_push_to_users([owner_id], title, body, category, data)
            sent += 1

    log.info(f"Close game alerts sent to {sent} managers.")
    return success_response(
        {"sent": sent, "week": week, "league_id": league_id},
        is_api=False,
    )
