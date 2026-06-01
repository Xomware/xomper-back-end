"""
Notification — Weekly Recap (scheduled)
=======================================
Runs Tuesday morning during the regular season + playoffs. For the
active whitelisted league, fetches the just-completed week's matchups
and sends a personalized push + email to every manager:

- Personal: "You won/lost X–Y vs <opponent>"
- League: top scorer of the week, biggest blowout, closest game

Triggered by EventBridge cron (no API Gateway). Event payload is the
EventBridge scheduled event shape — body parsing is irrelevant; we
read directly from Supabase + Sleeper.

Idempotency: re-invoking for the same week regenerates the same
content and sends again. Acceptable for a recap (low blast radius);
if needed, gate behind a DynamoDB "last sent week" record.
"""
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
    get_sleeper_league,
    get_sleeper_league_rosters,
    get_sleeper_league_users,
    get_sleeper_league_matchups,
    get_nfl_state,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.push_templates import weekly_recap_push
from lambdas.common.ses_helper import send_emails_concurrently
from lambdas.common.email_templates import (
    generate_weekly_recap_email,
    generate_weekly_recap_email_plain_text,
)

log = get_logger(__file__)
HANDLER = "notif_weekly_recap"
LAMBDA_CRON_KEY = "notif_weekly_recap"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Starting Weekly Recap notification...")

    # 0. Admin cron settings gate (admin-cron-settings).
    #    `enabled=false` → no-op skip. `test_mode=true` → restrict
    #    recipient list to admin only before SES/SNS fan-out.
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

    # 1. Resolve active league
    league_row = get_active_whitelisted_league()
    if not league_row:
        log.warning("No active whitelisted league configured. Skipping.")
        return success_response({"sent": 0, "reason": "no active league"}, is_api=False)

    league_id = league_row["league_id"]
    league_name = league_row.get("league_name", "League")

    # 2. Determine the week to recap. Override via event["week"] for
    #    backfill / testing; otherwise use the just-completed NFL week.
    nfl_state = get_nfl_state()
    current_week = nfl_state.get("week", 1)
    week_override = event.get("week") if isinstance(event, dict) else None
    target_week = int(week_override) if week_override else max(current_week - 1, 1)

    log.info(f"Recapping week {target_week} for league {league_id} ({league_name})")

    # 3. Pull rosters, users, matchups
    rosters = get_sleeper_league_rosters(league_id)
    users = get_sleeper_league_users(league_id)
    matchups = get_sleeper_league_matchups(league_id, target_week)

    # 4. Build helpful indexes
    user_by_id = {u["user_id"]: u for u in users}
    roster_by_id = {r["roster_id"]: r for r in rosters}

    def team_name_for_roster(roster_id: int) -> str:
        roster = roster_by_id.get(roster_id) or {}
        owner_id = roster.get("owner_id")
        user = user_by_id.get(owner_id, {}) if owner_id else {}
        team_name = (user.get("metadata") or {}).get("team_name")
        return team_name or user.get("display_name") or "Unknown"

    # 5. Pair matchups by matchup_id
    by_matchup: dict[int, list[dict[str, Any]]] = {}
    for m in matchups:
        mid = m.get("matchup_id")
        if mid is None:
            continue
        by_matchup.setdefault(mid, []).append(m)

    # 6. Compute league-wide stats: highest single-team score
    if not matchups:
        log.warning(f"No matchup data for week {target_week}. Skipping.")
        return success_response({"sent": 0, "reason": "no matchup data"}, is_api=False)

    top_entry = max(matchups, key=lambda m: m.get("points", 0.0))
    top_team_name = team_name_for_roster(top_entry["roster_id"])
    top_pts = float(top_entry.get("points", 0.0))

    # 7. Resolve recipients from Supabase (sleeper_user_id → push + email)
    whitelisted = get_active_whitelisted_users()
    if test_mode:
        whitelisted = filter_to_admin_only(whitelisted)
        log.info(
            f"{LAMBDA_CRON_KEY} test_mode=true — restricting recipients to "
            f"admin only ({len(whitelisted)} user(s))"
        )
    sleeper_id_to_user = {
        w.get("sleeper_user_id"): w
        for w in whitelisted
        if w.get("sleeper_user_id")
    }

    # Pre-compute the league-wide standings table once so each manager's
    # email gets the same sorted score listing.
    all_scores: list[tuple[str, float]] = sorted(
        [
            (team_name_for_roster(m["roster_id"]), float(m.get("points", 0.0)))
            for m in matchups
        ],
        key=lambda t: t[1],
        reverse=True,
    )

    push_sent = 0
    email_tasks: list[tuple[str, str, str, str]] = []

    # 8. For each matchup pair, send personalized push + email to both managers
    for mid, pair in by_matchup.items():
        if len(pair) != 2:
            continue  # skip byes / malformed
        a, b = pair
        a_pts = float(a.get("points", 0.0))
        b_pts = float(b.get("points", 0.0))

        for me, opp, me_pts, opp_pts in [
            (a, b, a_pts, b_pts),
            (b, a, b_pts, a_pts),
        ]:
            roster = roster_by_id.get(me["roster_id"]) or {}
            owner_id = roster.get("owner_id")
            if not owner_id:
                continue
            if owner_id not in sleeper_id_to_user:
                # Manager not in our whitelisted_users — skip silently.
                continue

            user_team_name = team_name_for_roster(me["roster_id"])
            opp_team_name = team_name_for_roster(opp["roster_id"])
            user_won = me_pts > opp_pts
            is_tie = abs(me_pts - opp_pts) < 0.0001

            title, body, category, data = weekly_recap_push(
                week=target_week,
                user_team_name=user_team_name,
                user_won=user_won,
                user_pts=me_pts,
                opp_team_name=opp_team_name,
                opp_pts=opp_pts,
                league_high_team=top_team_name,
                league_high_pts=top_pts,
            )
            send_push_to_users([owner_id], title, body, category, data)
            push_sent += 1

            wl_user = sleeper_id_to_user[owner_id]
            email = wl_user.get("email")
            if not email:
                continue
            manager_name = (
                wl_user.get("display_name")
                or user_by_id.get(owner_id, {}).get("display_name")
                or wl_user.get("sleeper_username")
                or "Manager"
            )
            subject = f"Week {target_week} {league_name} recap"
            html = generate_weekly_recap_email(
                manager_name=manager_name,
                league_name=league_name,
                week=target_week,
                user_team_name=user_team_name,
                user_points=me_pts,
                opponent_team_name=opp_team_name,
                opponent_points=opp_pts,
                user_won=user_won,
                is_tie=is_tie,
                league_high_team=top_team_name,
                league_high_points=top_pts,
                all_scores=all_scores,
            )
            text = generate_weekly_recap_email_plain_text(
                manager_name=manager_name,
                league_name=league_name,
                week=target_week,
                user_team_name=user_team_name,
                user_points=me_pts,
                opponent_team_name=opp_team_name,
                opponent_points=opp_pts,
                user_won=user_won,
                is_tie=is_tie,
                league_high_team=top_team_name,
                league_high_points=top_pts,
                all_scores=all_scores,
            )
            email_tasks.append((email, subject, html, text))

    email_sent, email_failed = (0, 0)
    if email_tasks:
        email_sent, email_failed = send_emails_concurrently(email_tasks)

    log.info(
        f"Weekly recap: {push_sent} push, {email_sent} email "
        f"({email_failed} email failed) for week {target_week}."
    )
    return success_response(
        {
            "push_sent": push_sent,
            "email_sent": email_sent,
            "email_failed": email_failed,
            "week": target_week,
            "league_id": league_id,
        },
        is_api=False,
    )
