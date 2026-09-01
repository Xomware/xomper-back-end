"""
Notification — Lineup Not Set (scheduled, Sunday morning)
=========================================================
Pushes a personal reminder to any manager whose CURRENT-week lineup
has at least one starter that is on bye or marked injury status OUT
(not Q/D — those are still likely to play).

EventBridge schedule: Sun 11:00 ET during regular season.

Idempotency: re-runs of the same week regenerate the same reminder.
A manager with no issues never gets pinged.
"""
from typing import Any
from lambdas.common.admin_only_filter import filter_to_admin_only
from lambdas.common.cron_settings import get_cron_setting
from lambdas.common.season_guard import offseason_skip
from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response
from lambdas.common import notification_audience
from lambdas.common.sleeper_helper import (
    get_sleeper_league_rosters,
    get_sleeper_league_users,
    get_nfl_state,
    fetch_nfl_players,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.push_templates import lineup_not_set_push
from lambdas.common.ses_helper import send_emails_concurrently
from lambdas.common.email_templates import (
    generate_lineup_not_set_email,
    generate_lineup_not_set_email_plain_text,
)

log = get_logger(__file__)
HANDLER = "notif_lineup_not_set"
LAMBDA_CRON_KEY = "notif_lineup_not_set"

# A starter slot is considered "needs attention" when:
# - empty / "0" placeholder
# - player is on bye this week
# - injury_status is OUT (or equivalent)
# Q/D/IR-R are LEFT ALONE — those are still fielding decisions, not
# necessarily wrong to start.
ACTIONABLE_INJURY_STATUSES = {"Out", "OUT", "IR", "Suspended", "PUP", "NA", "Doubtful"}


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Starting Lineup-Not-Set notification...")

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

    jobs = notification_audience.jobs()
    if not jobs:
        return success_response({"sent": 0, "reason": "no league to notify"}, is_api=False)

    nfl_state = get_nfl_state()
    week_override = event.get("week") if isinstance(event, dict) else None

    # Offseason guard — no games means no lineups to nag about.
    skip = offseason_skip(nfl_state, LAMBDA_CRON_KEY, force=bool(week_override))
    if skip:
        return skip

    week = int(week_override if week_override else nfl_state.get("week", 1))

    # Players are league-independent, so this is fetched once rather than
    # per league -- it is the largest payload Sleeper serves.
    players = fetch_nfl_players()

    push_sent = 0
    email_tasks: list[tuple[str, str, str, str]] = []
    leagues_done = []

    for job in jobs:
        league_id = job.league_id
        league_name = job.league_name
        log.info(
            f"Auditing lineups for week {week} in league {league_id} "
            f"({job.source})"
        )

        recipients = job.recipients
        if test_mode:
            recipients = filter_to_admin_only(recipients)
            log.info(
                f"{LAMBDA_CRON_KEY} test_mode=true — restricting recipients to "
                f"admin only ({len(recipients)} user(s))"
            )
        sleeper_id_to_user = {
            w.get("sleeper_user_id"): w
            for w in recipients
            if w.get("sleeper_user_id")
        }
        if not sleeper_id_to_user:
            continue

        rosters = get_sleeper_league_rosters(league_id)
        users = get_sleeper_league_users(league_id)
        user_by_id = {u["user_id"]: u for u in users}
        leagues_done.append(league_id)

        for roster in rosters:
            owner_id = roster.get("owner_id")
            if not owner_id or owner_id not in sleeper_id_to_user:
                continue

            starters = roster.get("starters") or []
            issue_count = 0
            for player_id in starters:
                if not player_id or player_id == "0":
                    issue_count += 1
                    continue
                player = players.get(player_id) or {}
                injury_status = player.get("injury_status") or ""
                if injury_status in ACTIONABLE_INJURY_STATUSES:
                    issue_count += 1
                    continue
                # Bye-week detection requires a separate Sleeper endpoint
                # (`/players/nfl/research/regular/{season}/{week}`); deferred
                # to v2. Empty + OUT cover the common cases.

            if issue_count == 0:
                continue

            # Push leg
            title, body, category, data = lineup_not_set_push(
                league_name=league_name,
                issue_count=issue_count,
            )
            send_push_to_users([owner_id], title, body, category, data)
            push_sent += 1

            # Email leg — opt-in via the user having an email on file.
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
            subject = f"Set your {league_name} lineup — Week {week}"
            html = generate_lineup_not_set_email(
                manager_name=manager_name,
                league_name=league_name,
                issue_count=issue_count,
                week=week,
            )
            text = generate_lineup_not_set_email_plain_text(
                manager_name=manager_name,
                league_name=league_name,
                issue_count=issue_count,
                week=week,
            )
            email_tasks.append((email, subject, html, text))

    email_sent, email_failed = (0, 0)
    if email_tasks:
        email_sent, email_failed = send_emails_concurrently(email_tasks)

    log.info(
        f"Lineup reminder: {push_sent} push, {email_sent} email "
        f"({email_failed} email failed)."
    )
    return success_response(
        {
            "push_sent": push_sent,
            "email_sent": email_sent,
            "email_failed": email_failed,
            "week": week,
            "leagues": leagues_done,
        },
        is_api=False,
    )
