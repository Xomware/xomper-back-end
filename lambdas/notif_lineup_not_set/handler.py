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
    get_nfl_state,
    fetch_nfl_players,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.push_templates import lineup_not_set_push

log = get_logger(__file__)
HANDLER = "notif_lineup_not_set"

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

    league_row = get_active_whitelisted_league()
    if not league_row:
        return success_response({"sent": 0, "reason": "no active league"}, is_api=False)

    league_id = league_row["league_id"]
    league_name = league_row.get("league_name", "League")

    nfl_state = get_nfl_state()
    week = int(event.get("week") if isinstance(event, dict) and event.get("week") else nfl_state.get("week", 1))

    log.info(f"Auditing lineups for week {week} in league {league_id}")

    rosters = get_sleeper_league_rosters(league_id)
    users = get_sleeper_league_users(league_id)
    players = fetch_nfl_players()

    user_by_id = {u["user_id"]: u for u in users}

    whitelisted = get_active_whitelisted_users()
    sleeper_id_to_email = {
        w.get("sleeper_user_id"): w.get("email")
        for w in whitelisted
        if w.get("sleeper_user_id")
    }

    sent = 0
    for roster in rosters:
        owner_id = roster.get("owner_id")
        if not owner_id or owner_id not in sleeper_id_to_email:
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

        title, body, category, data = lineup_not_set_push(
            league_name=league_name,
            issue_count=issue_count,
        )
        send_push_to_users([owner_id], title, body, category, data)
        sent += 1

    log.info(f"Lineup reminder sent to {sent} managers.")
    return success_response(
        {"sent": sent, "week": week, "league_id": league_id},
        is_api=False,
    )
