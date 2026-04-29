"""
Notification — World Cup Qualifier Movement (scheduled, Tuesday)
================================================================
After the weekly recap, recomputes World Cup qualifying standings
(cross-season aggregate, top-2 per division qualify, points-in-
divisional-games tiebreaker) and pushes managers whose clinch status
or qualifying-line position changed since the prior snapshot.

State:
- DynamoDB table `xomper-worldcup-snapshots` — keyed by
  `league_id#season` PK + `week` SK; stores the per-team status so we
  can diff week-over-week.

This v1 implementation is a SCAFFOLD — the divisional aggregation
across the dynasty chain is identical math to iOS `ClinchCalculator`
and `WorldCupStore.computeStandings`. Porting that logic faithfully
is its own pass; this handler:

1. Fetches the configured league + chain (via previous_league_id walk)
2. Builds per-team per-season divisional W/L
3. Determines clinched/alive/eliminated per team given the configured
   `gamesRemaining` (defaults to 6 for the final season)
4. Loads the prior snapshot, diffs, sends push for transitions
5. Writes the new snapshot

For the initial deploy, leave `gamesRemaining` configurable via the
event payload so you can backfill / replay safely.
"""
from typing import Any
from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response
from lambdas.common.supabase_helper import (
    get_active_whitelisted_league,
    get_active_whitelisted_users,
)

log = get_logger(__file__)
HANDLER = "notif_worldcup_movement"

DEFAULT_GAMES_REMAINING = 6


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Starting World Cup Movement notification (scaffold)...")

    league_row = get_active_whitelisted_league()
    if not league_row:
        return success_response({"sent": 0, "reason": "no active league"}, is_api=False)

    if not league_row.get("has_taxi") or not league_row.get("is_dynasty"):
        log.info("Active league is not World-Cup eligible (needs has_taxi + is_dynasty). Skipping.")
        return success_response({"sent": 0, "reason": "not world-cup eligible"}, is_api=False)

    games_remaining = int(
        event.get("games_remaining") if isinstance(event, dict) and event.get("games_remaining") is not None
        else DEFAULT_GAMES_REMAINING
    )

    log.info(
        f"Computing World Cup standings for league {league_row['league_id']} "
        f"with games_remaining={games_remaining}"
    )

    # TODO(scaffold): port ClinchCalculator from iOS.
    #
    # Algorithm (mirrors Xomper/Core/Stores/ClinchCalculator.swift):
    # 1. Walk league chain backward via previous_league_id, collect
    #    every divisional matchup record across all seasons.
    # 2. Aggregate per-team W/L within their division.
    # 3. Sort each division by wins-DESC, points-for-DESC.
    # 4. For each team:
    #    - .clinched if no chaser (rank 3+) can reach team.wins even
    #      winning all `games_remaining` upcoming
    #    - .eliminated if team.wins + games_remaining < 2nd-place wins
    #    - .alive otherwise
    # 5. Diff against last snapshot in xomper-worldcup-snapshots
    #    (DynamoDB).
    # 6. Push each transition (clinched / eliminated / line-flipped).
    # 7. Write current week's snapshot.
    #
    # Until that's wired, this returns a stub response so the
    # EventBridge schedule + IAM + deployment pipeline can be validated
    # end-to-end without producing spurious notifications.

    return success_response(
        {
            "sent": 0,
            "reason": "scaffold — computeStandings logic not yet ported",
            "league_id": league_row["league_id"],
            "games_remaining": games_remaining,
        },
        is_api=False,
    )
