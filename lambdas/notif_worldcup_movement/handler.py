"""
Notification — World Cup Qualifier Movement (scheduled, Tuesday)
================================================================
Recomputes World Cup qualifying standings (cross-season divisional
aggregate, top-2 per division, points-for tiebreaker) and pushes
managers whose clinch status or qualifying-line position changed
since last week's snapshot.

Algorithm (mirrors iOS `Xomper/Core/Stores/ClinchCalculator.swift`
+ `WorldCupStore.computeStandings`):
1. Walk league chain backward via `previous_league_id`.
2. For every league in the chain, fetch users + rosters + all 17
   weeks of matchups; pair matchups by `matchup_id` and build the
   intra-divisional records.
3. Aggregate per-team W/L/PF across all divisional regular-season
   matchups across the entire chain.
4. Group by division, sort wins-DESC then PF-DESC.
5. Compute clinch/alive/eliminated per team given
   `games_remaining` (defaults to 6 in the final season).
6. Diff against the previous week's snapshot in
   `xomper-worldcup-snapshots`. For each user with a status or
   cutoff-line transition, send the corresponding push.
7. Write the new snapshot.

EventBridge schedule: Tuesday 10:00 ET during weeks 1-14.

Idempotency: re-running for the same week reads the same snapshot
back, sees no transitions, sends nothing. Safe to retry.
"""
import json
import os
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from lambdas.common.admin_only_filter import filter_to_admin_only
from lambdas.common.cron_settings import get_cron_setting
from lambdas.common.season_guard import offseason_skip
from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response
from lambdas.common import notification_audience
from lambdas.common.sleeper_helper import (
    get_sleeper_league,
    get_sleeper_league_rosters,
    get_sleeper_league_users,
    get_sleeper_league_matchups,
    get_nfl_state,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.push_templates import (
    worldcup_clinched_push,
    worldcup_eliminated_push,
    worldcup_line_moved_push,
)
from lambdas.common.worldcup_helper import (
    DEFAULT_GAMES_REMAINING,
    CLINCHED,
    ELIMINATED,
    compute_division_standings,
    clinch_for_division,
    status_map_from_standings,
    diff_snapshots,
    get_league_chain,
    division_name_map_from_league,
)

log = get_logger(__file__)
HANDLER = "notif_worldcup_movement"
LAMBDA_CRON_KEY = "notif_worldcup_movement"

SNAPSHOT_TABLE = os.environ.get("APP_NAME", "xomper") + "-worldcup-snapshots"
TOTAL_REGULAR_WEEKS = 17

_dynamodb = boto3.resource("dynamodb")


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Starting World Cup Movement notification...")

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

    # Offseason guard — standings don't move once the season ends. A
    # manual week/games_remaining override bypasses for backfill/testing.
    _override = isinstance(event, dict) and (
        event.get("week") is not None or event.get("games_remaining") is not None
    )
    skip = offseason_skip(get_nfl_state(), LAMBDA_CRON_KEY, force=bool(_override))
    if skip:
        return skip

    games_remaining = int(
        event.get("games_remaining")
        if isinstance(event, dict) and event.get("games_remaining") is not None
        else DEFAULT_GAMES_REMAINING
    )
    week = int(
        event.get("week")
        if isinstance(event, dict) and event.get("week") is not None
        else 0  # 0 = pre-season placeholder; production cron passes the current week
    )

    sent = 0
    transitions_seen = 0
    leagues_done = []

    for job in jobs:
        s, t = _worldcup_for_league(job, week, games_remaining, test_mode)
        if t:
            leagues_done.append(job.league_id)
        sent += s
        transitions_seen += t

    log.info(
        f"World Cup movement: {sent} pushes across {transitions_seen} "
        f"transitions in {len(leagues_done)} league(s)."
    )
    return success_response(
        {
            "sent": sent,
            "transitions": transitions_seen,
            "leagues": leagues_done,
            "week": week,
            "games_remaining": games_remaining,
        },
        is_api=False,
    )


# MARK: - Internal helpers


def _gather_chain_matchups(chain: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Walk every league in the chain, fetch its users/rosters/all
    17 weeks, pair matchups by matchup_id, and emit
    MatchupHistoryRecord-shaped dicts for the aggregator.

    Skips leagues with status `pre_draft` (no games yet).
    """
    records: list[dict[str, Any]] = []
    for league in chain:
        if league.get("status") == "pre_draft":
            continue
        league_id = league["league_id"]
        season = league.get("season", "")
        rosters = get_sleeper_league_rosters(league_id)
        users = get_sleeper_league_users(league_id)
        roster_by_id = {r["roster_id"]: r for r in rosters}
        user_by_id = {u["user_id"]: u for u in users}

        for week in range(1, TOTAL_REGULAR_WEEKS + 1):
            try:
                week_matchups = get_sleeper_league_matchups(league_id, week)
            except Exception as e:
                log.warning(f"Week {week} matchup fetch failed for {league_id}: {e}")
                continue

            grouped: dict[int, list[dict[str, Any]]] = {}
            for m in week_matchups:
                mid = m.get("matchup_id")
                if mid is None:
                    continue
                grouped.setdefault(mid, []).append(m)

            for pair in grouped.values():
                if len(pair) != 2:
                    continue
                a, b = pair
                roster_a = roster_by_id.get(a["roster_id"]) or {}
                roster_b = roster_by_id.get(b["roster_id"]) or {}
                user_a = user_by_id.get(roster_a.get("owner_id") or "") or {}
                user_b = user_by_id.get(roster_b.get("owner_id") or "") or {}
                a_pts = float(a.get("points") or 0)
                b_pts = float(b.get("points") or 0)
                if a_pts > b_pts:
                    winner = a["roster_id"]
                elif b_pts > a_pts:
                    winner = b["roster_id"]
                else:
                    winner = None

                records.append({
                    "league_id": league_id,
                    "season": season,
                    "week": week,
                    "team_a_user_id": roster_a.get("owner_id") or "",
                    "team_a_username": user_a.get("username") or "",
                    "team_a_team_name": (user_a.get("metadata") or {}).get("team_name") or user_a.get("display_name") or "",
                    "team_a_division": roster_a.get("settings", {}).get("division") or 0,
                    "team_a_points": a_pts,
                    "team_b_user_id": roster_b.get("owner_id") or "",
                    "team_b_username": user_b.get("username") or "",
                    "team_b_team_name": (user_b.get("metadata") or {}).get("team_name") or user_b.get("display_name") or "",
                    "team_b_division": roster_b.get("settings", {}).get("division") or 0,
                    "team_b_points": b_pts,
                    "winner_roster_id": winner,
                    "is_playoff": week > 14,
                })
    return records


def _snapshot_pk(league_id: str, season: str) -> str:
    return f"{league_id}#{season}"


def _read_snapshot(league_id: str, season: str, week: int) -> dict[str, dict[str, Any]]:
    """Read the prior week's snapshot from DynamoDB. Returns an empty
    dict on first run / missing entry — first run produces no events
    by design (handled in `diff_snapshots`)."""
    if week <= 0:
        return {}
    table = _dynamodb.Table(SNAPSHOT_TABLE)
    try:
        resp = table.get_item(
            Key={"league_id_season": _snapshot_pk(league_id, season), "week": week},
            ConsistentRead=False,
        )
    except Exception as e:
        log.warning(f"Snapshot read failed: {e}")
        return {}
    item = resp.get("Item")
    if not item:
        return {}
    raw = item.get("status_map")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return raw or {}


def _write_snapshot(
    league_id: str,
    season: str,
    week: int,
    status_map: dict[str, dict[str, Any]],
) -> None:
    """Persist the current week's snapshot. Stored as a JSON string
    in `status_map` so deeply-nested per-user records don't run into
    DynamoDB's number-precision quirks (rank is an int but
    boto3.resource serializes ints as Decimal, which JSON-roundtrips
    cleaner)."""
    table = _dynamodb.Table(SNAPSHOT_TABLE)
    try:
        table.put_item(
            Item={
                "league_id_season": _snapshot_pk(league_id, season),
                "week": week,
                "status_map": json.dumps(status_map),
            }
        )
    except Exception as e:
        log.error(f"Snapshot write failed: {e}")


def _worldcup_for_league(
    job: notification_audience.NotificationJob,
    week: int,
    games_remaining: int,
    test_mode: bool,
) -> tuple[int, int]:
    """Run the World Cup diff for one league. Returns (sent, transitions).

    Eligibility is read from Sleeper rather than the whitelist row's
    has_taxi / is_dynasty columns, which only ever existed for the one
    configured league and were maintained by hand.

    This walks the whole league chain and every regular-season week, so
    the recipient check comes first: an ineligible or unwatched league
    must not pay for the walk.
    """
    head_league_id = job.league_id

    recipients = job.recipients
    if test_mode:
        recipients = filter_to_admin_only(recipients)
    eligible_user_ids = {
        w.get("sleeper_user_id") for w in recipients if w.get("sleeper_user_id")
    }
    if not eligible_user_ids:
        return (0, 0)

    head = get_sleeper_league(head_league_id) or {}
    settings = head.get("settings") or {}
    # Sleeper: type 2 is dynasty; taxi_slots 0 means no taxi squad.
    if int(settings.get("type", 0)) != 2 or int(settings.get("taxi_slots", 0)) <= 0:
        log.info(f"League {head_league_id} is not World-Cup eligible. Skipping.")
        return (0, 0)

    # Step 1: walk the chain
    chain = get_league_chain(head_league_id, fetch_league_fn=get_sleeper_league)
    if not chain:
        return (0, 0)
    head_league = chain[0]
    season = head_league.get("season", "")
    division_name_map = division_name_map_from_league(head_league)

    # Step 2: build matchup records from each league in the chain
    matchups = _gather_chain_matchups(chain)

    # Steps 3-5: aggregate + clinch
    standings = compute_division_standings(matchups, division_name_map)
    for _div_id, _div_name, teams in standings:
        clinch_for_division(teams, games_remaining=games_remaining)

    current = status_map_from_standings(standings)

    # Step 6: diff against previous snapshot
    prev = _read_snapshot(head_league_id, season, max(week - 1, 0))
    transitions = diff_snapshots(current, prev)

    # Step 7: persist current snapshot
    _write_snapshot(head_league_id, season, week, current)

    if not transitions:
        log.info("No transitions this week. No pushes sent.")
        return (0, 0)

    sent = 0
    for event_dict in transitions:
        user_id = event_dict["user_id"]
        if user_id not in eligible_user_ids:
            continue

        division_name = division_name_map.get(
            event_dict.get("division", 0),
            f"Division {event_dict.get('division', 0)}",
        )

        if event_dict["kind"] == "status":
            to_status = event_dict["to"]
            if to_status == CLINCHED:
                title, body, category, data = worldcup_clinched_push(division_name)
            elif to_status == ELIMINATED:
                title, body, category, data = worldcup_eliminated_push(division_name)
            else:
                # alive ← clinched/eliminated reversal — no push (rare,
                # only happens on a backfill / replay; managers don't
                # want a "JK you're alive again" alert).
                continue
        else:  # line crossing
            crossed_into = event_dict["direction"] == "in"
            title, body, category, data = worldcup_line_moved_push(division_name, crossed_into)

        send_push_to_users([user_id], title, body, category, data)
        sent += 1


    return sent, len(transitions)