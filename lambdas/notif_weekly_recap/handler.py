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
import requests
from lambdas.common.admin_only_filter import filter_to_admin_only
from lambdas.common.constants import TOTAL_REGULAR_WEEKS
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
from lambdas.common.worldcup_helper import (
    compute_division_standings,
    division_name_map_from_league,
    gather_chain_matchups,
    get_league_chain,
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

    # Cumulative season standings — read directly from each roster's
    # settings block (Sleeper's source of truth for W-L + PF). Sorted
    # wins-DESC, then PF-DESC for the email's standings table.
    def _roster_pf(r: dict[str, Any]) -> float:
        s = r.get("settings") or {}
        return float(s.get("fpts", 0)) + float(s.get("fpts_decimal", 0)) / 100.0

    standings_rows: list[tuple[str, int, int, int, float]] = sorted(
        [
            (
                team_name_for_roster(r["roster_id"]),
                int((r.get("settings") or {}).get("wins", 0)),
                int((r.get("settings") or {}).get("losses", 0)),
                int((r.get("settings") or {}).get("ties", 0)),
                _roster_pf(r),
            )
            for r in rosters
        ],
        key=lambda t: (t[1], t[4]),
        reverse=True,
    )

    # Playoff bracket — only fetch from Sleeper once the playoffs are
    # near (week >= 12) so we don't burn a network call every Tuesday
    # in September. Sleeper materializes the bracket once the
    # commissioner sets playoff_week_start in league settings.
    bracket_rounds_payload: list[tuple[str, list[tuple[str, float, str, float]]]] | None = None
    if target_week >= 12:
        try:
            br_resp = requests.get(
                f"https://api.sleeper.app/v1/league/{league_id}/winners_bracket",
                timeout=10,
            )
            bracket_raw = br_resp.json() if br_resp.status_code == 200 else []
        except Exception as e:
            log.warning(f"winners_bracket fetch failed: {e}")
            bracket_raw = []
        if bracket_raw:
            bracket_rounds_payload = _bracket_to_rounds(
                bracket_raw,
                team_name_for_roster,
            )

    # World Cup standings — walk the league chain and aggregate
    # divisional matchups across all seasons. Heavyweight (many
    # Sleeper calls) so we compute once before the per-manager loop.
    wc_full_payload: list[tuple[str, list[dict[str, Any]]]] | None = None
    wc_by_user: dict[str, dict[str, Any]] = {}
    try:
        chain = get_league_chain(league_id, fetch_league_fn=get_sleeper_league)
        head_league = chain[0] if chain else {}
        div_names = division_name_map_from_league(head_league)
        chain_matchups = gather_chain_matchups(
            chain,
            total_regular_weeks=TOTAL_REGULAR_WEEKS,
            fetch_rosters_fn=get_sleeper_league_rosters,
            fetch_users_fn=get_sleeper_league_users,
            fetch_matchups_fn=get_sleeper_league_matchups,
            log_fn=log.warning,
        )
        wc_standings = compute_division_standings(chain_matchups, div_names)
        wc_full_payload, wc_by_user = _wc_to_template_payload(wc_standings)
    except Exception as e:
        log.warning(f"World Cup computation failed; skipping WC sections: {e}")

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
            wc_personal = wc_by_user.get(owner_id)
            template_kwargs: dict[str, Any] = {
                "manager_name": manager_name,
                "league_name": league_name,
                "week": target_week,
                "user_team_name": user_team_name,
                "user_points": me_pts,
                "opponent_team_name": opp_team_name,
                "opponent_points": opp_pts,
                "user_won": user_won,
                "is_tie": is_tie,
                "league_high_team": top_team_name,
                "league_high_points": top_pts,
                "all_scores": all_scores,
                "standings": standings_rows,
                "bracket_rounds": bracket_rounds_payload,
                "wc_personal": wc_personal,
                # Mark only the current recipient as `is_user` so the
                # full-WC table highlights their row, not every row.
                "wc_divisions": _mark_user_in_wc(wc_full_payload, owner_id) if wc_full_payload else None,
            }
            html = generate_weekly_recap_email(**template_kwargs)
            text = generate_weekly_recap_email_plain_text(**template_kwargs)
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


# ---------------------------------------------------------------------------
# Section payload builders
# ---------------------------------------------------------------------------


def _bracket_to_rounds(
    bracket_raw: list[dict[str, Any]],
    team_name_for_roster,
) -> list[tuple[str, list[tuple[str, float, str, float]]]]:
    """Convert Sleeper's winners_bracket array into the template's
    (round_label, [(team_a, score_a, team_b, score_b)]) shape.

    Sleeper item shape: {r: round, m: matchup_id, t1, t2 (roster ids
    or {w: prev_m} placeholders), w, l (resolved winner/loser),
    t1_from, t2_from}. Scores aren't in the bracket payload — we use
    `None` for unplayed matchups so the template renders an "X vs Y"
    line without scores.
    """
    rounds: dict[int, list[tuple[str, float | None, str, float | None]]] = {}
    for item in bracket_raw:
        rd = int(item.get("r") or 0)
        if rd <= 0:
            continue
        t1 = item.get("t1")
        t2 = item.get("t2")
        a_name = team_name_for_roster(t1) if isinstance(t1, int) else "TBD"
        b_name = team_name_for_roster(t2) if isinstance(t2, int) else "TBD"
        # Scores not on the bracket payload; leave None.
        rounds.setdefault(rd, []).append((a_name, None, b_name, None))

    # Generic per-round labels — Sleeper bracket payloads don't carry
    # explicit "Quarterfinal" / "Semifinal" tags, so derive from
    # round count: last round is "Championship", second-to-last is
    # "Semifinals", etc.
    sorted_rounds = sorted(rounds.keys())
    label_for: dict[int, str] = {}
    total = len(sorted_rounds)
    for idx, rd in enumerate(sorted_rounds):
        depth_from_end = total - idx
        if depth_from_end == 1:
            label_for[rd] = "Championship"
        elif depth_from_end == 2:
            label_for[rd] = "Semifinals"
        elif depth_from_end == 3:
            label_for[rd] = "Quarterfinals"
        else:
            label_for[rd] = f"Round {rd}"

    return [(label_for[rd], rounds[rd]) for rd in sorted_rounds]


def _wc_to_template_payload(
    wc_standings: list[tuple[int, str, list[Any]]],
) -> tuple[list[tuple[str, list[dict[str, Any]]]], dict[str, dict[str, Any]]]:
    """Convert `compute_division_standings` output into the template's
    full-table shape AND a per-user lookup so the per-recipient
    personal panel can be built without re-running the aggregation.

    Returns:
        (wc_divisions, by_user) where
        wc_divisions = [(division_name, [{team_name, wins, losses,
                       ties, points_for, status, is_user}])]
        by_user      = {user_id: {division, position, status, wins,
                       losses, ties, points_for, points_back}}
    """
    wc_divisions: list[tuple[str, list[dict[str, Any]]]] = []
    by_user: dict[str, dict[str, Any]] = {}

    for _div_id, div_name, teams in wc_standings:
        # `teams` is sorted wins-DESC then PF-DESC. The conservative
        # cutoff for top-2 is element index 1's wins (worst clinched
        # wins) — anyone below that AND below by enough wins is
        # eliminated; otherwise alive. The clinch_status field is set
        # by clinch_for_division upstream.
        cutoff_wins = teams[1].wins if len(teams) >= 2 else 0
        team_dicts: list[dict[str, Any]] = []
        for position, t in enumerate(teams, start=1):
            team_dicts.append({
                "team_name": t.team_name,
                "user_id": t.user_id,
                "wins": t.wins,
                "losses": t.losses,
                "ties": t.ties,
                "points_for": t.points_for,
                "status": t.clinch_status,
                # `is_user` is filled in per-recipient via
                # `_mark_user_in_wc`.
                "is_user": False,
            })
            points_back = None
            if position > 2 and t.clinch_status != "eliminated":
                # Rough proxy — how many wins back from the 2-seed.
                wins_back = max(cutoff_wins - t.wins, 0)
                if wins_back > 0:
                    points_back = float(wins_back) * 100.0  # placeholder magnitude
            by_user[t.user_id] = {
                "division": div_name,
                "position": position,
                "status": t.clinch_status,
                "wins": t.wins,
                "losses": t.losses,
                "ties": t.ties,
                "points_for": t.points_for,
                "points_back": points_back,
            }
        wc_divisions.append((div_name, team_dicts))

    return wc_divisions, by_user


def _mark_user_in_wc(
    wc_divisions: list[tuple[str, list[dict[str, Any]]]],
    user_id: str,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Return a copy of `wc_divisions` with `is_user=True` on the one
    row whose `user_id` matches `user_id` (anywhere across divisions).
    Lets each manager's email highlight only their own row without
    re-aggregating everything per recipient."""
    if not user_id:
        return wc_divisions
    out: list[tuple[str, list[dict[str, Any]]]] = []
    for div_name, rows in wc_divisions:
        out_rows = [
            {**row, "is_user": row.get("user_id") == user_id}
            for row in rows
        ]
        out.append((div_name, out_rows))
    return out
