"""
Week Preview orchestrator
=========================
Pure orchestration logic for the Wednesday-morning Week Preview
newsletter. Mirrors `weekly_orchestrator.run_weekly` shape so both
`notif_week_preview` (EventBridge cron) and
`api_admin_ai_review_week_preview_trigger` (admin POST) share one
pipeline.

Flow:
1. Resolve the active whitelisted league.
2. Pre-flight: `season_type` must be `regular` or `post` (otherwise
   skip).
3. Resolve the upcoming week (caller override OR `nfl_state.week`).
4. Idempotency: bail with `ReportAlreadyExistsError` if a row exists
   for `(league_id, "weekPreview", "<season>W<NN>")` and `force=False`.
5. Pull data — rosters, users, this week's matchups, WC chain
   matchups (best-effort), recent memories.
6. Build the system + user prompts, call `claude_helper.generate`.
7. Parse Claude's JSON envelope. On parse failure, persist the raw
   response as `body_markdown`.
8. Persist via `ai_reports_store.write_report` with the standings +
   wc_divisions payloads pre-baked so the email send fan-out doesn't
   re-fetch.
9. Fan out emails (admin-only when dry_run; full league otherwise).
"""
from __future__ import annotations

import json
from typing import Any

from lambdas.common import (
    ai_memories_store,
    ai_reports_store,
    claude_helper,
)
from lambdas.common.admin_only_filter import filter_to_admin_only
from lambdas.common.constants import (
    AI_REVIEW_DEFAULT_MODEL,
    AI_REVIEW_WEEK_PREVIEW_MAX_TOKENS,
    AI_REVIEW_WEEK_PREVIEW_MEMORY_LOOKBACK,
    AI_REVIEW_WEEK_PREVIEW_OK_SEASON_TYPES,
    AI_REVIEW_WEEK_PREVIEW_PROMPT_VERSION,
    TOTAL_REGULAR_WEEKS,
)
from lambdas.common.email_templates import (
    generate_week_preview_email,
    generate_week_preview_email_plain_text,
)
from lambdas.common.errors import (
    NotFoundError,
    ReportAlreadyExistsError,
    ValidationError,
)
from lambdas.common.logger import get_logger
from lambdas.common.ses_helper import send_emails_concurrently
from lambdas.common.sleeper_helper import (
    get_nfl_state,
    get_sleeper_league,
    get_sleeper_league_matchups,
    get_sleeper_league_rosters,
    get_sleeper_league_users,
)
from lambdas.common.supabase_helper import (
    get_active_whitelisted_league,
    get_active_whitelisted_users,
)
from lambdas.common.week_preview_prompts import (
    build_system_blocks,
    build_user_prompt,
)
from lambdas.common.worldcup_helper import (
    compute_division_standings,
    division_name_map_from_league,
    gather_chain_matchups,
    get_league_chain,
)

log = get_logger(__file__)

REPORT_TYPE = "weekPreview"


def run_week_preview(
    *,
    week: int | None = None,
    dry_run: bool = True,
    force: bool = False,
    created_by_user_id: str | None = None,
) -> dict[str, Any]:
    """Generate + deliver one Week Preview newsletter for the active
    whitelisted league. Returns a dict describing what happened —
    suitable for the admin trigger response or cron log.

    Args:
        week: Optional override of the upcoming week. Defaults to
            `nfl_state.week` (the upcoming week per Sleeper).
        dry_run: When True (the cron's test_mode default), deliveries
            are restricted to the admin only.
        force: When True, skips the idempotency guard. Used to
            re-generate a stored preview after a prompt tweak.
        created_by_user_id: Sleeper user id of the admin who triggered
            this run. None when fired from the EventBridge cron.
    """
    # --- league + NFL state --------------------------------------------------
    league_row = get_active_whitelisted_league()
    if not league_row:
        raise NotFoundError(
            message="No active whitelisted league configured.",
            handler="week_preview_orchestrator",
        )
    league_id = league_row["league_id"]
    league_name = league_row.get("league_name", "League")

    nfl_state = get_nfl_state() or {}
    season_type = (nfl_state.get("season_type") or "").lower()
    nfl_season = str(nfl_state.get("season") or "")
    nfl_week_raw = nfl_state.get("week")

    if season_type and season_type not in AI_REVIEW_WEEK_PREVIEW_OK_SEASON_TYPES:
        log.info(
            f"week_preview: season_type={season_type!r} not in "
            f"{AI_REVIEW_WEEK_PREVIEW_OK_SEASON_TYPES} — skipping"
        )
        return {
            "status": "skipped_offseason",
            "season_type": season_type,
        }

    # --- resolve target week -------------------------------------------------
    if week is not None:
        try:
            resolved_week = max(int(week), 1)
        except (TypeError, ValueError):
            raise ValidationError(
                message="`week` override must be a positive integer",
                handler="week_preview_orchestrator",
                field="week",
            )
    else:
        try:
            resolved_week = max(int(nfl_week_raw or 1), 1)
        except (TypeError, ValueError):
            resolved_week = 1

    period = f"{nfl_season or 'unknown'}W{resolved_week:02d}"

    # --- idempotency ---------------------------------------------------------
    if not force:
        existing = ai_reports_store.get_report(
            league_id=league_id,
            report_type=REPORT_TYPE,
            period=period,
        )
        if existing:
            raise ReportAlreadyExistsError(
                message=(
                    f"A weekPreview report already exists for period {period}"
                ),
                handler="week_preview_orchestrator",
                existing={
                    "league_id": existing.get("league_id"),
                    "report_type": existing.get("report_type"),
                    "period": existing.get("period"),
                    "created_at": existing.get("created_at"),
                },
            )

    # --- data load -----------------------------------------------------------
    rosters = get_sleeper_league_rosters(league_id) or []
    sleeper_users = get_sleeper_league_users(league_id) or []
    matchups_raw = get_sleeper_league_matchups(league_id, resolved_week) or []

    user_by_id = {u["user_id"]: u for u in sleeper_users}
    roster_by_id = {r["roster_id"]: r for r in rosters}

    def team_name_for_roster(roster_id: int) -> str:
        roster = roster_by_id.get(roster_id) or {}
        owner_id = roster.get("owner_id")
        user = user_by_id.get(owner_id, {}) if owner_id else {}
        team_name = (user.get("metadata") or {}).get("team_name")
        return team_name or user.get("display_name") or "Unknown"

    def manager_for_roster(roster_id: int) -> str:
        roster = roster_by_id.get(roster_id) or {}
        owner_id = roster.get("owner_id")
        user = user_by_id.get(owner_id, {}) if owner_id else {}
        return user.get("display_name") or "Unknown"

    # Pair Sleeper matchup entries by matchup_id to form the
    # matchup_pairs prompt structure.
    grouped: dict[int, list[dict[str, Any]]] = {}
    for m in matchups_raw:
        mid = m.get("matchup_id")
        if mid is None:
            continue
        grouped.setdefault(mid, []).append(m)

    matchup_pairs: list[dict[str, Any]] = []
    for mid, pair in grouped.items():
        if len(pair) != 2:
            continue
        a, b = pair
        roster_a = roster_by_id.get(a["roster_id"]) or {}
        roster_b = roster_by_id.get(b["roster_id"]) or {}
        sa = roster_a.get("settings") or {}
        sb = roster_b.get("settings") or {}
        matchup_pairs.append({
            "matchup_id": mid,
            "team_a": {
                "team_name": team_name_for_roster(a["roster_id"]),
                "manager": manager_for_roster(a["roster_id"]),
                "user_id": roster_a.get("owner_id"),
                "wins": int(sa.get("wins", 0)),
                "losses": int(sa.get("losses", 0)),
                "starters": a.get("starters") or [],
            },
            "team_b": {
                "team_name": team_name_for_roster(b["roster_id"]),
                "manager": manager_for_roster(b["roster_id"]),
                "user_id": roster_b.get("owner_id"),
                "wins": int(sb.get("wins", 0)),
                "losses": int(sb.get("losses", 0)),
                "starters": b.get("starters") or [],
            },
        })

    # Cumulative standings sorted wins-DESC / PF-DESC, mirrored from
    # the weekly_recap handler so the prompt + email tables stay
    # consistent.
    def _roster_pf(r: dict[str, Any]) -> float:
        s = r.get("settings") or {}
        return float(s.get("fpts", 0)) + float(s.get("fpts_decimal", 0)) / 100.0

    standings_rows = sorted(
        [
            {
                "team_name": team_name_for_roster(r["roster_id"]),
                "manager": manager_for_roster(r["roster_id"]),
                "user_id": (r.get("owner_id") or ""),
                "wins": int((r.get("settings") or {}).get("wins", 0)),
                "losses": int((r.get("settings") or {}).get("losses", 0)),
                "points_for": _roster_pf(r),
            }
            for r in rosters
        ],
        key=lambda d: (d["wins"], d["points_for"]),
        reverse=True,
    )
    for idx, row in enumerate(standings_rows, start=1):
        row["rank"] = idx

    # Standings tuple shape for the email template (mirrors weekly_recap)
    standings_for_email: list[tuple[str, int, int, int, float]] = [
        (r["team_name"], r["wins"], r["losses"], 0, r["points_for"])
        for r in standings_rows
    ]

    # World Cup chain walk — best-effort. If anything errors, skip
    # the WC sections rather than fail the whole preview.
    wc_full_email: list[tuple[str, list[dict[str, Any]]]] | None = None
    wc_for_prompt: list[dict[str, Any]] = []
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
        wc_full_email = []
        for _div_id, div_name, teams in wc_standings:
            email_rows: list[dict[str, Any]] = []
            for position, t in enumerate(teams, start=1):
                email_rows.append({
                    "team_name": t.team_name,
                    "wins": t.wins,
                    "losses": t.losses,
                    "ties": t.ties,
                    "points_for": t.points_for,
                    "status": t.clinch_status,
                })
                wc_for_prompt.append({
                    "division": div_name,
                    "position": position,
                    "team_name": t.team_name,
                    "user_id": t.user_id,
                    "wins": t.wins,
                    "losses": t.losses,
                    "points_for": t.points_for,
                    "status": t.clinch_status,
                })
            wc_full_email.append((div_name, email_rows))
    except Exception as e:
        log.warning(f"week_preview: WC computation failed — {e}")

    prior_memories = ai_memories_store.list_recent_memories(
        league_id,
        nfl_season,
        limit=AI_REVIEW_WEEK_PREVIEW_MEMORY_LOOKBACK,
    )

    # --- prompts + generate --------------------------------------------------
    system_blocks = build_system_blocks()
    user_prompt = build_user_prompt(
        league_name=league_name,
        season=nfl_season,
        week=resolved_week,
        matchup_pairs=matchup_pairs,
        standings=standings_rows,
        wc_standings=wc_for_prompt,
        recent_memories=prior_memories,
    )

    raw_text, token_usage = claude_helper.generate(
        prompt=user_prompt,
        system=system_blocks,
        model=AI_REVIEW_DEFAULT_MODEL,
        max_tokens=AI_REVIEW_WEEK_PREVIEW_MAX_TOKENS,
        return_usage=True,
    )

    body_markdown, parse_ok = _parse_envelope(raw_text)

    # --- persist report ------------------------------------------------------
    metadata: dict[str, Any] = {
        "dry_run": dry_run,
        "force": force,
        "model": AI_REVIEW_DEFAULT_MODEL,
        "prompt_version": AI_REVIEW_WEEK_PREVIEW_PROMPT_VERSION,
        "token_usage": token_usage,
        "nfl_season": nfl_season,
        "nfl_season_type": season_type,
        "week": resolved_week,
        "envelope_parsed": parse_ok,
        "broadcast_at": None,
    }
    if created_by_user_id:
        metadata["created_by_user_id"] = created_by_user_id

    report_row = ai_reports_store.write_report(
        league_id=league_id,
        report_type=REPORT_TYPE,
        period=period,
        body_markdown=body_markdown,
        metadata=metadata,
    )

    # --- fan out emails ------------------------------------------------------
    whitelisted = get_active_whitelisted_users()
    if dry_run:
        whitelisted = filter_to_admin_only(whitelisted)
        log.info(
            f"week_preview: dry_run=True — restricting recipients to "
            f"admin only ({len(whitelisted)} user(s))"
        )

    email_tasks: list[tuple[str, str, str, str]] = []
    subject = f"Week {resolved_week} {league_name} preview"
    for wl in whitelisted:
        email = wl.get("email")
        if not email:
            continue
        manager_name = (
            wl.get("display_name")
            or wl.get("sleeper_username")
            or "Manager"
        )
        html = generate_week_preview_email(
            manager_name=manager_name,
            league_name=league_name,
            week=resolved_week,
            body_markdown=body_markdown,
            standings=standings_for_email,
            wc_divisions=wc_full_email,
        )
        text = generate_week_preview_email_plain_text(
            manager_name=manager_name,
            league_name=league_name,
            week=resolved_week,
            body_markdown=body_markdown,
            standings=standings_for_email,
            wc_divisions=wc_full_email,
        )
        email_tasks.append((email, subject, html, text))

    email_sent, email_failed = (0, 0)
    if email_tasks:
        email_sent, email_failed = send_emails_concurrently(email_tasks)

    log.info(
        f"week_preview: {email_sent} email sent, {email_failed} failed "
        f"for {period} (dry_run={dry_run})"
    )

    return {
        "status": "ok",
        "report_id": report_row.get("id") or f"LEAGUE#{league_id}|REPORT#{REPORT_TYPE}#{period}",
        "league_id": league_id,
        "league_name": league_name,
        "period": period,
        "week": resolved_week,
        "dry_run": dry_run,
        "email_sent": email_sent,
        "email_failed": email_failed,
        "envelope_parsed": parse_ok,
        "token_usage": token_usage,
    }


def _parse_envelope(raw: str) -> tuple[str, bool]:
    """Pull `body_markdown` out of Claude's JSON envelope. On parse
    failure return the raw response so we don't lose the recap."""
    if not raw:
        return "", False
    try:
        parsed = json.loads(raw)
        body = parsed.get("body_markdown")
        if isinstance(body, str) and body.strip():
            return body, True
    except (json.JSONDecodeError, AttributeError, TypeError) as e:
        log.warning(f"week_preview: JSON envelope parse failed - {e}")
    return raw, False
