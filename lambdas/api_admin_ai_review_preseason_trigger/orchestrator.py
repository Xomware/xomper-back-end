"""
Preseason AI Review orchestrator
================================
Pure orchestration logic for F2. Mirrors the F1 post-draft
orchestrator shape — handler stays tiny, this module is testable
without faking API Gateway events.

Flow (see `docs/features/ai-review/f2-preseason/PLAN.md`):

1. Resolve the active whitelisted league.
2. Idempotency: bail with `ReportAlreadyExistsError` if a row exists
   for `(league_id, "preseason", "2026-PRESEASON")` and `force=False`.
3. Pre-flight: confirm NFL state's `season_type` is `pre` or `off`.
   If the regular season has already started, raise
   `PreseasonWindowPassedError`.
4. Pull data — this year's rosters + users, last year's
   rosters + users (walked via `previous_league_id`).
5. Build the system + user prompts, call `claude_helper.generate`.
6. Persist via `ai_reports_store.write_report` with token-usage
   metadata.
7. Deliver via SES + SNS (single admin for dry-run, all 12 for
   broadcast). Stamp `metadata.broadcast_at` on the non-dry-run path.

Returns a dict suitable for JSON-encoding in the API response.
"""
from __future__ import annotations

from typing import Any

from lambdas.common import ai_reports_store, claude_helper
from lambdas.common.constants import (
    ADMIN_DOMINICK_USER_ID,
    AI_REVIEW_DEFAULT_MODEL,
    AI_REVIEW_PRESEASON_MAX_TOKENS,
    AI_REVIEW_PRESEASON_OK_SEASON_TYPES,
    AI_REVIEW_PRESEASON_PERIOD,
    AI_REVIEW_PRESEASON_PROMPT_VERSION,
)
from lambdas.common.email_templates.ai_review import (
    build_email_payload,
    render_preview_for_user,
)
from lambdas.common.errors import (
    DoNotBroadcastError,
    NotFoundError,
    PreseasonWindowPassedError,
    ReportAlreadyExistsError,
)
from lambdas.common.logger import get_logger
from lambdas.common.ses_helper import send_emails_concurrently
from lambdas.common.sleeper_helper import (
    get_nfl_state,
    get_previous_league_id,
    get_sleeper_league,
    get_sleeper_league_rosters,
    get_sleeper_league_users,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.supabase_helper import (
    get_active_whitelisted_league,
    get_active_whitelisted_users,
)

from lambdas.api_admin_ai_review_preseason_trigger.prompts import (
    build_system_blocks,
    build_user_prompt,
)

log = get_logger(__file__)

REPORT_TYPE = "preseason"
PUSH_TITLE_BROADCAST = "Your preseason AI review is in"
PUSH_BODY_BROADCAST = "Tap to see last year's grade + this year's outlook."
PUSH_TITLE_DRY_RUN = "[DRY RUN] Preseason AI review ready"
PUSH_BODY_DRY_RUN = "Tap to preview before broadcasting."
DEEP_LINK = f"xomper://ai-review/preseason/{AI_REVIEW_PRESEASON_PERIOD}"
PUSH_CATEGORY = "AI_REVIEW"


def run(
    *,
    dry_run: bool = True,
    force: bool = False,
    created_by_user_id: str | None = None,
) -> dict[str, Any]:
    """Drive the preseason AI Review generation + delivery.

    Args:
        dry_run: When True (default), the generated report is still
            written to Dynamo but only delivered to
            `ADMIN_DOMINICK_USER_ID` for tone calibration. When False,
            broadcasts to all 12 league managers.
        force: When False (default), an existing report for this
            league/type/period raises `ReportAlreadyExistsError`.
            When True, overwrites the existing row.
        created_by_user_id: Sleeper user_id of the admin who fired the
            trigger. Stamped into `metadata` for auditability.

    Returns:
        Dict carrying `report_id`, `dry_run`, `delivery_count`,
        `model`, `token_usage`, `status`, `report`. The handler
        wraps this for the API response.

    Raises:
        PreseasonWindowPassedError: NFL state shows the regular
            season has already started.
        ReportAlreadyExistsError: Existing row + force=False.
        NotFoundError: No active whitelisted league configured.
        ClaudeAPIError: Anthropic call failed after retries.
    """
    league_row = get_active_whitelisted_league()
    if not league_row:
        raise NotFoundError(
            message="No active whitelisted league configured",
            handler="notif_ai_review_preseason",
            function="run",
            resource="whitelisted_leagues",
        )
    league_id = (
        league_row.get("sleeper_league_id")
        or league_row.get("league_id")
        or ""
    )
    if not league_id:
        raise NotFoundError(
            message="Active league row is missing sleeper_league_id",
            handler="notif_ai_review_preseason",
            function="run",
            resource="whitelisted_leagues",
        )

    period = AI_REVIEW_PRESEASON_PERIOD

    # 1. Idempotency
    existing = ai_reports_store.get_latest(league_id, REPORT_TYPE)
    if existing and not force:
        raise ReportAlreadyExistsError(
            message="A preseason report already exists for this league",
            handler="notif_ai_review_preseason",
            existing={
                "league_id": existing.get("league_id"),
                "report_type": existing.get("report_type"),
                "period": existing.get("period"),
                "created_at": existing.get("created_at"),
            },
        )

    # 2. Pre-flight: confirm the regular season has not yet started.
    nfl_state = get_nfl_state() or {}
    season_type = (nfl_state.get("season_type") or "").lower()
    nfl_season = str(nfl_state.get("season") or "")
    if season_type and season_type not in AI_REVIEW_PRESEASON_OK_SEASON_TYPES:
        raise PreseasonWindowPassedError(
            message=(
                f"Regular season already underway "
                f"(NFL season={nfl_season!r}, season_type={season_type!r})"
            ),
            nfl_season=nfl_season,
            season_type=season_type,
        )

    # 3. Load data
    league = get_sleeper_league(league_id)
    sleeper_users = get_sleeper_league_users(league_id)
    rosters = get_sleeper_league_rosters(league_id)

    prior_league_id = get_previous_league_id(league_id)
    prior_standings: list[dict[str, Any]] = []
    if prior_league_id:
        try:
            prior_rosters = get_sleeper_league_rosters(prior_league_id)
            prior_users = get_sleeper_league_users(prior_league_id)
            prior_standings = _build_prior_standings(prior_rosters, prior_users)
        except Exception as err:  # noqa: BLE001 — prior season is best-effort
            log.warning(
                f"prior-standings fetch failed for league {prior_league_id}: "
                f"{err}; continuing without prior standings"
            )

    team_rosters = _build_current_rosters(
        rosters=rosters,
        sleeper_users=sleeper_users,
        prior_standings=prior_standings,
    )

    league_name = league.get("name") or league_row.get("league_name") or "League"
    season = str(league.get("season") or nfl_season or "2026")

    # 4. Build prompts + generate
    system_blocks = build_system_blocks()
    user_prompt = build_user_prompt(
        league_name=league_name,
        season=season,
        team_rosters=team_rosters,
        prior_standings=prior_standings,
    )

    markdown, token_usage = claude_helper.generate(
        prompt=user_prompt,
        system=system_blocks,
        model=AI_REVIEW_DEFAULT_MODEL,
        max_tokens=AI_REVIEW_PRESEASON_MAX_TOKENS,
        return_usage=True,
    )

    # 5. Persist
    metadata: dict[str, Any] = {
        "dry_run": dry_run,
        "force": force,
        "model": AI_REVIEW_DEFAULT_MODEL,
        "prompt_version": AI_REVIEW_PRESEASON_PROMPT_VERSION,
        "token_usage": token_usage,
        "nfl_season": nfl_season,
        "nfl_season_type": season_type,
        "prior_league_id": prior_league_id,
        "broadcast_at": None,
    }
    if created_by_user_id:
        metadata["created_by_user_id"] = created_by_user_id

    report_row = ai_reports_store.write_report(
        league_id=league_id,
        report_type=REPORT_TYPE,
        period=period,
        body_markdown=markdown,
        metadata=metadata,
    )

    # 6. Pre-broadcast DNB check (Admin Portal F3).
    # Re-read the row right before SES fan-out so admins who flipped
    # `do_not_broadcast=true` AFTER generation but BEFORE broadcast
    # still get the abort. Dry-run path skips — locking only matters
    # for real-broadcast attempts.
    if not dry_run:
        _enforce_not_dnb(league_id=league_id, period=period)

    # 7. Deliver
    delivery_count = _deliver(
        dry_run=dry_run,
        league_id=league_id,
        league_name=league_name,
        body_markdown=markdown,
        period=period,
    )

    # 7b. Render previews for the Admin Portal F2 pre-broadcast surface.
    previews = _build_previews(
        dry_run=dry_run,
        body_markdown=markdown,
        period=period,
        league_name=league_name,
    )
    if previews is not None:
        print(
            f"[preseason-trigger] preview generated for {len(previews)} "
            f"users (dry_run=true)"
        )

    # 8. Stamp broadcast_at on the non-dry-run path AFTER SES success.
    if not dry_run:
        try:
            updated = ai_reports_store.stamp_broadcast_at(
                league_id=league_id,
                report_type=REPORT_TYPE,
                period=period,
            )
            if isinstance(updated, dict) and updated.get("metadata"):
                metadata.update(updated["metadata"])
                report_row["metadata"] = metadata
        except Exception as err:  # noqa: BLE001 — non-blocking
            log.warning(f"stamp_broadcast_at failed: {err}")

    status = "dry_run_sent" if dry_run else "broadcast"
    return {
        "status": status,
        "report_id": report_row.get("sk"),
        "report": report_row,
        "dry_run": dry_run,
        "delivery_count": delivery_count,
        "model": AI_REVIEW_DEFAULT_MODEL,
        "token_usage": token_usage,
        # Admin Portal F2 — populated on dry-run only.
        "previews": previews,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _enforce_not_dnb(*, league_id: str, period: str) -> None:
    """Re-read the just-written report row and raise
    `DoNotBroadcastError` (HTTP 409) when `metadata.do_not_broadcast`
    is truthy. Called immediately before SES fan-out on the real-
    broadcast path only — dry-run delivery is unaffected.

    Cheap: one Dynamo `get_item`. Catches the race where an admin
    flips DNB on between `write_report` and the broadcast attempt.
    """
    fresh = ai_reports_store.get_report(
        league_id=league_id,
        report_type=REPORT_TYPE,
        period=period,
    )
    if not fresh:
        return
    meta = fresh.get("metadata") or {}
    flag = meta.get("do_not_broadcast")
    if flag is True or (isinstance(flag, str) and flag.lower() == "true"):
        raise DoNotBroadcastError(
            handler="notif_ai_review_preseason",
            report_type=REPORT_TYPE,
            period=period,
        )


def _build_prior_standings(
    rosters: list[dict[str, Any]],
    users: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Convert Sleeper rosters + users into a sorted standings list.

    Sleeper rosters carry `settings.wins`, `losses`, `ties`, `fpts`,
    `fpts_decimal`, `fpts_against`, `fpts_against_decimal`. Sort by
    wins desc, then points-for desc.
    """
    users_by_id = {u.get("user_id"): u for u in users}
    rows: list[dict[str, Any]] = []
    for roster in rosters:
        settings = roster.get("settings") or {}
        owner_id = roster.get("owner_id")
        user = users_by_id.get(owner_id, {}) if owner_id else {}
        team_name = (user.get("metadata") or {}).get("team_name")
        display_name = user.get("display_name") or "Unknown"

        wins = int(settings.get("wins") or 0)
        losses = int(settings.get("losses") or 0)
        ties = int(settings.get("ties") or 0)
        pf_whole = settings.get("fpts") or 0
        pf_dec = settings.get("fpts_decimal") or 0
        pa_whole = settings.get("fpts_against") or 0
        pa_dec = settings.get("fpts_against_decimal") or 0
        points_for = float(pf_whole) + (float(pf_dec) / 100.0)
        points_against = float(pa_whole) + (float(pa_dec) / 100.0)

        rows.append(
            {
                "user_id": owner_id,
                "manager_display_name": display_name,
                "team_name": team_name,
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "points_for": points_for,
                "points_against": points_against,
            }
        )

    rows.sort(key=lambda r: (-r["wins"], -r["points_for"]))
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
        if idx == 1:
            row["playoff_note"] = "won the chip"
        elif idx <= 4:
            row["playoff_note"] = "made the playoffs"
        elif idx == len(rows):
            row["playoff_note"] = "finished last"
        else:
            row["playoff_note"] = ""
    return rows


def _build_current_rosters(
    *,
    rosters: list[dict[str, Any]],
    sleeper_users: list[dict[str, Any]],
    prior_standings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Group player_ids by manager from this year's rosters.

    Returns one dict per manager, sorted by `roster_id` asc (stable
    proxy for draft-slot ordering on the backend, which doesn't have
    access to the iOS `historyStore.upcomingDraft`). Each manager dict
    carries:
      - `roster_id` (int)
      - `user_id` (str | None)
      - `manager_display_name` (str)
      - `team_name` (str | None)
      - `player_ids` (list[str])  — keeper / starter pool from the
        current Sleeper roster snapshot
      - `prior_finish` (dict | None)  — `{rank, wins, losses, ties,
        points_for}` lifted from the matched prior-standings row, or
        None when there's no prior season
      - `position_counts` (dict[str, int]) — best-effort breakdown
        keyed off Sleeper's `roster_positions` slots (empty when
        positional data isn't available)
    """
    users_by_id = {u.get("user_id"): u for u in sleeper_users}
    prior_by_user = {
        row.get("user_id"): row for row in prior_standings if row.get("user_id")
    }

    summaries: list[dict[str, Any]] = []
    for roster in rosters:
        owner_id = roster.get("owner_id")
        user = users_by_id.get(owner_id, {}) if owner_id else {}
        team_name = (user.get("metadata") or {}).get("team_name")
        display_name = user.get("display_name") or "Unknown"

        players = roster.get("players") or []
        # Sleeper roster `players` is a list of player_id strings.
        player_ids = [str(pid) for pid in players if pid]

        starters_raw = roster.get("starters") or []
        starters = [str(pid) for pid in starters_raw if pid and pid != "0"]

        prior_row = prior_by_user.get(owner_id)
        prior_finish: dict[str, Any] | None = None
        if prior_row:
            prior_finish = {
                "rank": prior_row.get("rank"),
                "wins": prior_row.get("wins"),
                "losses": prior_row.get("losses"),
                "ties": prior_row.get("ties"),
                "points_for": prior_row.get("points_for"),
            }

        summaries.append(
            {
                "roster_id": roster.get("roster_id"),
                "user_id": owner_id,
                "manager_display_name": display_name,
                "team_name": team_name,
                "player_ids": player_ids,
                "starters": starters,
                "player_count": len(player_ids),
                "prior_finish": prior_finish,
            }
        )

    summaries.sort(key=lambda r: (r.get("roster_id") or 999))
    return summaries


def _deliver(
    *,
    dry_run: bool,
    league_id: str,
    league_name: str,
    body_markdown: str,
    period: str,
) -> int:
    """Build per-user email payloads + push notifications and fan
    them out. Returns the number of recipients we attempted to
    deliver to (not the number of successful sends — per-channel
    success is logged via the existing notification_log path)."""
    recipients = _resolve_recipients(dry_run=dry_run)
    if not recipients:
        log.warning(
            f"No recipients resolved for AI Review delivery "
            f"(dry_run={dry_run}, league={league_id})"
        )
        return 0

    period_label = f"Preseason {period}"
    email_tasks: list[tuple[str, str, str, str]] = []
    push_user_ids: list[str] = []

    for recipient in recipients:
        sleeper_user_id = recipient.get("sleeper_user_id")
        email = recipient.get("email")
        first_name = (
            recipient.get("display_name")
            or recipient.get("sleeper_username")
            or "there"
        )
        if email:
            payload = build_email_payload(
                user_email=email,
                user_first_name=first_name,
                report_type=REPORT_TYPE,
                body_markdown=body_markdown,
                period_label=period_label,
                league_name=league_name,
            )
            subject = payload["subject"]
            if dry_run:
                subject = f"[DRY RUN] {subject}"
            email_tasks.append(
                (
                    email,
                    subject,
                    payload["html_body"],
                    payload["text_body"],
                )
            )
        if sleeper_user_id:
            push_user_ids.append(sleeper_user_id)

    if email_tasks:
        send_emails_concurrently(email_tasks)

    if push_user_ids:
        title = PUSH_TITLE_DRY_RUN if dry_run else PUSH_TITLE_BROADCAST
        body = PUSH_BODY_DRY_RUN if dry_run else PUSH_BODY_BROADCAST
        send_push_to_users(
            push_user_ids,
            title,
            body,
            PUSH_CATEGORY,
            {"url": DEEP_LINK, "period": period},
        )

    return len(recipients)


def _resolve_recipients(*, dry_run: bool) -> list[dict[str, Any]]:
    """For dry_run, return the single admin-row (resolved from the
    whitelisted_users table). For broadcast, return all active users."""
    all_users = get_active_whitelisted_users()
    if dry_run:
        admin = next(
            (
                u
                for u in all_users
                if u.get("sleeper_user_id") == ADMIN_DOMINICK_USER_ID
            ),
            None,
        )
        return [admin] if admin else []
    return all_users


def _build_previews(
    *,
    dry_run: bool,
    body_markdown: str,
    period: str,
    league_name: str,
) -> list[dict[str, Any]] | None:
    """Render an email preview row per active whitelisted user.

    Returns:
        On dry-run: list of preview dicts sorted alphabetically by
        `display_name` (one per active user — typically 12).
        On real broadcast: `None`.
    """
    if not dry_run:
        return None

    users = get_active_whitelisted_users() or []
    sorted_users = sorted(
        users,
        key=lambda u: (u.get("display_name") or "").lower(),
    )

    period_label = f"Preseason {period}"
    return [
        render_preview_for_user(
            user=user,
            report_type=REPORT_TYPE,
            body_markdown=body_markdown,
            period_label=period_label,
            league_name=league_name,
        )
        for user in sorted_users
    ]
