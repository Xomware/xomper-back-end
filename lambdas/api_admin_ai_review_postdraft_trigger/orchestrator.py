"""
Post-Draft AI Review orchestrator
=================================
Pure orchestration logic for F1 — kept out of `handler.py` so the
admin-gate / API GW response shaping stays tiny and this module is
testable without faking API Gateway events.

Flow (see `docs/features/ai-review/f1-post-draft/PLAN.md`):

1. Resolve the active whitelisted league.
2. Idempotency: bail with `ReportAlreadyExistsError` if a row exists
   for `(league_id, "postDraft", "2026")` and `force=False`.
3. Pre-flight: confirm the Sleeper draft is `status == "complete"`.
4. Pull data — draft picks, league users, rosters, prior-season
   standings (walked via `previous_league_id`).
5. Build the system + user prompts, call `claude_helper.generate`.
6. Persist via `ai_reports_store.write_report` (with token-usage
   metadata).
7. Deliver via SES + SNS (single user for dry-run, all 12 for
   broadcast). Stamp `metadata.broadcast_at` on the non-dry-run path.

Returns a dict suitable for JSON-encoding in the API response.
"""
from __future__ import annotations

from typing import Any

from lambdas.common import ai_reports_store, claude_helper
from lambdas.common.constants import (
    ADMIN_DOMINICK_USER_ID,
    AI_REVIEW_DEFAULT_MODEL,
    AI_REVIEW_MAX_TOKENS,
    AI_REVIEW_POSTDRAFT_PERIOD,
    AI_REVIEW_PROMPT_VERSION,
)
from lambdas.common.email_templates.ai_review import (
    build_email_payload,
    render_preview_for_user,
)
from lambdas.common.errors import (
    DoNotBroadcastError,
    DraftNotCompleteError,
    NotFoundError,
    ReportAlreadyExistsError,
)
from lambdas.common.logger import get_logger
from lambdas.common.season_resolver import resolve_league_by_year
from lambdas.common.ses_helper import send_emails_concurrently
from lambdas.common.sleeper_helper import (
    get_previous_league_id,
    get_sleeper_draft,
    get_sleeper_draft_picks,
    get_sleeper_league,
    get_sleeper_league_rosters,
    get_sleeper_league_users,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.supabase_helper import (
    get_active_whitelisted_league,
    get_active_whitelisted_users,
)

from lambdas.api_admin_ai_review_postdraft_trigger.prompts import (
    build_system_blocks,
    build_user_prompt,
)

log = get_logger(__file__)

REPORT_TYPE = "postDraft"
PUSH_TITLE_BROADCAST = "Your post-draft AI review is in"
PUSH_BODY_BROADCAST = "Tap to see how your draft graded out."
PUSH_TITLE_DRY_RUN = "[DRY RUN] Post-draft AI review ready"
PUSH_BODY_DRY_RUN = "Tap to preview before broadcasting."
PUSH_CATEGORY = "AI_REVIEW"


def run(
    *,
    dry_run: bool = True,
    force: bool = False,
    created_by_user_id: str | None = None,
    target_year: int | None = None,
) -> dict[str, Any]:
    """Drive the post-draft AI Review generation + delivery.

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
        target_year: When None (default), uses the active league's
            current draft and writes period=`AI_REVIEW_POSTDRAFT_PERIOD`.
            When an int (e.g. 2024), walks `previous_league_id` from
            the active league until a league with `season ==
            str(target_year)` is found, then runs the entire pipeline
            against THAT league's draft + rosters + users + prior
            standings. Period becomes `str(target_year)`. Used for
            historical backfill.

    Returns:
        Dict carrying `report_id`, `dry_run`, `delivery_count`,
        `model`, `token_usage`, `status`, `existing`, `target_year`,
        `period`. The handler wraps this for the API response.

    Raises:
        DraftNotCompleteError: Sleeper draft status != "complete".
        ReportAlreadyExistsError: Existing row + force=False.
        NotFoundError: No active whitelisted league configured, or
            `target_year` not reachable via previous_league_id.
        ClaudeAPIError: Anthropic call failed after retries.
    """
    league_row = get_active_whitelisted_league()
    if not league_row:
        raise NotFoundError(
            message="No active whitelisted league configured",
            handler="notif_ai_review_postdraft",
            function="run",
            resource="whitelisted_leagues",
        )
    active_league_id = (
        league_row.get("sleeper_league_id")
        or league_row.get("league_id")
        or ""
    )
    if not active_league_id:
        raise NotFoundError(
            message="Active league row is missing sleeper_league_id",
            handler="notif_ai_review_postdraft",
            function="run",
            resource="whitelisted_leagues",
        )

    # Resolve the league to operate against. For target_year backfill
    # we walk the previous_league_id chain to the matching season; the
    # report is still keyed under the ACTIVE league_id (one report row
    # per (league_id, type, period)) but every Sleeper fetch is
    # relative to the resolved historical league.
    if target_year is not None:
        league = resolve_league_by_year(
            active_league_id,
            target_year,
            league_fetcher=get_sleeper_league,
        )
        league_id = active_league_id
        operating_league_id = (
            league.get("league_id") or str(league.get("league_id") or "")
        )
        if not operating_league_id:
            raise NotFoundError(
                message=(
                    f"Resolved historical league for target_year="
                    f"{target_year} is missing a league_id"
                ),
                handler="notif_ai_review_postdraft",
                function="run",
                resource="previous_league_id",
            )
        period = str(target_year)
        log.info(
            f"notif_ai_review_postdraft: backfill target_year="
            f"{target_year} -> historical league_id="
            f"{operating_league_id}"
        )
    else:
        league = get_sleeper_league(active_league_id)
        league_id = active_league_id
        operating_league_id = active_league_id
        period = AI_REVIEW_POSTDRAFT_PERIOD

    # 1. Idempotency — keyed by (active league_id, "postDraft", period)
    existing = ai_reports_store.get_latest(league_id, REPORT_TYPE)
    if existing and not force and existing.get("period") == period:
        raise ReportAlreadyExistsError(
            message=(
                f"A post-draft report already exists for this league "
                f"and period {period}"
            ),
            existing={
                "league_id": existing.get("league_id"),
                "report_type": existing.get("report_type"),
                "period": existing.get("period"),
                "created_at": existing.get("created_at"),
            },
        )

    # 2. Pre-flight: confirm draft is complete
    draft_id = league.get("draft_id")
    if not draft_id:
        raise DraftNotCompleteError(
            message=(
                f"League {operating_league_id} has no draft_id — "
                "Sleeper draft not provisioned yet"
            ),
            draft_status=None,
        )

    draft = get_sleeper_draft(draft_id)
    draft_status = draft.get("status")
    if draft_status != "complete":
        raise DraftNotCompleteError(
            message=f"Sleeper draft is not complete (status={draft_status!r})",
            draft_status=draft_status,
        )

    # 3. Load data — all relative to the operating (possibly
    # historical) league.
    picks = get_sleeper_draft_picks(draft_id)
    sleeper_users = get_sleeper_league_users(operating_league_id)
    rosters = get_sleeper_league_rosters(operating_league_id)

    prior_league_id = get_previous_league_id(operating_league_id)
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

    team_summaries = _build_team_summaries(
        picks=picks,
        rosters=rosters,
        sleeper_users=sleeper_users,
    )
    league_name = league.get("name") or league_row.get("league_name") or "League"
    season = str(league.get("season") or period)

    # 4. Build prompts + generate
    system_blocks = build_system_blocks()
    user_prompt = build_user_prompt(
        league_name=league_name,
        season=season,
        team_summaries=team_summaries,
        prior_standings=prior_standings,
    )

    markdown, token_usage = claude_helper.generate(
        prompt=user_prompt,
        system=system_blocks,
        model=AI_REVIEW_DEFAULT_MODEL,
        max_tokens=AI_REVIEW_MAX_TOKENS,
        return_usage=True,
    )

    # 5. Persist
    metadata: dict[str, Any] = {
        "dry_run": dry_run,
        "force": force,
        "model": AI_REVIEW_DEFAULT_MODEL,
        "prompt_version": AI_REVIEW_PROMPT_VERSION,
        "token_usage": token_usage,
        "draft_id": str(draft_id),
        "broadcast_at": None,
        "target_year": target_year,
        "operating_league_id": operating_league_id,
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
    # Only fired on the dry-run path; real broadcasts return `previews=None`
    # so we don't burn template-render cycles or pad the response body.
    previews = _build_previews(
        dry_run=dry_run,
        body_markdown=markdown,
        period=period,
        league_name=league_name,
    )
    if previews is not None:
        print(
            f"[postdraft-trigger] preview generated for {len(previews)} "
            f"users (dry_run=true)"
        )

    # 8. Stamp broadcast_at on the non-dry-run path AFTER SES success.
    # Partial-failure safe: helper only runs if `_deliver` returned
    # without raising.
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
        "period": period,
        "target_year": target_year,
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
    # Persists as "true"/"false" string (admin-portal F3 flag endpoint
    # writes through `update_metadata` with stringified bools) but
    # accept native bool too for forward-compat.
    if flag is True or (isinstance(flag, str) and flag.lower() == "true"):
        raise DoNotBroadcastError(
            handler="notif_ai_review_postdraft",
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


def _build_team_summaries(
    *,
    picks: list[dict[str, Any]],
    rosters: list[dict[str, Any]],
    sleeper_users: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Group draft picks by manager, in draft-slot order (round-1 slot
    1 -> 12). Returns one summary dict per manager.

    Slot is derived from the manager's round-1 `pick_no` so the
    output mirrors the visual draft board. Managers with no round-1
    pick (e.g. traded away) fall back to their lowest round/pick_no.
    """
    users_by_id = {u.get("user_id"): u for u in sleeper_users}
    roster_by_id = {r.get("roster_id"): r for r in rosters}

    # Build the canonical manager set from the rosters (12 entries).
    managers: dict[str, dict[str, Any]] = {}
    for roster in rosters:
        owner_id = roster.get("owner_id")
        if not owner_id:
            continue
        user = users_by_id.get(owner_id, {})
        team_name = (user.get("metadata") or {}).get("team_name")
        managers[owner_id] = {
            "user_id": owner_id,
            "manager_display_name": user.get("display_name") or "Unknown",
            "team_name": team_name,
            "roster_id": roster.get("roster_id"),
            "picks": [],
            "slot": None,
        }

    # Walk picks; attach each to its owning manager. Prefer `picked_by`
    # (the user who actually drafted the player) when present, falling
    # back to `roster_id -> owner` (Sleeper's draft endpoint quirk).
    for pick in picks:
        owner_id = pick.get("picked_by") or None
        if not owner_id:
            roster = roster_by_id.get(pick.get("roster_id")) or {}
            owner_id = roster.get("owner_id")
        if not owner_id or owner_id not in managers:
            continue
        meta = pick.get("metadata") or {}
        player_first = meta.get("first_name") or ""
        player_last = meta.get("last_name") or ""
        player_name = f"{player_first} {player_last}".strip() or "Unknown"
        managers[owner_id]["picks"].append(
            {
                "round": pick.get("round"),
                "pick_no": pick.get("pick_no"),
                "slot": pick.get("draft_slot"),
                "player_name": player_name,
                "position": meta.get("position") or "?",
                "nfl_team": meta.get("team") or "FA",
            }
        )

    # Resolve each manager's draft slot from the round-1 pick when
    # possible. `draft_slot` on the round-1 pick is the canonical
    # column on Sleeper's board.
    for manager in managers.values():
        round_one = [p for p in manager["picks"] if p.get("round") == 1]
        if round_one:
            primary = min(round_one, key=lambda p: p.get("pick_no") or 99)
            manager["slot"] = primary.get("slot") or primary.get("pick_no")
        elif manager["picks"]:
            primary = min(
                manager["picks"], key=lambda p: p.get("pick_no") or 999
            )
            manager["slot"] = primary.get("slot") or primary.get("pick_no")
        # Sort each manager's picks by round then pick_no for readable
        # output.
        manager["picks"].sort(
            key=lambda p: (p.get("round") or 99, p.get("pick_no") or 999)
        )

    summaries = list(managers.values())
    summaries.sort(
        key=lambda m: (m["slot"] if m["slot"] is not None else 999)
    )
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

    period_label = f"Post-Draft {period}"
    deep_link = f"xomper://ai-review/post-draft/{period}"
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
            {"url": deep_link, "period": period},
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
        On real broadcast: `None`. The iOS admin surface only shows
        previews under the dry-run banner; skipping the render here
        keeps the broadcast response payload small.
    """
    if not dry_run:
        return None

    users = get_active_whitelisted_users() or []
    sorted_users = sorted(
        users,
        key=lambda u: (u.get("display_name") or "").lower(),
    )

    period_label = f"Post-Draft {period}"
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
