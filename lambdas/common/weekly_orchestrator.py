"""
Weekly AI Review orchestrator (F3)
==================================
Pure orchestration logic for F3. Lives in `lambdas/common/` because
both `notif_ai_review_weekly` (EventBridge cron) and
`api_admin_ai_review_weekly_trigger` (admin POST) call it — and the
deploy script zips each lambda dir in isolation. The only shared
runtime surface across lambdas is the common layer.

Flow (see `docs/features/ai-review/f3-weekly/PLAN.md`):

1. Resolve the active whitelisted league.
2. Resolve the week to recap (caller override OR `nfl_state.week - 1`,
   clamped to >= 1).
3. Pre-flight: confirm `season_type` is `regular` or `post` (unless
   `use_previous_season=True` for dry-run calibration).
4. Idempotency: bail with `ReportAlreadyExistsError` if a row exists
   for `(league_id, "weekly", "<season>W<NN>")` and `force=False`.
5. Pull data — matchups for this week, rosters + users, last N
   memories. For `use_previous_season=True`, swap matchups + rosters
   + users to the prior league.
6. Build the system + user prompts, call `claude_helper.generate`.
7. Parse Claude's JSON envelope. On parse failure, persist the raw
   response as `body_markdown` and skip memory append (so the recap
   isn't lost).
8. Persist via `ai_reports_store.write_report` with token-usage
   metadata + memory counts.
9. Write each new memory via `ai_memories_store.write_memory`.
10. Deliver via SES + SNS (single admin for dry-run, all 12 for
    broadcast). Stamp `metadata.broadcast_at` on the non-dry-run path.

Returns a dict suitable for JSON-encoding in the API response.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from lambdas.common import (
    ai_memories_store,
    ai_reports_store,
    claude_helper,
)
from lambdas.common.constants import (
    ADMIN_DOMINICK_USER_ID,
    AI_REVIEW_DEFAULT_MODEL,
    AI_REVIEW_WEEKLY_MAX_NEW_MEMORIES,
    AI_REVIEW_WEEKLY_MAX_TOKENS,
    AI_REVIEW_WEEKLY_MEMORY_LOOKBACK,
    AI_REVIEW_WEEKLY_OK_SEASON_TYPES,
    AI_REVIEW_WEEKLY_PROMPT_VERSION,
)
from lambdas.common.email_templates.ai_review import build_email_payload
from lambdas.common.errors import (
    NotFoundError,
    ReportAlreadyExistsError,
    ValidationError,
)
from lambdas.common.logger import get_logger
from lambdas.common.ses_helper import send_emails_concurrently
from lambdas.common.sleeper_helper import (
    get_nfl_state,
    get_previous_league_id,
    get_sleeper_league,
    get_sleeper_league_matchups,
    get_sleeper_league_rosters,
    get_sleeper_league_users,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.supabase_helper import (
    get_active_whitelisted_league,
    get_active_whitelisted_users,
)
from lambdas.common.weekly_prompts import (
    build_system_blocks,
    build_user_prompt,
)

log = get_logger(__file__)

REPORT_TYPE = "weekly"
PUSH_TITLE_BROADCAST = "This week's AI recap is in"
PUSH_BODY_BROADCAST = "Tap to see who got roasted this week."
PUSH_TITLE_DRY_RUN = "[DRY RUN] Weekly AI recap ready"
PUSH_BODY_DRY_RUN = "Tap to preview before broadcasting."
PUSH_CATEGORY = "AI_REVIEW"

# Sentinel sentiments accepted on incoming memories from Claude. Any
# other value gets dropped with a warning at write-time.
_VALID_SENTIMENTS: tuple[str, ...] = ("roast", "praise", "lore")
_MEMORY_TEXT_MAX = 200


def run_weekly(
    *,
    week: int | None = None,
    dry_run: bool = True,
    force: bool = False,
    use_previous_season: bool = False,
    created_by_user_id: str | None = None,
) -> dict[str, Any]:
    """Drive the weekly AI Review generation + delivery.

    Args:
        week: NFL week to recap (1-22). When None, resolves from
            `nfl_state.week - 1` (the just-completed week).
        dry_run: When True (default), the report is still written
            to Dynamo but only delivered to `ADMIN_DOMINICK_USER_ID`
            for tone calibration. When False, broadcasts to all 12.
        force: When False (default), an existing report for this
            (league, "weekly", period) raises
            `ReportAlreadyExistsError`. When True, overwrites.
        use_previous_season: For dry-run calibration against last
            year's matchup data. Swaps Sleeper fetches to the prior
            league via `previous_league_id`. The cron NEVER sets
            this; only the admin trigger does.
        created_by_user_id: Sleeper user_id of the caller (admin
            trigger) or None (cron). Stamped into metadata for
            auditability.

    Returns:
        Dict carrying `status`, `report`, `dry_run`, `delivery_count`,
        `model`, `token_usage`, `week`, `period`, `memory_count_in`,
        `memory_count_out`. Both handlers wrap this for their
        responses.

    Raises:
        ReportAlreadyExistsError: Existing row + force=False.
        NotFoundError: No active whitelisted league configured.
        ValidationError: Bad week input.
        ClaudeAPIError: Anthropic call failed after retries.
    """
    league_row = get_active_whitelisted_league()
    if not league_row:
        raise NotFoundError(
            message="No active whitelisted league configured",
            handler="notif_ai_review_weekly",
            function="run_weekly",
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
            handler="notif_ai_review_weekly",
            function="run_weekly",
            resource="whitelisted_leagues",
        )

    nfl_state = get_nfl_state() or {}
    season_type = (nfl_state.get("season_type") or "").lower()
    nfl_season = str(nfl_state.get("season") or "")
    nfl_week_raw = nfl_state.get("week")

    # --- resolve which week to recap -----------------------------------------
    resolved_week = _resolve_week(week=week, nfl_week=nfl_week_raw)

    # --- pre-flight ----------------------------------------------------------
    # For previous-season dry-runs we deliberately bypass the
    # season-type gate (calibrating against finished 2025 data).
    if not use_previous_season:
        if season_type and season_type not in AI_REVIEW_WEEKLY_OK_SEASON_TYPES:
            log.info(
                f"notif_ai_review_weekly: season_type={season_type!r} "
                f"is not in {AI_REVIEW_WEEKLY_OK_SEASON_TYPES} — "
                f"skipping (off-season / preseason no-op)"
            )
            return {
                "status": "skipped_offseason",
                "report": None,
                "dry_run": dry_run,
                "delivery_count": 0,
                "model": AI_REVIEW_DEFAULT_MODEL,
                "token_usage": None,
                "week": resolved_week,
                "period": _period(resolved_week, nfl_season),
                "memory_count_in": 0,
                "memory_count_out": 0,
                "skipped": True,
                "reason": f"season_type={season_type!r}",
            }

    # --- data fetch source league --------------------------------------------
    matchup_league_id = league_id
    if use_previous_season:
        prior_id = get_previous_league_id(league_id)
        if not prior_id:
            raise NotFoundError(
                message=(
                    "use_previous_season=True but the active league "
                    "has no previous_league_id linked"
                ),
                handler="notif_ai_review_weekly",
                function="run_weekly",
                resource="previous_league_id",
            )
        matchup_league_id = prior_id
        log.info(
            f"notif_ai_review_weekly: dry-run calibration vs "
            f"previous league {prior_id} (week {resolved_week})"
        )

    league = get_sleeper_league(matchup_league_id)
    league_name = (
        league.get("name")
        or league_row.get("league_name")
        or "League"
    )
    season = str(
        league.get("season") or nfl_season or "2026"
    )
    period = _period(resolved_week, season)

    # --- idempotency ---------------------------------------------------------
    existing = ai_reports_store.get_latest(league_id, REPORT_TYPE)
    if existing and not force:
        if existing.get("period") == period:
            raise ReportAlreadyExistsError(
                message=(
                    f"A weekly report already exists for period {period}"
                ),
                handler="notif_ai_review_weekly",
                existing={
                    "league_id": existing.get("league_id"),
                    "report_type": existing.get("report_type"),
                    "period": existing.get("period"),
                    "created_at": existing.get("created_at"),
                },
            )

    # --- data load -----------------------------------------------------------
    matchups_raw = get_sleeper_league_matchups(
        matchup_league_id, resolved_week
    ) or []
    sleeper_users = get_sleeper_league_users(matchup_league_id) or []
    rosters = get_sleeper_league_rosters(matchup_league_id) or []

    matchups = _build_matchups(
        matchups_raw=matchups_raw,
        rosters=rosters,
        sleeper_users=sleeper_users,
    )

    prior_memories = ai_memories_store.list_recent_memories(
        league_id, season, limit=AI_REVIEW_WEEKLY_MEMORY_LOOKBACK
    )

    # --- prompts + generate --------------------------------------------------
    system_blocks = build_system_blocks()
    user_prompt = build_user_prompt(
        league_name=league_name,
        season=season,
        week=resolved_week,
        matchups=matchups,
        prior_memories=prior_memories,
    )

    raw_text, token_usage = claude_helper.generate(
        prompt=user_prompt,
        system=system_blocks,
        model=AI_REVIEW_DEFAULT_MODEL,
        max_tokens=AI_REVIEW_WEEKLY_MAX_TOKENS,
        return_usage=True,
    )

    # --- parse JSON envelope -------------------------------------------------
    body_markdown, parsed_memories, parse_ok = _parse_envelope(raw_text)

    # --- persist report ------------------------------------------------------
    metadata: dict[str, Any] = {
        "dry_run": dry_run,
        "force": force,
        "use_previous_season": use_previous_season,
        "model": AI_REVIEW_DEFAULT_MODEL,
        "prompt_version": AI_REVIEW_WEEKLY_PROMPT_VERSION,
        "token_usage": token_usage,
        "nfl_season": nfl_season,
        "nfl_season_type": season_type,
        "matchup_league_id": matchup_league_id,
        "week": resolved_week,
        "memory_count_in": len(prior_memories),
        "memory_count_out": 0,  # patched below post-write
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

    # --- write memories ------------------------------------------------------
    written_memories = 0
    if parse_ok and parsed_memories:
        # Capped at AI_REVIEW_WEEKLY_MAX_NEW_MEMORIES at parse time.
        for mem in parsed_memories:
            try:
                ai_memories_store.write_memory(
                    league_id=league_id,
                    season=season,
                    week=resolved_week,
                    text=mem["text"],
                    sentiment=mem["sentiment"],
                    manager_user_id=mem.get("manager_user_id"),
                )
                written_memories += 1
            except Exception as err:  # noqa: BLE001 — memories are best-effort
                log.warning(
                    f"notif_ai_review_weekly: write_memory failed "
                    f"(text={mem.get('text', '')[:60]!r}): {err}"
                )

        if written_memories:
            try:
                ai_reports_store.update_metadata(
                    league_id=league_id,
                    report_type=REPORT_TYPE,
                    period=period,
                    partial={"memory_count_out": written_memories},
                )
                metadata["memory_count_out"] = written_memories
                report_row["metadata"] = metadata
            except Exception as err:  # noqa: BLE001 — non-blocking
                log.warning(
                    f"notif_ai_review_weekly: update_metadata "
                    f"memory_count_out failed: {err}"
                )

    # --- deliver -------------------------------------------------------------
    delivery_count = _deliver(
        dry_run=dry_run,
        league_id=league_id,
        league_name=league_name,
        body_markdown=body_markdown,
        period=period,
        week=resolved_week,
    )

    # --- stamp broadcast_at on broadcast path --------------------------------
    if not dry_run:
        broadcast_at = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        try:
            ai_reports_store.update_metadata(
                league_id=league_id,
                report_type=REPORT_TYPE,
                period=period,
                partial={"broadcast_at": broadcast_at},
            )
            metadata["broadcast_at"] = broadcast_at
            report_row["metadata"] = metadata
        except Exception as err:  # noqa: BLE001 — non-blocking
            log.warning(
                f"notif_ai_review_weekly: update_metadata "
                f"broadcast_at failed: {err}"
            )

    status = "dry_run_sent" if dry_run else "broadcast"
    return {
        "status": status,
        "report_id": report_row.get("sk"),
        "report": report_row,
        "dry_run": dry_run,
        "delivery_count": delivery_count,
        "model": AI_REVIEW_DEFAULT_MODEL,
        "token_usage": token_usage,
        "week": resolved_week,
        "period": period,
        "memory_count_in": len(prior_memories),
        "memory_count_out": written_memories,
        "envelope_parsed": parse_ok,
        "use_previous_season": use_previous_season,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_week(*, week: int | None, nfl_week: Any) -> int:
    """Return the week to recap. Caller override takes precedence;
    otherwise `nfl_state.week - 1`, clamped to >= 1. Week 22 is the
    upper bound (regular season + playoffs)."""
    if week is not None:
        try:
            value = int(week)
        except (TypeError, ValueError) as err:
            raise ValidationError(
                message=f"Invalid week override: {week!r}",
                handler="notif_ai_review_weekly",
                function="_resolve_week",
                field="week",
            ) from err
        if value < 1 or value > 22:
            raise ValidationError(
                message=(
                    f"week override must be in 1..22, got {value}"
                ),
                handler="notif_ai_review_weekly",
                function="_resolve_week",
                field="week",
            )
        return value

    try:
        nfl_week_value = int(nfl_week or 1)
    except (TypeError, ValueError):
        nfl_week_value = 1
    candidate = nfl_week_value - 1
    return max(1, candidate)


def _period(week: int, season: str | int) -> str:
    """Build the SK-suffix period string. e.g. (4, "2026") ->
    "2026W04". Falls back to "2026" when the season string isn't a
    year-shaped value."""
    season_str = str(season) if season else "2026"
    # Trim a possible PRESEASON-style suffix back to year.
    if "-" in season_str:
        season_str = season_str.split("-")[0]
    if not season_str.isdigit():
        season_str = "2026"
    return f"{season_str}W{int(week):02d}"


def _build_matchups(
    *,
    matchups_raw: list[dict[str, Any]],
    rosters: list[dict[str, Any]],
    sleeper_users: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Pair Sleeper matchup entries by `matchup_id` into a list of
    head-to-heads sorted by descending margin. Each entry carries
    `winner`, `loser`, `margin`, `is_tie`, plus bench-point hints
    pulled from `players_points` minus `starters_points`."""
    users_by_id = {u.get("user_id"): u for u in sleeper_users}
    roster_by_id = {r.get("roster_id"): r for r in rosters}

    def _side(entry: dict[str, Any]) -> dict[str, Any]:
        roster_id = entry.get("roster_id")
        roster = roster_by_id.get(roster_id, {}) if roster_id else {}
        owner_id = roster.get("owner_id")
        user = users_by_id.get(owner_id, {}) if owner_id else {}
        team_name = (user.get("metadata") or {}).get("team_name")
        display_name = user.get("display_name") or "Unknown"

        points = float(entry.get("points") or 0.0)
        starters_points = entry.get("starters_points") or []
        players_points = entry.get("players_points") or {}

        # Bench points = sum of all players_points minus the
        # starters_points list. Robust against missing fields.
        try:
            all_pp_total = sum(
                float(v or 0.0) for v in players_points.values()
            )
        except Exception:  # noqa: BLE001 — defensive
            all_pp_total = points
        try:
            starters_total = sum(
                float(v or 0.0) for v in starters_points
            )
        except Exception:  # noqa: BLE001 — defensive
            starters_total = points
        bench_points = max(0.0, all_pp_total - starters_total)

        return {
            "roster_id": roster_id,
            "user_id": owner_id,
            "manager_display_name": display_name,
            "team_name": team_name,
            "points": round(points, 2),
            "bench_points": round(bench_points, 2),
        }

    pairs: dict[int, list[dict[str, Any]]] = {}
    for entry in matchups_raw:
        mid = entry.get("matchup_id")
        if mid is None:
            continue
        pairs.setdefault(mid, []).append(entry)

    matchups: list[dict[str, Any]] = []
    for mid, group in pairs.items():
        if len(group) != 2:
            continue
        a, b = group
        a_side = _side(a)
        b_side = _side(b)
        if a_side["points"] >= b_side["points"]:
            winner, loser = a_side, b_side
        else:
            winner, loser = b_side, a_side
        margin = round(winner["points"] - loser["points"], 2)
        matchups.append(
            {
                "matchup_id": mid,
                "winner": winner,
                "loser": loser,
                "margin": margin,
                "is_tie": abs(margin) < 0.0001,
            }
        )

    matchups.sort(key=lambda m: float(m.get("margin") or 0.0), reverse=True)
    return matchups


def _parse_envelope(
    raw_text: str,
) -> tuple[str, list[dict[str, Any]], bool]:
    """Parse Claude's response as the F3 JSON envelope.

    Returns a tuple of `(body_markdown, memories, parse_ok)`.

    - On clean parse with `body_markdown` + `new_memories`: returns
      the markdown and a sanitized memory list, `parse_ok=True`.
    - On `json.JSONDecodeError` or missing `body_markdown`: returns
      the raw text as the markdown body, an empty memory list, and
      `parse_ok=False`. The recap is preserved even if Claude
      drifted away from the JSON contract.
    """
    text = (raw_text or "").strip()
    if not text:
        return "", [], False

    # Claude occasionally wraps the JSON in ```json fences despite
    # the explicit instruction. Strip them defensively.
    fenced = text
    if fenced.startswith("```"):
        fenced = fenced.split("\n", 1)[1] if "\n" in fenced else fenced[3:]
        if fenced.endswith("```"):
            fenced = fenced[:-3]
        fenced = fenced.strip()
        # Drop leading "json" tag if present.
        if fenced.lower().startswith("json\n"):
            fenced = fenced[5:].strip()

    try:
        envelope = json.loads(fenced)
    except json.JSONDecodeError as err:
        log.warning(
            f"notif_ai_review_weekly: failed to parse JSON envelope "
            f"({err}); persisting raw response as markdown"
        )
        return text, [], False

    if not isinstance(envelope, dict):
        log.warning(
            "notif_ai_review_weekly: JSON envelope is not an object; "
            "persisting raw response as markdown"
        )
        return text, [], False

    body_markdown = envelope.get("body_markdown")
    if not isinstance(body_markdown, str) or not body_markdown.strip():
        log.warning(
            "notif_ai_review_weekly: envelope missing body_markdown; "
            "persisting raw response as markdown"
        )
        return text, [], False

    raw_memories = envelope.get("new_memories") or []
    if not isinstance(raw_memories, list):
        raw_memories = []

    sanitized: list[dict[str, Any]] = []
    for entry in raw_memories[:AI_REVIEW_WEEKLY_MAX_NEW_MEMORIES]:
        if not isinstance(entry, dict):
            continue
        mtext = entry.get("text")
        sentiment = entry.get("sentiment")
        manager_user_id = entry.get("manager_user_id")
        if not isinstance(mtext, str) or not mtext.strip():
            continue
        if sentiment not in _VALID_SENTIMENTS:
            log.warning(
                f"notif_ai_review_weekly: dropping memory with bad "
                f"sentiment={sentiment!r}"
            )
            continue
        trimmed = mtext.strip()[:_MEMORY_TEXT_MAX]
        sanitized.append(
            {
                "text": trimmed,
                "sentiment": sentiment,
                "manager_user_id": (
                    manager_user_id
                    if isinstance(manager_user_id, str)
                    and manager_user_id.strip()
                    else None
                ),
            }
        )

    return body_markdown, sanitized, True


def _deliver(
    *,
    dry_run: bool,
    league_id: str,
    league_name: str,
    body_markdown: str,
    period: str,
    week: int,
) -> int:
    """Build per-user email payloads + push notifications and fan
    them out. Returns the number of recipients attempted (not the
    number of successful sends — per-channel success is logged via
    the existing notification_log path)."""
    recipients = _resolve_recipients(dry_run=dry_run)
    if not recipients:
        log.warning(
            f"notif_ai_review_weekly: no recipients resolved "
            f"(dry_run={dry_run}, league={league_id})"
        )
        return 0

    period_label = f"Week {week}"
    deep_link = f"xomper://ai-review/weekly/{period}"
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
            {"url": deep_link, "period": period, "week": week},
        )

    return len(recipients)


def _resolve_recipients(*, dry_run: bool) -> list[dict[str, Any]]:
    """For dry_run, return the single admin row (resolved from the
    whitelisted_users table). For broadcast, return all active
    users."""
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
