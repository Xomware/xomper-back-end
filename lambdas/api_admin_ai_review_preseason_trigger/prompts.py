"""
Preseason AI Review prompts
===========================
Pure functions that build the system + user prompts for F2's
preseason "year-in-review + outlook" analysis. Kept separate from
`handler.py` so the prompt text is unit-testable in isolation and
changes go through normal PR review.

The system prompt is a 2-block payload (tone+safety re-anchored on
preseason, lore via the shared `lore_prompt` helper) — each block is
sent to Anthropic with `cache_control: ephemeral` so re-runs reuse
the cached prefix and so the lore tokens amortize across F1 + F2 +
F3 when calls land within the cache TTL.

User prompt: this year's roster snapshot per team (`player_id`
strings — no name resolution in v1) + last year's final standings
per team. Ordered by `roster_id` asc on the backend (proxy for the
draft-slot ordering the iOS client uses).

Pinned `PROMPT_VERSION` is `f2-preseason-2026-05-21`.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.constants import AI_REVIEW_PRESEASON_PROMPT_VERSION
from lambdas.common.lore_prompt import (
    LORE_TEXT as _LORE_TEXT,
    build_lore_block,
)


# Mirror of F1's PROMPT_VERSION convention — also re-exported as a
# module constant so tests + metadata writes can pin against drift.
PROMPT_VERSION = AI_REVIEW_PRESEASON_PROMPT_VERSION


# ---------------------------------------------------------------------------
# System prompt — block 1 (tone + safety, re-anchored for preseason)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_TONE = """You are the resident sports-talk-radio host for the Xomper fantasy football league, a 12-person dynasty league of college friends. Your job is to write the preseason year-in-review + season outlook report, fired in late August / early September before Week 1 kickoff.

Voice:
- Funny, unserious, lightly mean. Roast each manager by name.
- You're roasting your friends about their fantasy futures, leaning on last year's wins and losses and the lineup they're walking into Week 1 with. Personal but never cruel. Specific, not generic.
- Lean on inside jokes from the lore section below.
- Sound like a friend at a bar who has watched too much fantasy YouTube, not a corporate AI assistant.
- No corporate hedge phrases. No "however, on the other hand…". No "as an AI language model". Never apologize.
- Short punchy paragraphs. Use markdown headings (H2 per team).

Safety rails — these are non-negotiable:
- No slurs, no racial / ethnic / religious / sexual / gender / disability mockery.
- No jokes about substance abuse, addiction, eating disorders, self-harm, suicide, or mental health crises.
- No jokes about violence against people (real or implied).
- No jokes about anyone NOT in the lore section. Do not invent league members.
- No sexual content. Keep it PG-13 frat-house humor, not locker-room cruelty.
- Do NOT invent NFL news, injuries, arrests, trades, or suspensions. If you don't have data on a player_id in the user prompt, lean on your training-data knowledge but treat them as a generic player at their position when in doubt — never fabricate news events.
- Do not reference specific real-world tragedies, deaths, or news events.
- If a roast feels like it crosses a line, soften it. Better to tame than to land wrong.

Output format:
- Open with a one-paragraph league-wide intro setting up the year — last year's chip winner, last year's basement team, who's hungry, who's coasting.
- Then one H2 section per team, in the same order as the team rosters provided in the user prompt (sorted by roster_id ascending).
- Each team section is ~2 short paragraphs: first paragraph is the year-in-review (one or two lines about last year's record + PF), second paragraph is the outlook for the coming year tying the current roster + a lore tidbit together.
- Every paragraph must reference at least one concrete data point: a record from last year, a points-for total, a specific player_id from the current roster, or a known lore tidbit.
- End with a one-paragraph closer projecting who you think wins it all this year and who's drafting in the basement again.
- Output pure markdown. No HTML. No code fences around the markdown. Use **bold** for emphasis and `>` for the occasional one-liner pull-quote."""


# ---------------------------------------------------------------------------
# System prompt — block 2 (lore, sourced from lambdas/common/lore_prompt.py)
# ---------------------------------------------------------------------------

# Re-exported so callers + tests that want to assert against the lore
# content can pull it from this module alongside `SYSTEM_PROMPT_TONE`.
SYSTEM_PROMPT_LORE = _LORE_TEXT


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


def build_system_blocks() -> list[dict[str, Any]]:
    """Return the two-block system prompt payload Anthropic expects.

    Each block is tagged with `cache_control: ephemeral` so that the
    tone+safety scaffolding and the league lore are cached across
    subsequent calls (dry-run + force re-runs land warm)."""
    return [
        {
            "type": "text",
            "text": SYSTEM_PROMPT_TONE,
            "cache_control": {"type": "ephemeral"},
        },
        build_lore_block(),
    ]


def build_user_prompt(
    *,
    league_name: str,
    season: str,
    team_rosters: list[dict[str, Any]],
    prior_standings: list[dict[str, Any]] | None,
) -> str:
    """Build the per-report user prompt.

    Args:
        league_name: Sleeper league name (e.g. "CLT DYNASTY").
        season: The upcoming season as a string (e.g. "2026").
        team_rosters: One dict per manager, sorted by roster_id asc.
            Each dict carries:
              - `roster_id` (int)
              - `manager_display_name` (str)
              - `team_name` (str | None)
              - `user_id` (str | None)
              - `player_ids`: list of player_id strings
              - `starters`: list of player_id strings (best-effort)
              - `prior_finish`: optional dict with `{rank, wins,
                losses, ties, points_for}` from last year, or None
        prior_standings: Optional list of `{rank, manager_display_name,
            team_name, wins, losses, ties, points_for, points_against,
            playoff_note}` dicts, sorted best -> worst. Pass `None` or
            empty when the prior season isn't available.

    Returns:
        The user message to send as the `user` role in Anthropic's
        Messages API.
    """
    lines: list[str] = []
    lines.append(
        f"This is the {season} preseason. Here is the data for the league "
        f"\"{league_name}\"."
    )
    lines.append("")

    # --- prior standings table -------------------------------------------------
    prior_year = _prior_year_label(season)
    lines.append(f"## Final {prior_year} Standings")
    lines.append("")
    if not prior_standings:
        lines.append(
            "_Prior season standings unavailable (inaugural season or "
            "Sleeper history not linked). Skip the year-in-review angle "
            "and roast on the outlook alone._"
        )
    else:
        for row in prior_standings:
            rank = row.get("rank")
            mgr = row.get("manager_display_name") or "Unknown"
            team_name = row.get("team_name") or mgr
            wins = row.get("wins", 0)
            losses = row.get("losses", 0)
            ties = row.get("ties", 0)
            pf = row.get("points_for", 0)
            pa = row.get("points_against", 0)
            note = row.get("playoff_note") or ""
            record = f"{wins}-{losses}"
            if ties:
                record += f"-{ties}"
            line = (
                f"- #{rank} {mgr} ({team_name}) — {record}, "
                f"PF {pf:.1f}, PA {pa:.1f}"
            )
            if note:
                line += f" — {note}"
            lines.append(line)
    lines.append("")

    # --- current roster snapshot ----------------------------------------------
    lines.append(f"## {season} Rosters")
    lines.append("")
    if not team_rosters:
        lines.append(
            "_No current rosters available — treat this as a fresh league._"
        )
    else:
        for team in team_rosters:
            roster_id = team.get("roster_id")
            name = team.get("manager_display_name") or "Unknown"
            team_name = team.get("team_name")
            prior = team.get("prior_finish") or {}
            header_bits = [f"Roster #{roster_id}", name]
            if team_name:
                header_bits.append(f"({team_name})")
            if prior:
                wins = prior.get("wins", 0)
                losses = prior.get("losses", 0)
                ties = prior.get("ties", 0)
                record = f"{wins}-{losses}"
                if ties:
                    record += f"-{ties}"
                prior_pf = prior.get("points_for")
                rank = prior.get("rank")
                bits = []
                if rank is not None:
                    bits.append(f"#{rank}")
                bits.append(record)
                if prior_pf is not None:
                    bits.append(f"{prior_pf:.1f} PF")
                header_bits.append(f"[last year: {' / '.join(bits)}]")
            lines.append(f"### {' '.join(header_bits)}")
            player_ids = team.get("player_ids") or []
            starters = team.get("starters") or []
            if not player_ids:
                lines.append("- (no players on roster)")
            else:
                lines.append(
                    f"- Roster size: {len(player_ids)} "
                    f"(starters: {len(starters)})"
                )
                if starters:
                    lines.append(
                        "- Starters: "
                        + ", ".join(starters)
                    )
                bench = [pid for pid in player_ids if pid not in set(starters)]
                if bench:
                    lines.append("- Bench: " + ", ".join(bench))
            lines.append("")

    # --- task footer ----------------------------------------------------------
    lines.append("## Your task")
    lines.append("")
    lines.append(
        "Write a preseason recap that grades each manager on last year and "
        "previews this year, with one H2 per team in roster_id ascending "
        "order. Use the lore. Use the data. Don't invent injuries, trades, "
        "or news that isn't in the data — player_ids are raw Sleeper IDs, "
        "lean on training-data knowledge for player narratives but never "
        "fabricate events. Keep each team section ~2 paragraphs."
    )

    return "\n".join(lines)


def _prior_year_label(season: str) -> str:
    """Best-effort guess at the prior-year label for the standings
    header. Falls back to "the prior season" when the season string
    isn't an integer year (e.g. "2026-PRESEASON")."""
    try:
        year = int(season)
        return str(year - 1)
    except (TypeError, ValueError):
        return "the prior season"
