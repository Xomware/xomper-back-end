"""
Post-Draft AI Review prompts
============================
Pure functions that build the system + user prompts for F1's
post-draft analysis. Kept separate from `handler.py` so the prompt
text is unit-testable in isolation and changes go through normal PR
review.

The system prompt is a 2-block payload (tone+safety, lore) — each
block is sent to Anthropic with `cache_control: ephemeral` so
re-runs of the same league reuse the cached prefix. The user prompt
is uncached and carries the per-draft data.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.lore_prompt import (
    LORE_TEXT as _LORE_TEXT,
    build_lore_block,
)


# ---------------------------------------------------------------------------
# System prompt — block 1 (tone + safety)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_TONE = """You are the resident sports-talk-radio host for the Xomper fantasy football league, a 12-person dynasty league of college friends. Your job is to write a post-draft analysis report after the rookie draft.

Voice:
- Funny, unserious, lightly mean. Roast each manager by name.
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
- Do NOT invent NFL news, injuries, arrests, or suspensions. If you don't have data on a player in the user prompt, treat them as a generic prospect at their position.
- Do not reference specific real-world tragedies, deaths, or news events.
- If a roast feels like it crosses a line, soften it. Better to be tame than to land wrong.

Output format:
- Open with a one-paragraph league-wide intro setting up the draft.
- Then one H2 section per team, in the same order as the team summaries provided in the user prompt (which mirrors draft slot 1 -> 12 in the first round).
- Each team section is ~2 short paragraphs: first paragraph grades the picks + analyzes positional / value fit, second paragraph is the personality roast tying picks to lore.
- End with a one-paragraph closer projecting who "won the draft" and who needs a hug.
- Output pure markdown. No HTML. No code fences around the markdown. Use **bold** for emphasis and `>` for the occasional one-liner pull-quote."""


# ---------------------------------------------------------------------------
# System prompt — block 2 (lore, factored to lambdas/common/lore_prompt.py)
# ---------------------------------------------------------------------------

# Re-exported so callers + tests that have been importing the lore
# constant from this module continue to work after the F2 refactor.
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
    team_summaries: list[dict[str, Any]],
    prior_standings: list[dict[str, Any]] | None,
) -> str:
    """Build the per-report user prompt.

    Args:
        league_name: Sleeper league name (e.g. "CLT DYNASTY").
        season: The draft season as a string (e.g. "2026").
        team_summaries: One dict per manager, in draft-slot order
            (slot 1 -> 12). Each dict carries:
              - `slot` (int, 1-indexed)
              - `manager_display_name` (str)
              - `team_name` (str | None)
              - `user_id` (str | None)
              - `picks`: list of `{round, pick_no, slot, player_name,
                position, nfl_team}` dicts
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
        f"The Xomper {season} rookie draft just wrapped for league "
        f"\"{league_name}\". Here's what happened."
    )
    lines.append("")
    lines.append("## Draft order and picks")
    lines.append("")

    if not team_summaries:
        lines.append("_No team summaries provided — treat the draft as empty._")
    else:
        for team in team_summaries:
            slot = team.get("slot")
            name = team.get("manager_display_name") or "Unknown"
            team_name = team.get("team_name")
            header = f"### Slot {slot}: {name}"
            if team_name:
                header += f" ({team_name})"
            lines.append(header)
            picks = team.get("picks") or []
            if not picks:
                lines.append("- (no picks recorded)")
            else:
                for pick in picks:
                    rd = pick.get("round")
                    pick_no = pick.get("pick_no")
                    player = pick.get("player_name") or "Unknown player"
                    pos = pick.get("position") or "?"
                    nfl_team = pick.get("nfl_team") or "FA"
                    lines.append(
                        f"- Round {rd}, Pick {pick_no}: {player} "
                        f"({pos}, {nfl_team})"
                    )
            lines.append("")

    lines.append("## Each team's prior-season standing")
    lines.append("")
    if not prior_standings:
        lines.append(
            "_Prior season standings unavailable (inaugural season or "
            "Sleeper history not linked). Skip the prior-standings angle "
            "and roast based on draft choices alone._"
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

    lines.append("## Your job")
    lines.append("")
    lines.append(
        "Write the post-draft analysis report following the system "
        "prompt's tone, safety, and format rules. One H2 per team, in "
        "slot order. Roast everyone. Don't invent news. Keep it tight."
    )

    return "\n".join(lines)
