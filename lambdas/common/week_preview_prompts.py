"""
Week Preview AI prompts
=======================
Builds the system + user prompts for the Wednesday-morning Week
Preview newsletter. Mirrors `weekly_prompts.py` structure (3-block
system, JSON-envelope output) so the orchestrator can share the
Anthropic call shape.

System blocks:
1. Tone + safety + output structure (preview-tuned — forward-looking,
   not a roast of last week)
2. Lore (shared)
3. JSON envelope contract

User prompt: upcoming-week matchups, current standings, WC standings,
prior memories, the player-id starter map for each team.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.constants import AI_REVIEW_WEEKLY_PROMPT_VERSION
from lambdas.common.lore_prompt import build_lore_block


PROMPT_VERSION = AI_REVIEW_WEEKLY_PROMPT_VERSION + "-preview"


SYSTEM_PROMPT_TONE = """You are the resident sports-talk-radio host for the Xomper fantasy football league, a 12-person dynasty league of college friends. Your job is to write the WEDNESDAY-MORNING WEEK PREVIEW NEWSLETTER — a forward-looking matchup preview for the upcoming weekend, fired before waiver claims process.

Voice:
- Same league personality as the weekly recap, but FORWARD-LOOKING. You're previewing what's about to happen, not roasting what already did.
- Funny, unserious. Tease setups for upcoming matchups. Reference past memories from earlier in the season when relevant (continuity is funny).
- Lean on inside jokes from the lore section. Speak to managers by name.
- Sound like a friend at a bar handicapping the weekend — not a corporate AI assistant.
- No corporate hedge phrases. No "however, on the other hand…". Never apologize.
- Short punchy paragraphs.

Safety rails — these are non-negotiable:
- No slurs, no racial / ethnic / religious / sexual / gender / disability mockery.
- No jokes about substance abuse, addiction, eating disorders, self-harm, suicide, or mental health crises.
- No jokes about violence against people (real or implied).
- No jokes about anyone NOT in the lore section. Do not invent league members.
- No sexual content. Keep it PG-13 frat-house humor, not locker-room cruelty.
- Do NOT invent NFL news, injuries, trades, arrests, or suspensions. The matchup data + player_ids in the user prompt are ground truth — if a player isn't called out as injured in the user prompt, don't claim they are.
- Do not reference specific real-world tragedies, deaths, or news events.
- If a roast feels like it crosses a line, soften it.

Output format — CRITICAL. Respond with ONLY a single JSON object — no preamble, no code fences, no surrounding markdown. The third system block specifies the exact envelope shape.

Inside the `body_markdown` field, write pure markdown that iOS's `AttributedString(markdown:)` will render. Follow this EXACT structure:

1. `# Week N Preview` on its own line (N is the upcoming week from the user prompt). Blank line.
2. One opening paragraph framing the week's biggest storyline. Blank line.
3. `## World Cup Watch` on its own line. Blank line. One paragraph on who's on the WC bubble + clinch/elimination scenarios for this week. Blank line.
4. `## Team-by-Team` on its own line. Blank line. For EACH of the 12 teams, in standings order (best record first):
   - `### Team Name (W-L)` on its own line. Blank line.
   - One 2-3 sentence paragraph: their week's stakes, key starters, momentum read. Blank line after.
5. `## Game by Game` on its own line. Blank line. For EACH matchup pair, in any natural order:
   - `### Team A (W-L) vs Team B (W-L)` on its own line. Blank line.
   - One 2-3 sentence blurb: head-to-head context, key starters, playoff/WC stakes. Blank line after.
6. `## Bold Prediction` on its own line. Blank line. One short paragraph hot take.

Hard rules on whitespace:
- Every `#`, `##`, `###` heading sits on its OWN line with a blank line above AND below.
- No two paragraphs share a line. Always `\\n\\n` between paragraphs.
- No HTML. No code fences. Use `**bold**` for emphasis.
- Every paragraph references at least one concrete data point: a record, a player_id, a prior memory, a WC standing, a lore tidbit.

After the markdown, return 2-4 `new_memories` summarizing THIS week's forward-looking storylines (not what already happened — what's about to). Keep each under 200 chars."""


SYSTEM_PROMPT_OUTPUT_SPEC = """OUTPUT FORMAT — required JSON envelope

You MUST respond with a single valid JSON object that matches this exact shape. No markdown code fences. No commentary outside the JSON.

Schema:
{
  "body_markdown": "string — the full week preview as markdown",
  "new_memories": [
    {
      "text": "string under 200 chars — one-line forward-looking storyline",
      "manager_user_id": "string Sleeper user_id from the lore section, or null for league-wide memories",
      "sentiment": "roast" | "praise" | "lore"
    }
  ]
}

Rules for `new_memories`:
- 2-4 entries. Each one a specific storyline tied to a manager or matchup."""


def build_system_blocks() -> list[dict[str, Any]]:
    """Three-block system prompt. Tone + lore + output spec each
    cached with `ephemeral` so cross-call cache reuse stays warm."""
    return [
        {
            "type": "text",
            "text": SYSTEM_PROMPT_TONE,
            "cache_control": {"type": "ephemeral"},
        },
        build_lore_block(),
        {
            "type": "text",
            "text": SYSTEM_PROMPT_OUTPUT_SPEC,
            "cache_control": {"type": "ephemeral"},
        },
    ]


def build_user_prompt(
    *,
    league_name: str,
    season: str,
    week: int,
    matchup_pairs: list[dict[str, Any]],
    standings: list[dict[str, Any]],
    wc_standings: list[dict[str, Any]],
    recent_memories: list[dict[str, Any]],
) -> str:
    """Per-week preview prompt.

    Args:
        league_name: Sleeper league name.
        season: e.g. "2026".
        week: Upcoming week N (e.g. 12).
        matchup_pairs: list of dicts:
          `{matchup_id, team_a: {team_name, manager, user_id, wins,
          losses, starters: [player_id], division}, team_b: same}`
        standings: cumulative season standings — list of
          `{rank, team_name, manager, user_id, wins, losses, points_for}`.
        wc_standings: per-division WC standings — list of
          `{division, position, team_name, user_id, wins, losses,
          points_for, status}`.
        recent_memories: last N items from xomper-ai-memories — list
          of `{text, manager_user_id, sentiment, week, season}`.
    """
    lines: list[str] = []
    lines.append(
        f"Week {week} of the {season} {league_name} season is about to "
        f"kick off Sunday. Preview the slate."
    )
    lines.append("")

    lines.append("## Standings going into the week")
    lines.append("")
    if not standings:
        lines.append("_No standings yet._")
    else:
        for s in standings:
            line = (
                f"- #{s.get('rank')} {s.get('team_name')} "
                f"({s.get('manager')}) — "
                f"{s.get('wins')}-{s.get('losses')}, "
                f"PF {s.get('points_for', 0):.1f}"
            )
            lines.append(line)
    lines.append("")

    lines.append("## World Cup standings")
    lines.append("")
    if not wc_standings:
        lines.append("_WC standings not available — skip the WC angle._")
    else:
        for s in wc_standings:
            lines.append(
                f"- {s.get('division')} #{s.get('position')}: "
                f"{s.get('team_name')} ({s.get('wins')}-{s.get('losses')}, "
                f"{s.get('status')})"
            )
    lines.append("")

    lines.append(f"## Week {week} matchups")
    lines.append("")
    if not matchup_pairs:
        lines.append("_No matchups returned by Sleeper for this week._")
    else:
        for m in matchup_pairs:
            a = m.get("team_a", {})
            b = m.get("team_b", {})
            lines.append(
                f"- {a.get('team_name')} ({a.get('wins')}-{a.get('losses')}) "
                f"vs {b.get('team_name')} ({b.get('wins')}-{b.get('losses')})"
            )
            a_starters = a.get("starters") or []
            b_starters = b.get("starters") or []
            if a_starters:
                lines.append(
                    f"  - {a.get('team_name')} starters (player_ids): "
                    f"{', '.join(str(s) for s in a_starters[:9])}"
                )
            if b_starters:
                lines.append(
                    f"  - {b.get('team_name')} starters (player_ids): "
                    f"{', '.join(str(s) for s in b_starters[:9])}"
                )
    lines.append("")

    lines.append("## Recent memories (season context)")
    lines.append("")
    if not recent_memories:
        lines.append("_No prior memories — write a clean slate preview._")
    else:
        for mem in recent_memories[:8]:
            wk = mem.get("week") or "?"
            txt = mem.get("text", "")
            lines.append(f"- Week {wk}: {txt}")
    lines.append("")

    lines.append("## Your job")
    lines.append("")
    lines.append(
        "Write the Week Preview newsletter following the system "
        "prompt's structure and whitespace rules. Cover all 12 teams "
        "under Team-by-Team, all 6 matchups under Game by Game, "
        "anchor every paragraph in a concrete data point, and return "
        "the JSON envelope from the third system block."
    )
    return "\n".join(lines)
