"""
Lore prompt helper
==================
Shared builder for the "LEAGUE LORE" system block used by every AI
Review pipeline (F1 post-draft, F2 preseason, F3 weekly). Renders
`league_lore.LEAGUE_LORE` into a single cached system block so the
~3k-token lore payload is paid once per Anthropic cache TTL across
re-runs and across pipelines.

Extracted from `api_admin_ai_review_postdraft_trigger.prompts` during
F2 so both lambdas import from the same source. Any future tweak to
the rendered lore shape lives here.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.league_lore import LEAGUE_LORE, ManagerProfile


def _render_profile(user_id: str, profile: ManagerProfile) -> str:
    """Render a single ManagerProfile into the lore-section markdown
    fragment the prompt expects."""
    nicknames = ", ".join(profile.nicknames) if profile.nicknames else "(none)"
    teams = ", ".join(profile.favorite_teams) if profile.favorite_teams else "(none)"
    school = profile.school or "(unknown)"
    stories = (
        " | ".join(profile.notable_stories)
        if profile.notable_stories
        else "(none)"
    )
    life = " | ".join(profile.life_events) if profile.life_events else "(none)"
    return (
        f"### {profile.display_name}  (sleeper user_id: {user_id})\n"
        f"- Nicknames: {nicknames}\n"
        f"- Favorite teams: {teams}\n"
        f"- School: {school}\n"
        f"- Notable stories: {stories}\n"
        f"- Life events: {life}\n"
    )


def render_league_lore_text() -> str:
    """Render the full LEAGUE_LORE map into the static lore block text.

    Returned as a plain string so callers can either embed it inside a
    larger text payload or wrap it in a single cache-controlled
    Anthropic block via `build_lore_block`.
    """
    rendered = "\n".join(
        _render_profile(uid, profile) for uid, profile in LEAGUE_LORE.items()
    )
    return (
        "LEAGUE LORE — the 12 managers and what to know about them. "
        "Use this as the source of truth for any joke that references a person.\n\n"
        f"{rendered}"
    )


def build_lore_block() -> dict[str, Any]:
    """Return the lore as a single `{type, text, cache_control}` block.

    Marks `cache_control={"type": "ephemeral"}` so re-runs within the
    Anthropic cache TTL skip re-paying the lore-tokens cost.
    """
    return {
        "type": "text",
        "text": render_league_lore_text(),
        "cache_control": {"type": "ephemeral"},
    }


# Module-level snapshot so callers that want the raw text don't pay
# the rendering cost on every call (deterministic — LEAGUE_LORE is
# immutable for the lifetime of the lambda container).
LORE_TEXT = render_league_lore_text()
