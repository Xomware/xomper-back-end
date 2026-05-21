"""
Tests for `api_admin_ai_review_preseason_trigger.prompts`.

Locks the prompt structure that F2 sends to Anthropic — drift in
either system block or in the user prompt body would land badly on
Claude's output, so these are intentionally noisy.
"""
from __future__ import annotations

import pytest

from lambdas.api_admin_ai_review_preseason_trigger.prompts import (
    PROMPT_VERSION,
    SYSTEM_PROMPT_LORE,
    SYSTEM_PROMPT_TONE,
    build_system_blocks,
    build_user_prompt,
)
from lambdas.common.constants import AI_REVIEW_PRESEASON_PROMPT_VERSION
from lambdas.common.league_lore import LEAGUE_LORE


class TestSystemBlocks:
    def test_returns_two_blocks(self) -> None:
        blocks = build_system_blocks()
        assert len(blocks) == 2

    def test_both_blocks_are_text_with_ephemeral_cache(self) -> None:
        for block in build_system_blocks():
            assert block["type"] == "text"
            assert block["cache_control"] == {"type": "ephemeral"}
            assert isinstance(block["text"], str)
            assert block["text"].strip(), "system block text is empty"

    def test_block_one_re_anchored_on_preseason(self) -> None:
        blocks = build_system_blocks()
        text = blocks[0]["text"]
        assert text == SYSTEM_PROMPT_TONE
        # The preseason-tuned anchors must appear.
        for phrase in [
            "preseason",
            "year-in-review",
            "outlook",
        ]:
            assert phrase in text, f"missing phrase: {phrase!r}"

    def test_block_one_keeps_safety_rails_verbatim(self) -> None:
        text = build_system_blocks()[0]["text"]
        # Same safety rails as F1 — must survive any preseason edit.
        for phrase in [
            "Safety rails",
            "No slurs",
            "Do NOT invent NFL news",
            "PG-13",
        ]:
            assert phrase in text, f"missing safety phrase: {phrase!r}"

    def test_block_two_is_the_shared_lore_block(self) -> None:
        blocks = build_system_blocks()
        lore = blocks[1]["text"]
        # SYSTEM_PROMPT_LORE re-exports the shared lore text — both
        # should match the shared `lore_prompt.LORE_TEXT` snapshot.
        from lambdas.common.lore_prompt import LORE_TEXT

        assert lore == LORE_TEXT
        assert lore == SYSTEM_PROMPT_LORE

    def test_block_two_contains_all_twelve_managers(self) -> None:
        lore = build_system_blocks()[1]["text"]
        for uid, profile in LEAGUE_LORE.items():
            assert uid in lore, f"missing user_id in lore: {uid}"
            assert profile.display_name in lore, (
                f"missing display_name in lore: {profile.display_name}"
            )

    def test_prompt_version_constant_is_pinned(self) -> None:
        # Tests + metadata writes both pin against this — keep it
        # locked to the constants module's value.
        assert PROMPT_VERSION == AI_REVIEW_PRESEASON_PROMPT_VERSION
        assert PROMPT_VERSION.startswith("f2-preseason-")


class TestBuildUserPrompt:
    def _sample_team(
        self,
        *,
        roster_id: int,
        manager: str,
        player_count: int = 15,
        with_prior: bool = True,
    ) -> dict:
        player_ids = [f"pid-{roster_id}-{i}" for i in range(1, player_count + 1)]
        starters = player_ids[:9]
        prior_finish = None
        if with_prior:
            prior_finish = {
                "rank": roster_id,
                "wins": 13 - roster_id,
                "losses": roster_id - 1,
                "ties": 0,
                "points_for": 1800.0 - roster_id * 25,
            }
        return {
            "roster_id": roster_id,
            "user_id": f"u{roster_id}",
            "manager_display_name": manager,
            "team_name": f"{manager}'s Team",
            "player_ids": player_ids,
            "starters": starters,
            "player_count": len(player_ids),
            "prior_finish": prior_finish,
        }

    def _sample_standings(self, count: int = 12) -> list[dict]:
        return [
            {
                "rank": i,
                "user_id": f"u{i}",
                "manager_display_name": f"Manager{i}",
                "team_name": f"Team{i}",
                "wins": 13 - i,
                "losses": i - 1,
                "ties": 0,
                "points_for": 1800.0 - i * 25,
                "points_against": 1500.0 + i * 10,
                "playoff_note": "made the playoffs" if i <= 4 else "",
            }
            for i in range(1, count + 1)
        ]

    def test_includes_final_standings_header(self) -> None:
        prompt = build_user_prompt(
            league_name="CLT DYNASTY",
            season="2026",
            team_rosters=[],
            prior_standings=self._sample_standings(),
        )
        assert "## Final 2025 Standings" in prompt

    def test_includes_all_twelve_teams_in_roster_id_order(self) -> None:
        teams = [
            self._sample_team(roster_id=i, manager=f"Manager{i}")
            for i in range(1, 13)
        ]
        prompt = build_user_prompt(
            league_name="CLT DYNASTY",
            season="2026",
            team_rosters=teams,
            prior_standings=self._sample_standings(),
        )
        # Each manager's roster header appears.
        for i in range(1, 13):
            assert f"Roster #{i}" in prompt
            assert f"Manager{i}" in prompt
        # Roster #1 comes before Roster #12.
        assert prompt.index("Roster #1") < prompt.index("Roster #12")
        # Twelve H2 headers expected — one per team.
        # (Two H2s come from headings sections, one for standings, one
        # for rosters, one for task — assert team H3 count.)
        h3_count = sum(
            1 for line in prompt.splitlines() if line.startswith("### Roster #")
        )
        assert h3_count == 12

    def test_each_team_renders_player_ids_in_starters_and_bench(self) -> None:
        team = self._sample_team(
            roster_id=1, manager="Manager1", player_count=15
        )
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_rosters=[team],
            prior_standings=None,
        )
        # Starters line shows 9 ids, bench line shows the remainder.
        for pid in team["starters"]:
            assert pid in prompt
        bench_ids = [
            pid for pid in team["player_ids"] if pid not in set(team["starters"])
        ]
        for pid in bench_ids:
            assert pid in prompt
        assert "Roster size: 15" in prompt

    def test_team_header_includes_prior_year_record_when_available(self) -> None:
        team = self._sample_team(
            roster_id=1, manager="Manager1", with_prior=True
        )
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_rosters=[team],
            prior_standings=None,
        )
        # Header bears the last-year record + PF.
        assert "last year: #1 / 12-0 / 1775.0 PF" in prompt

    def test_missing_prior_standings_includes_fallback_text(self) -> None:
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_rosters=[],
            prior_standings=None,
        )
        assert "Prior season standings unavailable" in prompt

    def test_empty_team_rosters_includes_fallback_text(self) -> None:
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_rosters=[],
            prior_standings=None,
        )
        assert "No current rosters available" in prompt

    def test_contains_task_footer(self) -> None:
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_rosters=[],
            prior_standings=None,
        )
        assert "## Your task" in prompt
        assert "roster_id ascending" in prompt
        assert "Don't invent" in prompt

    def test_non_numeric_season_falls_back_for_prior_year_label(self) -> None:
        # Backend may receive "2026-PRESEASON" as the season string in
        # weird cases — _prior_year_label should not crash.
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026-PRESEASON",
            team_rosters=[],
            prior_standings=None,
        )
        assert "## Final the prior season Standings" in prompt
