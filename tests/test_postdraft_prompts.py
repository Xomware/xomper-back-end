"""
Tests for `api_admin_ai_review_postdraft_trigger.prompts`.

Locks the prompt structure that F1 sends to Anthropic — drift in
either system block or in the user prompt body would land badly on
Claude's output, so these are intentionally noisy.
"""
from __future__ import annotations

import pytest

from lambdas.api_admin_ai_review_postdraft_trigger.prompts import (
    SYSTEM_PROMPT_LORE,
    SYSTEM_PROMPT_TONE,
    build_system_blocks,
    build_user_prompt,
)
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

    def test_block_one_contains_safety_rails(self) -> None:
        blocks = build_system_blocks()
        text = blocks[0]["text"]
        assert text == SYSTEM_PROMPT_TONE
        # A few canonical safety phrases must survive any edit.
        for phrase in [
            "Safety rails",
            "No slurs",
            "Do NOT invent NFL news",
            "PG-13",
        ]:
            assert phrase in text, f"missing phrase: {phrase!r}"

    def test_block_two_contains_all_twelve_managers(self) -> None:
        blocks = build_system_blocks()
        lore = blocks[1]["text"]
        assert lore == SYSTEM_PROMPT_LORE
        for uid, profile in LEAGUE_LORE.items():
            assert uid in lore, f"missing user_id in lore: {uid}"
            assert profile.display_name in lore, (
                f"missing display_name in lore: {profile.display_name}"
            )

    def test_lore_includes_tony_toilet_story(self) -> None:
        # Spot-check that the lore content is real (not a placeholder).
        lore = build_system_blocks()[1]["text"]
        assert "toilet" in lore.lower()
        assert "sec championship" in lore.lower()


class TestBuildUserPrompt:
    def _sample_team(self, slot: int, manager: str, picks: int = 5) -> dict:
        return {
            "slot": slot,
            "manager_display_name": manager,
            "team_name": f"{manager}'s Team",
            "user_id": f"u{slot}",
            "picks": [
                {
                    "round": ((i - 1) // 12) + 1,
                    "pick_no": slot + (i - 1) * 12,
                    "slot": slot,
                    "player_name": f"Player {slot}-{i}",
                    "position": "WR",
                    "nfl_team": "DAL",
                }
                for i in range(1, picks + 1)
            ],
        }

    def _sample_standings(self, count: int = 12) -> list[dict]:
        return [
            {
                "rank": i,
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

    def test_includes_all_twelve_teams_in_slot_order(self) -> None:
        teams = [
            self._sample_team(slot=i, manager=f"Manager{i}", picks=5)
            for i in range(1, 13)
        ]
        prompt = build_user_prompt(
            league_name="CLT DYNASTY",
            season="2026",
            team_summaries=teams,
            prior_standings=self._sample_standings(),
        )
        # Each manager's H3 header appears.
        for i in range(1, 13):
            assert f"### Slot {i}: Manager{i}" in prompt
        # Slot 1 comes before slot 12.
        assert prompt.index("Slot 1:") < prompt.index("Slot 12:")

    def test_renders_each_pick_with_round_pick_position_and_team(self) -> None:
        teams = [self._sample_team(slot=1, manager="Manager1", picks=5)]
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_summaries=teams,
            prior_standings=None,
        )
        for i in range(1, 6):
            expected_round = ((i - 1) // 12) + 1
            expected_pick = 1 + (i - 1) * 12
            assert (
                f"Round {expected_round}, Pick {expected_pick}: Player 1-{i} (WR, DAL)"
                in prompt
            )

    def test_handles_sixty_picks_across_twelve_teams(self) -> None:
        teams = [
            self._sample_team(slot=i, manager=f"M{i}", picks=5)
            for i in range(1, 13)
        ]
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_summaries=teams,
            prior_standings=None,
        )
        # 12 teams * 5 picks = 60 pick lines.
        pick_lines = [line for line in prompt.splitlines() if line.startswith("- Round")]
        assert len(pick_lines) == 60

    def test_prior_standings_render_in_order_with_records(self) -> None:
        standings = self._sample_standings(12)
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_summaries=[],
            prior_standings=standings,
        )
        for row in standings:
            line = (
                f"- #{row['rank']} Manager{row['rank']} (Team{row['rank']}) — "
                f"{row['wins']}-{row['losses']}, "
                f"PF {row['points_for']:.1f}, "
                f"PA {row['points_against']:.1f}"
            )
            assert line in prompt or line.split(" — ")[0] in prompt

    def test_missing_prior_standings_includes_fallback_text(self) -> None:
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_summaries=[],
            prior_standings=None,
        )
        assert "Prior season standings unavailable" in prompt

    def test_empty_team_summaries_includes_fallback_text(self) -> None:
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_summaries=[],
            prior_standings=None,
        )
        assert "No team summaries provided" in prompt

    def test_contains_job_instruction_block(self) -> None:
        prompt = build_user_prompt(
            league_name="CLT",
            season="2026",
            team_summaries=[],
            prior_standings=None,
        )
        assert "## Your job" in prompt
        assert "One H2 per team" in prompt
        assert "Don't invent news" in prompt


class TestPromptCachingInjection:
    def test_default_cache_control_is_ephemeral(self) -> None:
        for block in build_system_blocks():
            assert block["cache_control"]["type"] == "ephemeral"
