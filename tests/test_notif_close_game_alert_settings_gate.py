"""
Tests for the admin-cron-settings gate inside `notif_close_game_alert`.

Covers:
  - enabled=False short-circuits before any data fetch + send.
  - test_mode=True restricts recipient list to admin only.
"""
from __future__ import annotations

from typing import Any

import pytest


ADMIN_ID = "594625531702460416"


def _user_row(sleeper_id: str, *, idx: int) -> dict[str, Any]:
    return {
        "sleeper_user_id": sleeper_id,
        "email": f"u{idx}@example.com",
        "is_active": True,
        "is_admin": sleeper_id == ADMIN_ID,
    }


def _close_matchups() -> list[dict[str, Any]]:
    """One pair within the close-game threshold (110 vs 105)."""
    return [
        {"matchup_id": 1, "roster_id": 1, "points": 110.0},
        {"matchup_id": 1, "roster_id": 2, "points": 105.0},
    ]


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    from lambdas.notif_close_game_alert import handler as h

    state: dict[str, Any] = {
        "cron_setting": {
            "cron_key": "notif_close_game_alert",
            "enabled": True,
            "test_mode": False,
            "description": "Close",
        },
        "whitelisted": [_user_row(ADMIN_ID, idx=0), _user_row("user-1", idx=1)],
        "send_push_calls": [],
    }

    def _get_cron_setting(cron_key: str):
        return dict(state["cron_setting"])

    def _send_push_to_users(user_ids, title, body, category, data):
        state["send_push_calls"].append({"user_ids": list(user_ids)})

    monkeypatch.setattr(h, "get_cron_setting", _get_cron_setting)
    monkeypatch.setattr(
        h,
        "get_active_whitelisted_league",
        lambda: {"league_id": "L1", "league_name": "Test"},
    )
    monkeypatch.setattr(
        h, "get_active_whitelisted_users", lambda: list(state["whitelisted"])
    )
    monkeypatch.setattr(
        h,
        "get_sleeper_league_rosters",
        lambda lid: [
            {"roster_id": 1, "owner_id": ADMIN_ID},
            {"roster_id": 2, "owner_id": "user-1"},
        ],
    )
    monkeypatch.setattr(
        h,
        "get_sleeper_league_users",
        lambda lid: [
            {"user_id": ADMIN_ID, "display_name": "Admin"},
            {"user_id": "user-1", "display_name": "U1"},
        ],
    )
    monkeypatch.setattr(h, "get_sleeper_league_matchups", lambda lid, w: _close_matchups())
    monkeypatch.setattr(h, "get_nfl_state", lambda: {"week": 4, "season": "2026"})
    monkeypatch.setattr(h, "send_push_to_users", _send_push_to_users)

    return state


class TestDisabledShortCircuit:
    def test_disabled_returns_skipped(self, patched) -> None:
        from lambdas.notif_close_game_alert.handler import handler

        patched["cron_setting"]["enabled"] = False
        response = handler({}, context=None)
        assert response["statusCode"] == 200
        body = response["body"]
        assert body["skipped"] is True
        assert patched["send_push_calls"] == []


class TestTestModeFiltering:
    def test_test_mode_filters_to_admin_only(self, patched) -> None:
        from lambdas.notif_close_game_alert.handler import handler

        patched["cron_setting"]["test_mode"] = True
        handler({}, context=None)
        recipients = {
            uid
            for call in patched["send_push_calls"]
            for uid in call["user_ids"]
        }
        assert recipients == {ADMIN_ID}


class TestDefaultPassthrough:
    def test_default_unfiltered_fan_out(self, patched) -> None:
        from lambdas.notif_close_game_alert.handler import handler

        handler({}, context=None)
        recipients = {
            uid
            for call in patched["send_push_calls"]
            for uid in call["user_ids"]
        }
        assert ADMIN_ID in recipients
        assert "user-1" in recipients
