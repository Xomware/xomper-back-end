"""
Tests for the admin-cron-settings gate inside `notif_lineup_not_set`.

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
        "display_name": f"U{idx}",
        "is_active": True,
        "is_admin": sleeper_id == ADMIN_ID,
    }


def _rosters_with_actionable_lineups() -> list[dict[str, Any]]:
    """Every roster has a starter marked OUT so each manager gets a ping."""
    return [
        {"roster_id": 1, "owner_id": ADMIN_ID, "starters": ["P_OUT"]},
        {"roster_id": 2, "owner_id": "user-1", "starters": ["P_OUT"]},
        {"roster_id": 3, "owner_id": "user-2", "starters": ["P_OUT"]},
    ]


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    from lambdas.notif_lineup_not_set import handler as h

    state: dict[str, Any] = {
        "cron_setting": {
            "cron_key": "notif_lineup_not_set",
            "enabled": True,
            "test_mode": False,
            "description": "Lineup",
        },
        "whitelisted": [
            _user_row(ADMIN_ID, idx=0),
            _user_row("user-1", idx=1),
            _user_row("user-2", idx=2),
        ],
        "send_push_calls": [],
        "send_email_call": None,
    }

    def _get_cron_setting(cron_key: str):
        return dict(state["cron_setting"])

    def _send_push_to_users(user_ids, title, body, category, data):
        state["send_push_calls"].append({"user_ids": list(user_ids)})

    def _send_emails(tasks):
        state["send_email_call"] = list(tasks)
        return (len(tasks), 0)

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
        h, "get_sleeper_league_rosters", lambda lid: _rosters_with_actionable_lineups()
    )
    monkeypatch.setattr(
        h,
        "get_sleeper_league_users",
        lambda lid: [
            {"user_id": ADMIN_ID, "display_name": "Admin"},
            {"user_id": "user-1", "display_name": "U1"},
            {"user_id": "user-2", "display_name": "U2"},
        ],
    )
    monkeypatch.setattr(
        h, "fetch_nfl_players", lambda: {"P_OUT": {"injury_status": "Out"}}
    )
    monkeypatch.setattr(
        h, "get_nfl_state", lambda: {"week": 4, "season": "2026"}
    )
    monkeypatch.setattr(h, "send_push_to_users", _send_push_to_users)
    monkeypatch.setattr(h, "send_emails_concurrently", _send_emails)

    return state


class TestDisabledShortCircuit:
    def test_disabled_returns_skipped(self, patched) -> None:
        from lambdas.notif_lineup_not_set.handler import handler

        patched["cron_setting"]["enabled"] = False
        response = handler({}, context=None)
        assert response["statusCode"] == 200
        body = response["body"]
        assert body["skipped"] is True
        assert patched["send_push_calls"] == []


class TestTestModeFiltering:
    def test_test_mode_filters_to_admin_only(self, patched) -> None:
        from lambdas.notif_lineup_not_set.handler import handler

        patched["cron_setting"]["test_mode"] = True
        handler({}, context=None)
        recipients = {
            uid
            for call in patched["send_push_calls"]
            for uid in call["user_ids"]
        }
        assert recipients == {ADMIN_ID}


class TestDefaultPassthrough:
    def test_default_settings_unfiltered_fan_out(self, patched) -> None:
        from lambdas.notif_lineup_not_set.handler import handler

        handler({}, context=None)
        recipients = {
            uid
            for call in patched["send_push_calls"]
            for uid in call["user_ids"]
        }
        assert ADMIN_ID in recipients
        assert "user-1" in recipients
        assert "user-2" in recipients
