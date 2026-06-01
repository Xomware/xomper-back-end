"""
Tests for the admin-cron-settings gate inside `notif_weekly_recap`.

Covers:
  - enabled=False short-circuits before any data fetch + send.
  - test_mode=True restricts recipient list to admin only.
  - Default (enabled=True, test_mode=False) preserves the existing
    behavior (whitelisted_users passed through unfiltered).
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest


ADMIN_ID = "594625531702460416"


def _admin_row() -> dict[str, Any]:
    return {
        "id": "row-admin",
        "sleeper_user_id": ADMIN_ID,
        "email": "admin@example.com",
        "display_name": "Admin",
        "is_active": True,
        "is_admin": True,
    }


def _other_user(idx: int) -> dict[str, Any]:
    return {
        "id": f"row-u{idx}",
        "sleeper_user_id": f"user-{idx}",
        "email": f"user{idx}@example.com",
        "display_name": f"User {idx}",
        "is_active": True,
        "is_admin": False,
    }


def _matchup_pair(matchup_id: int, roster_a: int, roster_b: int) -> list[dict[str, Any]]:
    return [
        {"matchup_id": matchup_id, "roster_id": roster_a, "points": 110.0},
        {"matchup_id": matchup_id, "roster_id": roster_b, "points": 95.0},
    ]


def _rosters() -> list[dict[str, Any]]:
    """Each roster's owner_id matches a user in the whitelisted list
    so the recipient resolution path is exercised."""
    return [
        {"roster_id": 1, "owner_id": ADMIN_ID, "starters": []},
        {"roster_id": 2, "owner_id": "user-1", "starters": []},
        {"roster_id": 3, "owner_id": "user-2", "starters": []},
        {"roster_id": 4, "owner_id": "user-3", "starters": []},
    ]


def _users() -> list[dict[str, Any]]:
    """Sleeper league users list (matches owner_id values in _rosters)."""
    return [
        {"user_id": ADMIN_ID, "display_name": "Admin", "metadata": {"team_name": "Admins"}},
        {"user_id": "user-1", "display_name": "U1", "metadata": {"team_name": "Ones"}},
        {"user_id": "user-2", "display_name": "U2", "metadata": {"team_name": "Twos"}},
        {"user_id": "user-3", "display_name": "U3", "metadata": {"team_name": "Threes"}},
    ]


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    from lambdas.notif_weekly_recap import handler as h

    state: dict[str, Any] = {
        "cron_setting": {
            "cron_key": "notif_weekly_recap",
            "enabled": True,
            "test_mode": False,
            "description": "Weekly recap",
        },
        "league_row": {"league_id": "L1", "league_name": "Test League"},
        "whitelisted": [_admin_row(), _other_user(1), _other_user(2), _other_user(3)],
        "rosters": _rosters(),
        "users": _users(),
        "nfl_state": {"week": 5, "season": "2026", "season_type": "regular"},
        "matchups": _matchup_pair(1, 1, 2) + _matchup_pair(2, 3, 4),
        "send_push_calls": [],
        "send_email_call": None,
    }

    def _get_cron_setting(cron_key: str):
        return dict(state["cron_setting"])

    def _send_push_to_users(user_ids, title, body, category, data):
        state["send_push_calls"].append(
            {"user_ids": list(user_ids), "title": title}
        )

    def _send_emails(tasks):
        state["send_email_call"] = list(tasks)
        return (len(tasks), 0)

    monkeypatch.setattr(h, "get_cron_setting", _get_cron_setting)
    monkeypatch.setattr(h, "get_active_whitelisted_league", lambda: state["league_row"])
    monkeypatch.setattr(
        h, "get_active_whitelisted_users", lambda: list(state["whitelisted"])
    )
    monkeypatch.setattr(h, "get_sleeper_league_rosters", lambda lid: state["rosters"])
    monkeypatch.setattr(h, "get_sleeper_league_users", lambda lid: state["users"])
    monkeypatch.setattr(
        h, "get_sleeper_league_matchups", lambda lid, week: state["matchups"]
    )
    monkeypatch.setattr(h, "get_nfl_state", lambda: state["nfl_state"])
    monkeypatch.setattr(h, "send_push_to_users", _send_push_to_users)
    monkeypatch.setattr(h, "send_emails_concurrently", _send_emails)

    return state


class TestDisabledShortCircuit:
    def test_disabled_returns_skipped(self, patched) -> None:
        from lambdas.notif_weekly_recap.handler import handler

        patched["cron_setting"]["enabled"] = False
        response = handler({}, context=None)

        assert response["statusCode"] == 200
        body = response["body"]
        assert body["skipped"] is True
        assert body["reason"] == "disabled"
        # No data fetch / send happened.
        assert patched["send_push_calls"] == []
        assert patched["send_email_call"] is None


class TestTestModeFiltering:
    def test_test_mode_filters_to_admin_only(self, patched) -> None:
        from lambdas.notif_weekly_recap.handler import handler

        patched["cron_setting"]["test_mode"] = True
        handler({}, context=None)

        # Only the admin's roster (owner_id=ADMIN_ID) should receive pushes.
        recipients = {
            uid
            for call in patched["send_push_calls"]
            for uid in call["user_ids"]
        }
        assert recipients == {ADMIN_ID}

    def test_test_mode_with_no_admin_in_list_sends_nothing(self, patched) -> None:
        """If the admin row is missing from whitelisted_users, test
        mode must produce zero sends (defensive behaviour)."""
        from lambdas.notif_weekly_recap.handler import handler

        patched["cron_setting"]["test_mode"] = True
        patched["whitelisted"] = [_other_user(1), _other_user(2)]

        handler({}, context=None)
        assert patched["send_push_calls"] == []


class TestDefaultPassthrough:
    def test_default_settings_unfiltered_fan_out(self, patched) -> None:
        from lambdas.notif_weekly_recap.handler import handler

        handler({}, context=None)
        recipients = {
            uid
            for call in patched["send_push_calls"]
            for uid in call["user_ids"]
        }
        # All 4 managers should be in the push set.
        assert ADMIN_ID in recipients
        assert "user-1" in recipients
        assert "user-2" in recipients
        assert "user-3" in recipients
