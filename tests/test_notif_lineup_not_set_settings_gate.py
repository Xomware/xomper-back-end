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
    # The handler now asks notification_audience for the whole work list
    # rather than reading the Supabase whitelist itself.
    monkeypatch.setattr(
        h.notification_audience,
        "jobs",
        lambda: [
            h.notification_audience.NotificationJob(
                league_id="L1",
                league_name="Test",
                source="whitelist",
                recipients=list(state["whitelisted"]),
            )
        ],
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


class TestMultiLeague:
    """The cron used to run for exactly one league: the single active
    whitelist row. It now runs for every league the audience resolver
    returns."""

    def _jobs(self, h, state, jobs):
        return lambda: [
            h.notification_audience.NotificationJob(**j) for j in jobs
        ]

    def test_runs_for_every_league_it_is_given(self, patched, monkeypatch) -> None:
        from lambdas.notif_lineup_not_set import handler as h

        seen = []
        monkeypatch.setattr(
            h,
            "get_sleeper_league_rosters",
            lambda lid: seen.append(lid) or _rosters_with_actionable_lineups(),
        )
        monkeypatch.setattr(
            h.notification_audience,
            "jobs",
            self._jobs(h, patched, [
                {"league_id": "L1", "league_name": "One", "source": "whitelist",
                 "recipients": list(patched["whitelisted"])},
                {"league_id": "L2", "league_name": "Two", "source": "follows",
                 "recipients": list(patched["whitelisted"])},
            ]),
        )

        body = h.handler({}, context=None)["body"]

        assert seen == ["L1", "L2"]
        assert body["leagues"] == ["L1", "L2"]

    def test_a_league_with_no_reachable_recipient_costs_no_sleeper_call(
        self, patched, monkeypatch
    ) -> None:
        from lambdas.notif_lineup_not_set import handler as h

        seen = []
        monkeypatch.setattr(
            h,
            "get_sleeper_league_rosters",
            lambda lid: seen.append(lid) or _rosters_with_actionable_lineups(),
        )
        monkeypatch.setattr(
            h.notification_audience,
            "jobs",
            self._jobs(h, patched, [
                {"league_id": "L1", "league_name": "One", "source": "follows",
                 "recipients": []},
                {"league_id": "L2", "league_name": "Two", "source": "follows",
                 "recipients": list(patched["whitelisted"])},
            ]),
        )

        h.handler({}, context=None)

        # Fetching rosters for a league with nobody to tell is the exact spend
        # the follow table exists to avoid.
        assert seen == ["L2"]

    def test_no_leagues_at_all_short_circuits(self, patched, monkeypatch) -> None:
        from lambdas.notif_lineup_not_set import handler as h

        monkeypatch.setattr(h.notification_audience, "jobs", lambda: [])

        body = h.handler({}, context=None)["body"]

        assert body["reason"] == "no league to notify"
        assert patched["send_push_calls"] == []

    def test_each_league_is_named_in_its_own_email(self, patched, monkeypatch) -> None:
        from lambdas.notif_lineup_not_set import handler as h

        monkeypatch.setattr(
            h.notification_audience,
            "jobs",
            self._jobs(h, patched, [
                {"league_id": "L1", "league_name": "Sunday Money", "source": "follows",
                 "recipients": list(patched["whitelisted"])},
            ]),
        )

        h.handler({}, context=None)
        subjects = [task[1] for task in patched["send_email_call"]]

        # One cron run now spans leagues, so a subject naming the wrong one
        # would be actively confusing rather than merely wrong.
        assert all("Sunday Money" in s for s in subjects)
