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
    # The handler asks notification_audience for the work list now.
    def _jobs():
        row = state["league_row"]
        if not row:
            return []
        return [
            h.notification_audience.NotificationJob(
                league_id=row["league_id"],
                league_name=row.get("league_name", "League"),
                source="whitelist",
                recipients=list(state["whitelisted"]),
            )
        ]

    monkeypatch.setattr(h.notification_audience, "jobs", _jobs)
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


class TestMultiLeague:
    """The recap body moved into _recap_league so the handler can loop.

    A ~200-line reindent is where a counter or an early return quietly
    changes meaning, so these pin the aggregation rather than the prose.
    """

    def _jobs(self, h, ids, recipients):
        return lambda: [
            h.notification_audience.NotificationJob(
                league_id=lid, league_name=f"L {lid}", source="follows",
                recipients=list(recipients),
            )
            for lid in ids
        ]

    def test_recaps_every_league_it_is_given(self, patched, monkeypatch) -> None:
        from lambdas.notif_weekly_recap import handler as h

        seen = []
        monkeypatch.setattr(
            h, "get_sleeper_league_matchups",
            lambda lid, week: seen.append(lid) or patched["matchups"],
        )
        monkeypatch.setattr(
            h.notification_audience, "jobs",
            self._jobs(h, ["L1", "L2"], patched["whitelisted"]),
        )

        body = h.handler({}, context=None)["body"]

        assert seen == ["L1", "L2"]
        assert sorted(body["leagues"]) == ["L1", "L2"]

    def test_counts_accumulate_across_leagues(self, patched, monkeypatch) -> None:
        from lambdas.notif_weekly_recap import handler as h

        monkeypatch.setattr(
            h.notification_audience, "jobs",
            self._jobs(h, ["L1"], patched["whitelisted"]),
        )
        one = h.handler({}, context=None)["body"]["push_sent"]

        monkeypatch.setattr(
            h.notification_audience, "jobs",
            self._jobs(h, ["L1", "L2"], patched["whitelisted"]),
        )
        two = h.handler({}, context=None)["body"]["push_sent"]

        # Counters live in the handler and are summed from the helper's
        # return; initialising them inside the loop would report only the
        # last league.
        assert one > 0
        assert two == one * 2

    def test_a_league_with_nobody_to_tell_costs_no_sleeper_call(
        self, patched, monkeypatch
    ) -> None:
        from lambdas.notif_weekly_recap import handler as h

        seen = []
        monkeypatch.setattr(
            h, "get_sleeper_league_matchups",
            lambda lid, week: seen.append(lid) or patched["matchups"],
        )
        monkeypatch.setattr(
            h.notification_audience, "jobs",
            lambda: [
                h.notification_audience.NotificationJob(
                    league_id="L1", league_name="One", source="follows", recipients=[],
                ),
                h.notification_audience.NotificationJob(
                    league_id="L2", league_name="Two", source="follows",
                    recipients=list(patched["whitelisted"]),
                ),
            ],
        )

        h.handler({}, context=None)

        assert seen == ["L2"]

    def test_a_league_that_sent_nothing_is_not_reported_as_done(
        self, patched, monkeypatch
    ) -> None:
        from lambdas.notif_weekly_recap import handler as h

        monkeypatch.setattr(
            h.notification_audience, "jobs",
            lambda: [
                h.notification_audience.NotificationJob(
                    league_id="L1", league_name="One", source="follows", recipients=[],
                ),
            ],
        )

        assert h.handler({}, context=None)["body"]["leagues"] == []

    def test_no_leagues_short_circuits(self, patched, monkeypatch) -> None:
        from lambdas.notif_weekly_recap import handler as h

        monkeypatch.setattr(h.notification_audience, "jobs", lambda: [])

        assert h.handler({}, context=None)["body"]["reason"] == "no league to notify"
