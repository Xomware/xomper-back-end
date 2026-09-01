"""
Tests for the admin-cron-settings gate inside `notif_worldcup_movement`.

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


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    from lambdas.notif_worldcup_movement import handler as h

    state: dict[str, Any] = {
        "cron_setting": {
            "cron_key": "notif_worldcup_movement",
            "enabled": True,
            "test_mode": False,
            "description": "Worldcup",
        },
        "whitelisted": [_user_row(ADMIN_ID, idx=0), _user_row("user-1", idx=1)],
        "send_push_calls": [],
        # Simulated transitions: both users clinched their division.
        "transitions": [
            {"user_id": ADMIN_ID, "kind": "status", "to": "clinched", "division": 1},
            {"user_id": "user-1", "kind": "status", "to": "clinched", "division": 2},
        ],
        "league_row": {
            "league_id": "L1",
            "has_taxi": True,
            "is_dynasty": True,
            "season": "2026",
        },
    }

    def _get_cron_setting(cron_key: str):
        return dict(state["cron_setting"])

    def _send_push_to_users(user_ids, title, body, category, data):
        state["send_push_calls"].append({"user_ids": list(user_ids)})

    # The worldcup handler does a LOT of helper invocation — stub the
    # internals it uses so the test focuses only on the gate logic.
    monkeypatch.setattr(h, "get_cron_setting", _get_cron_setting)
    # The handler asks notification_audience for the work list now, and
    # eligibility comes from Sleeper rather than whitelist columns.
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
    monkeypatch.setattr(
        h,
        "get_sleeper_league",
        lambda lid: {"settings": {"type": 2, "taxi_slots": 2}},
    )
    monkeypatch.setattr(h, "send_push_to_users", _send_push_to_users)

    # Replace the chain + standings + diff pipeline with deterministic
    # stubs so we test only the gate + recipient filter.
    monkeypatch.setattr(
        h,
        "get_league_chain",
        lambda head_id, fetch_league_fn=None: [
            {"league_id": "L1", "status": "in_season", "season": "2026"},
        ],
    )
    monkeypatch.setattr(h, "_gather_chain_matchups", lambda chain: [])
    monkeypatch.setattr(h, "compute_division_standings", lambda matchups, names: [])
    monkeypatch.setattr(h, "clinch_for_division", lambda teams, games_remaining=6: None)
    monkeypatch.setattr(h, "status_map_from_standings", lambda standings: {})
    monkeypatch.setattr(h, "diff_snapshots", lambda current, prev: state["transitions"])
    monkeypatch.setattr(h, "_read_snapshot", lambda lid, season, week: {})
    monkeypatch.setattr(h, "_write_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(h, "division_name_map_from_league", lambda league: {1: "AFC", 2: "NFC"})

    return state


class TestDisabledShortCircuit:
    def test_disabled_returns_skipped(self, patched) -> None:
        from lambdas.notif_worldcup_movement.handler import handler

        patched["cron_setting"]["enabled"] = False
        response = handler({}, context=None)
        assert response["statusCode"] == 200
        body = response["body"]
        assert body["skipped"] is True
        assert patched["send_push_calls"] == []


class TestTestModeFiltering:
    def test_test_mode_filters_to_admin_only(self, patched) -> None:
        from lambdas.notif_worldcup_movement.handler import handler

        patched["cron_setting"]["test_mode"] = True
        handler({"week": 5}, context=None)
        recipients = {
            uid
            for call in patched["send_push_calls"]
            for uid in call["user_ids"]
        }
        assert recipients == {ADMIN_ID}


class TestDefaultPassthrough:
    def test_default_unfiltered_fan_out(self, patched) -> None:
        from lambdas.notif_worldcup_movement.handler import handler

        handler({"week": 5}, context=None)
        recipients = {
            uid
            for call in patched["send_push_calls"]
            for uid in call["user_ids"]
        }
        assert ADMIN_ID in recipients
        assert "user-1" in recipients


class TestEligibilityAndFanOut:
    """This cron walks the whole league chain and every regular-season week,
    so what it declines to run for matters more than what it runs."""

    def _job(self, h, lid, recipients):
        return h.notification_audience.NotificationJob(
            league_id=lid, league_name=lid, source="follows",
            recipients=list(recipients),
        )

    def test_a_redraft_league_is_not_eligible(self, patched, monkeypatch) -> None:
        from lambdas.notif_worldcup_movement import handler as h

        walked = []
        monkeypatch.setattr(
            h, "get_league_chain",
            lambda head_id, fetch_league_fn=None: walked.append(head_id) or [],
        )
        # type 0 is redraft; the World Cup format is dynasty-only.
        monkeypatch.setattr(
            h, "get_sleeper_league",
            lambda lid: {"settings": {"type": 0, "taxi_slots": 2}},
        )
        monkeypatch.setattr(
            h.notification_audience, "jobs",
            lambda: [self._job(h, "L1", patched["whitelisted"])],
        )

        h.handler({}, context=None)

        assert walked == []

    def test_a_dynasty_league_without_taxi_is_not_eligible(
        self, patched, monkeypatch
    ) -> None:
        from lambdas.notif_worldcup_movement import handler as h

        walked = []
        monkeypatch.setattr(
            h, "get_league_chain",
            lambda head_id, fetch_league_fn=None: walked.append(head_id) or [],
        )
        monkeypatch.setattr(
            h, "get_sleeper_league",
            lambda lid: {"settings": {"type": 2, "taxi_slots": 0}},
        )
        monkeypatch.setattr(
            h.notification_audience, "jobs",
            lambda: [self._job(h, "L1", patched["whitelisted"])],
        )

        h.handler({}, context=None)

        assert walked == []

    def test_a_league_with_no_recipients_is_never_even_looked_up(
        self, patched, monkeypatch
    ) -> None:
        from lambdas.notif_worldcup_movement import handler as h

        looked_up = []
        monkeypatch.setattr(
            h, "get_sleeper_league",
            lambda lid: looked_up.append(lid) or {"settings": {"type": 2, "taxi_slots": 2}},
        )
        monkeypatch.setattr(
            h.notification_audience, "jobs", lambda: [self._job(h, "L1", [])],
        )

        h.handler({}, context=None)

        # The recipient check precedes the eligibility lookup, so an
        # unwatched league costs zero Sleeper calls.
        assert looked_up == []

    def test_no_leagues_short_circuits(self, patched, monkeypatch) -> None:
        from lambdas.notif_worldcup_movement import handler as h

        monkeypatch.setattr(h.notification_audience, "jobs", lambda: [])

        assert h.handler({}, context=None)["body"]["reason"] == "no league to notify"
