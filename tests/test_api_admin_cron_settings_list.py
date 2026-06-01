"""
Tests for `api_admin_cron_settings_list` (admin-cron-settings).

Endpoint shape: GET /admin/cron-settings-list

Covers:
  - 403 when caller not admin.
  - Happy path returns the rows passed back by list_cron_settings,
    with table_missing=false.
  - Empty result triggers table_missing=true (the migration probably
    hasn't been applied yet — helper degrades to [] on Supabase
    failure).
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"


def _api_event(*, sleeper_user_id: str | None = ADMIN_ID) -> dict[str, Any]:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "GET",
        "path": "/admin/cron-settings-list",
        "headers": headers,
        "queryStringParameters": {},
    }


def _seed_rows() -> list[dict[str, Any]]:
    return [
        {
            "cron_key": "notif_ai_review_weekly",
            "enabled": True,
            "test_mode": False,
            "description": "AI weekly newsletter — Tue 2pm ET",
            "updated_at": "2026-06-01T10:00:00Z",
        },
        {
            "cron_key": "notif_close_game_alert",
            "enabled": True,
            "test_mode": False,
            "description": "Close-game alerts — Sun + Mon 8pm ET",
            "updated_at": "2026-06-01T10:00:00Z",
        },
        {
            "cron_key": "notif_lineup_not_set",
            "enabled": True,
            "test_mode": False,
            "description": "Lineup-not-set alerts — Sun 11am ET",
            "updated_at": "2026-06-01T10:00:00Z",
        },
        {
            "cron_key": "notif_weekly_recap",
            "enabled": True,
            "test_mode": False,
            "description": "Weekly matchup recap — Tue 9am ET",
            "updated_at": "2026-06-01T10:00:00Z",
        },
        {
            "cron_key": "notif_worldcup_movement",
            "enabled": True,
            "test_mode": False,
            "description": "World Cup transitions — Tue 10am ET",
            "updated_at": "2026-06-01T10:00:00Z",
        },
    ]


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_cron_settings_list import handler as h

    state: dict[str, Any] = {
        "admin_row": {
            "id": "row-admin",
            "sleeper_user_id": ADMIN_ID,
            "is_admin": True,
            "is_active": True,
        },
        "rows": _seed_rows(),
        "list_calls": 0,
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _list_cron_settings():
        state["list_calls"] += 1
        return list(state["rows"])

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "list_cron_settings", _list_cron_settings)

    return state


class TestAdminGate:
    def test_non_admin_returns_403(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_cron_settings_list import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )
        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 403
        # Helper should NOT have been called.
        assert patched_handler["list_calls"] == 0


class TestHappyPath:
    def test_returns_all_seeded_rows(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_list import handler as h

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["count"] == 5
        assert body["table_missing"] is False
        keys = [r["cron_key"] for r in body["rows"]]
        assert "notif_weekly_recap" in keys
        assert "notif_ai_review_weekly" in keys
        assert "notif_close_game_alert" in keys
        assert "notif_lineup_not_set" in keys
        assert "notif_worldcup_movement" in keys

    def test_payload_carries_all_columns(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_list import handler as h

        body = json.loads(h.handler(_api_event(), context=None)["body"])
        row = body["rows"][0]
        assert {"cron_key", "enabled", "test_mode", "description", "updated_at"}.issubset(
            row.keys()
        )


class TestTableMissingGracefulFallback:
    """If the migration hasn't been applied yet, list_cron_settings
    degrades to [] on Supabase failure. The endpoint must mark
    table_missing=true so iOS renders an empty state."""

    def test_empty_rows_marks_table_missing(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_list import handler as h

        patched_handler["rows"] = []
        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["count"] == 0
        assert body["rows"] == []
        assert body["table_missing"] is True
