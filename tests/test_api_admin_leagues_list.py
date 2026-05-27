"""
Tests for `api_admin_leagues_list` (admin-portal F4).

Endpoint shape: GET /admin/leagues-list

Covers:
  - 403 when caller not admin.
  - Happy path: returns the full league rows including inactive ones
    (the iOS LeaguesListView surfaces inactive so admins can
    re-activate). count matches len(leagues).
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
        "path": "/admin/leagues-list",
        "headers": headers,
        "queryStringParameters": {"sleeper_user_id": sleeper_user_id},
    }


def _admin_row() -> dict[str, Any]:
    return {
        "id": "row-admin",
        "sleeper_user_id": ADMIN_ID,
        "is_admin": True,
        "is_active": True,
    }


def _leagues() -> list[dict[str, Any]]:
    return [
        {
            "sleeper_league_id": "L1",
            "league_name": "CLT DYNASTY",
            "is_active": True,
            "is_dynasty": True,
            "has_taxi": True,
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "sleeper_league_id": "L2",
            "league_name": "OLD LEAGUE",
            "is_active": False,  # surfaced even when inactive
            "is_dynasty": False,
            "has_taxi": False,
            "created_at": "2025-01-01T00:00:00Z",
        },
    ]


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_leagues_list import handler as h

    state: dict[str, Any] = {
        "admin_row": _admin_row(),
        "leagues": _leagues(),
        "list_calls": [],
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _list_rows(table: str, **kwargs: Any):
        state["list_calls"].append({"table": table, **kwargs})
        return (state["leagues"], None)

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "list_rows", _list_rows)

    return state


class TestAdminGate:
    def test_non_admin_returns_403(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_leagues_list import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 403
        assert patched_handler["list_calls"] == []


class TestHappyPath:
    def test_returns_all_leagues_active_and_inactive(self, patched_handler) -> None:
        from lambdas.api_admin_leagues_list import handler as h

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["Success"] is True
        assert parsed["count"] == 2
        assert len(parsed["leagues"]) == 2

        # The inactive league is present.
        ids = {row["sleeper_league_id"] for row in parsed["leagues"]}
        assert ids == {"L1", "L2"}
        active_flags = {row["sleeper_league_id"]: row["is_active"] for row in parsed["leagues"]}
        assert active_flags == {"L1": True, "L2": False}

        # list_rows called once against the right table.
        assert len(patched_handler["list_calls"]) == 1
        assert patched_handler["list_calls"][0]["table"] == "whitelisted_leagues"

    def test_empty_result_returns_zero_count(self, patched_handler) -> None:
        from lambdas.api_admin_leagues_list import handler as h

        patched_handler["leagues"] = []

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["count"] == 0
        assert parsed["leagues"] == []
