"""
Tests for `api_admin_announcements_list` (announcements #100 admin).

Endpoint shape: GET /admin/announcements-list

Covers:
  - 403 when caller not admin.
  - Happy path returns rows + table_missing=false.
  - Empty result triggers table_missing=true (migration not applied).
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
        "path": "/admin/announcements-list",
        "headers": headers,
        "queryStringParameters": {},
    }


def _seed_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": "00000000-0000-0000-0000-000000000001",
            "title": "2026 Rookie Draft",
            "body": "July 6, 2026 — 6:30pm ET sharp.",
            "priority": "critical",
            "expires_at": "2026-07-07T00:00:00+00:00",
            "is_active": True,
            "display_order": 0,
            "created_at": "2026-05-01T12:00:00+00:00",
            "updated_at": "2026-05-01T12:00:00+00:00",
        },
        {
            "id": "00000000-0000-0000-0000-000000000002",
            "title": "2026 Season Start",
            "body": "Week 1 kicks off Sunday September 8, 2026.",
            "priority": "info",
            "expires_at": "2026-09-09T00:00:00+00:00",
            "is_active": True,
            "display_order": 1,
            "created_at": "2026-05-01T12:00:00+00:00",
            "updated_at": "2026-05-01T12:00:00+00:00",
        },
        {
            "id": "00000000-0000-0000-0000-000000000003",
            "title": "Old expired entry",
            "body": "soft deleted",
            "priority": "info",
            "expires_at": None,
            "is_active": False,
            "display_order": 2,
            "created_at": "2026-04-01T12:00:00+00:00",
            "updated_at": "2026-05-01T12:00:00+00:00",
        },
    ]


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_announcements_list import handler as h

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

    def _list_all():
        state["list_calls"] += 1
        return list(state["rows"])

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "list_all", _list_all)

    return state


class TestAdminGate:
    def test_non_admin_returns_403(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_announcements_list import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )
        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 403
        assert patched_handler["list_calls"] == 0


class TestHappyPath:
    def test_returns_all_rows_including_inactive(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_list import handler as h

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["count"] == 3
        assert body["table_missing"] is False
        ids = [r["id"] for r in body["rows"]]
        assert "00000000-0000-0000-0000-000000000003" in ids  # inactive row present

    def test_payload_carries_all_columns(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_list import handler as h

        body = json.loads(h.handler(_api_event(), context=None)["body"])
        row = body["rows"][0]
        assert {
            "id",
            "title",
            "body",
            "priority",
            "expires_at",
            "is_active",
            "display_order",
            "created_at",
            "updated_at",
        }.issubset(row.keys())


class TestTableMissingGracefulFallback:
    def test_empty_rows_marks_table_missing(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_list import handler as h

        patched_handler["rows"] = []
        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["count"] == 0
        assert body["rows"] == []
        assert body["table_missing"] is True
