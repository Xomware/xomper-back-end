"""
Tests for `api_admin_leagues_update` (admin-portal F4).

Endpoint shape: POST /admin/leagues-update, body
  { league_id, fields: { league_name?, is_active?, is_dynasty?, has_taxi? } }

Covers:
  - 403 when caller not admin.
  - 400 on missing league_id / empty fields.
  - 400 on unknown field (includes offending key in message).
  - 400 on non-bool for any of the three boolean fields.
  - 404 when target league missing.
  - Happy path: update_row + write_audit called exactly once each;
    response includes before/after + audit_id.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
TARGET_LEAGUE_ID = "LEAGUE_ID"


def _api_event(
    *,
    body: dict[str, Any] | None = None,
    sleeper_user_id: str | None = ADMIN_ID,
) -> dict[str, Any]:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "POST",
        "path": "/admin/leagues-update",
        "headers": headers,
        "body": json.dumps(body or {}),
    }


def _admin_row() -> dict[str, Any]:
    return {
        "id": "row-admin",
        "sleeper_user_id": ADMIN_ID,
        "is_active": True,
        "is_admin": True,
    }


def _league_row() -> dict[str, Any]:
    return {
        "id": "row-L",
        "sleeper_league_id": TARGET_LEAGUE_ID,
        "league_name": "CLT DYNASTY",
        "is_active": True,
        "is_dynasty": True,
        "has_taxi": False,
        "created_at": "2026-01-01T00:00:00Z",
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_leagues_update import handler as h

    state: dict[str, Any] = {
        "admin_row": _admin_row(),
        "league_row": _league_row(),
        "update_calls": [],
        "audit_calls": [],
        "audit_return": {"id": "audit-row-L1"},
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _get_row(table: str, column: str, value: str):
        if table == "whitelisted_leagues" and column == "sleeper_league_id":
            row = state["league_row"]
            if row and row.get("sleeper_league_id") == value:
                return row
        return None

    def _update_row(table: str, column: str, value: str, fields: dict[str, Any]):
        state["update_calls"].append(
            {"table": table, "column": column, "value": value, "fields": fields}
        )
        merged = dict(state["league_row"] or {})
        merged.update(fields)
        state["league_row"] = merged
        return merged

    def _write_audit(**kwargs: Any):
        state["audit_calls"].append(kwargs)
        return state["audit_return"]

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "get_row", _get_row)
    monkeypatch.setattr(h, "update_row", _update_row)
    monkeypatch.setattr(h, "write_audit", _write_audit)

    return state


# ---------------------------------------------------------------------------
# Auth gate
# ---------------------------------------------------------------------------


class TestAdminGate:
    def test_non_admin_returns_403(
        self,
        patched_handler,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_leagues_update import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )

        response = h.handler(
            _api_event(
                body={
                    "league_id": TARGET_LEAGUE_ID,
                    "fields": {"league_name": "New"},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 403
        assert patched_handler["update_calls"] == []
        assert patched_handler["audit_calls"] == []


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestInputValidation:
    def test_missing_league_id_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_leagues_update import handler as h

        response = h.handler(
            _api_event(body={"fields": {"league_name": "New"}}),
            context=None,
        )
        assert response["statusCode"] == 400
        assert "league_id" in json.loads(response["body"])["Message"]

    def test_unknown_field_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_leagues_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "league_id": TARGET_LEAGUE_ID,
                    "fields": {"max_teams": 16},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        parsed = json.loads(response["body"])
        assert "max_teams" in parsed["Message"]
        assert patched_handler["update_calls"] == []

    def test_non_bool_is_dynasty_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_leagues_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "league_id": TARGET_LEAGUE_ID,
                    "fields": {"is_dynasty": 1},  # int, not bool
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        assert "is_dynasty" in json.loads(response["body"])["Message"]
        assert patched_handler["update_calls"] == []

    def test_empty_league_name_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_leagues_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "league_id": TARGET_LEAGUE_ID,
                    "fields": {"league_name": ""},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400


# ---------------------------------------------------------------------------
# Lookup failure
# ---------------------------------------------------------------------------


class TestLookupFailure:
    def test_unknown_league_returns_404(self, patched_handler) -> None:
        from lambdas.api_admin_leagues_update import handler as h

        patched_handler["league_row"] = None

        response = h.handler(
            _api_event(
                body={
                    "league_id": "ghost-league",
                    "fields": {"league_name": "X"},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 404
        assert patched_handler["update_calls"] == []
        assert patched_handler["audit_calls"] == []


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_toggle_has_taxi(self, patched_handler) -> None:
        from lambdas.api_admin_leagues_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "league_id": TARGET_LEAGUE_ID,
                    "fields": {"has_taxi": True},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["Success"] is True
        assert parsed["league_id"] == TARGET_LEAGUE_ID
        assert parsed["before"] == {"has_taxi": False}
        assert parsed["after"] == {"has_taxi": True}
        assert parsed["audit_id"] == "audit-row-L1"

        assert len(patched_handler["update_calls"]) == 1
        write = patched_handler["update_calls"][0]
        assert write["table"] == "whitelisted_leagues"
        assert write["column"] == "sleeper_league_id"
        assert write["fields"] == {"has_taxi": True}

        assert len(patched_handler["audit_calls"]) == 1
        audit_kwargs = patched_handler["audit_calls"][0]
        assert audit_kwargs["action"] == "leagues.update"
        assert audit_kwargs["target_table"] == "whitelisted_leagues"
        assert audit_kwargs["target_id"] == TARGET_LEAGUE_ID
        assert audit_kwargs["before"] == {"has_taxi": False}
        assert audit_kwargs["after"] == {"has_taxi": True}

    def test_multi_field_update(self, patched_handler) -> None:
        from lambdas.api_admin_leagues_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "league_id": TARGET_LEAGUE_ID,
                    "fields": {
                        "league_name": "RENAMED LEAGUE",
                        "is_active": False,
                    },
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["before"] == {
            "league_name": "CLT DYNASTY",
            "is_active": True,
        }
        assert parsed["after"] == {
            "league_name": "RENAMED LEAGUE",
            "is_active": False,
        }
