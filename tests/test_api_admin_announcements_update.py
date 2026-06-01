"""
Tests for `api_admin_announcements_update` (announcements #100).

Endpoint shape: POST /admin/announcements-update, body
  { id, fields: { title?, body?, priority?, expires_at?, is_active?,
                  display_order? } }

Covers:
  - 403 when caller not admin.
  - 400 on missing id, empty fields, unknown field.
  - 400 on invalid priority, non-bool is_active, non-int display_order.
  - 404 when row missing.
  - Happy path: update + write_audit called once each; response carries
    before/after + audit_id.
  - Audit failure does not fail the parent action.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
ROW_ID = "00000000-0000-0000-0000-000000000001"


def _api_event(
    *,
    body: dict[str, Any] | None = None,
    sleeper_user_id: str | None = ADMIN_ID,
) -> dict[str, Any]:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "POST",
        "path": "/admin/announcements-update",
        "headers": headers,
        "body": json.dumps(body or {}),
    }


def _existing_row() -> dict[str, Any]:
    return {
        "id": ROW_ID,
        "title": "Old title",
        "body": "Old body",
        "priority": "info",
        "expires_at": None,
        "is_active": True,
        "display_order": 0,
        "created_at": "2026-05-01T12:00:00+00:00",
        "updated_at": "2026-05-01T12:00:00+00:00",
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_announcements_update import handler as h

    state: dict[str, Any] = {
        "admin_row": {
            "id": "row-admin",
            "sleeper_user_id": ADMIN_ID,
            "is_admin": True,
            "is_active": True,
        },
        "existing": _existing_row(),
        "update_calls": [],
        "audit_calls": [],
        "audit_return": {"id": "audit-row-update"},
        "update_raises": None,
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _get_row(table: str, column: str, value: str):
        row = state["existing"]
        if row and row.get(column) == value:
            return row
        return None

    def _update_announcement(announcement_id: str, fields: dict[str, Any]):
        state["update_calls"].append({"id": announcement_id, "fields": fields})
        if state["update_raises"] is not None:
            raise state["update_raises"]
        merged = dict(state["existing"] or {})
        merged.update(fields)
        state["existing"] = merged
        return merged

    def _write_audit(**kwargs: Any):
        state["audit_calls"].append(kwargs)
        return state["audit_return"]

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "get_row", _get_row)
    monkeypatch.setattr(h, "update_announcement", _update_announcement)
    monkeypatch.setattr(h, "write_audit", _write_audit)
    return state


class TestAdminGate:
    def test_non_admin_returns_403(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_announcements_update import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )
        response = h.handler(
            _api_event(body={"id": ROW_ID, "fields": {"title": "X"}}),
            context=None,
        )
        assert response["statusCode"] == 403
        assert patched_handler["update_calls"] == []
        assert patched_handler["audit_calls"] == []


class TestInputValidation:
    def test_missing_id_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(
            _api_event(body={"fields": {"title": "X"}}), context=None
        )
        assert response["statusCode"] == 400
        assert "id" in json.loads(response["body"])["Message"]
        assert patched_handler["update_calls"] == []

    def test_missing_fields_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(_api_event(body={"id": ROW_ID}), context=None)
        assert response["statusCode"] == 400
        assert "fields" in json.loads(response["body"])["Message"]

    def test_empty_fields_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(
            _api_event(body={"id": ROW_ID, "fields": {}}), context=None
        )
        assert response["statusCode"] == 400

    def test_unknown_field_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(
            _api_event(
                body={"id": ROW_ID, "fields": {"created_at": "now"}}
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        assert "created_at" in json.loads(response["body"])["Message"]
        assert patched_handler["update_calls"] == []

    def test_invalid_priority_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(
            _api_event(
                body={"id": ROW_ID, "fields": {"priority": "urgent"}}
            ),
            context=None,
        )
        assert response["statusCode"] == 400

    def test_empty_title_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(
            _api_event(body={"id": ROW_ID, "fields": {"title": "  "}}),
            context=None,
        )
        assert response["statusCode"] == 400

    def test_non_bool_is_active_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(
            _api_event(body={"id": ROW_ID, "fields": {"is_active": 1}}),
            context=None,
        )
        assert response["statusCode"] == 400

    def test_non_int_display_order_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(
            _api_event(
                body={"id": ROW_ID, "fields": {"display_order": "abc"}}
            ),
            context=None,
        )
        assert response["statusCode"] == 400


class TestLookupFailure:
    def test_missing_row_returns_404(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        patched_handler["existing"] = None
        response = h.handler(
            _api_event(body={"id": "ghost", "fields": {"title": "X"}}),
            context=None,
        )
        assert response["statusCode"] == 404
        assert patched_handler["update_calls"] == []
        assert patched_handler["audit_calls"] == []


class TestHappyPath:
    def test_title_update_writes_audit(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(
            _api_event(
                body={"id": ROW_ID, "fields": {"title": "New title"}}
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["id"] == ROW_ID
        assert body["row"]["title"] == "New title"
        assert body["before"] == {"title": "Old title"}
        assert body["after"] == {"title": "New title"}
        assert body["audit_id"] == "audit-row-update"

        # update_announcement called once.
        assert len(patched_handler["update_calls"]) == 1
        # Audit row carries the before/after diff.
        audit = patched_handler["audit_calls"][0]
        assert audit["action"] == "announcements.update"
        assert audit["target_table"] == "league_announcements"
        assert audit["target_id"] == ROW_ID
        assert audit["before"] == {"title": "Old title"}
        assert audit["after"] == {"title": "New title"}
        assert audit["actor_user_id"] == ADMIN_ID

    def test_multi_field_update(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "id": ROW_ID,
                    "fields": {
                        "priority": "critical",
                        "is_active": False,
                    },
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["before"] == {"priority": "info", "is_active": True}
        assert body["after"] == {"priority": "critical", "is_active": False}

    def test_audit_failure_does_not_break_response(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_announcements_update import handler as h

        patched_handler["audit_return"] = None
        response = h.handler(
            _api_event(body={"id": ROW_ID, "fields": {"title": "X"}}),
            context=None,
        )
        assert response["statusCode"] == 200
        assert json.loads(response["body"])["audit_id"] is None
        assert len(patched_handler["update_calls"]) == 1


class TestRaceConditionNotFound:
    """If the row was deleted between the get_row snapshot fetch and
    the update call (race condition), update_announcement raises
    NotFoundError which the handler turns into a clean 404."""

    def test_update_raises_not_found_after_snapshot(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_announcements_update import handler as h
        from lambdas.common.errors import NotFoundError

        patched_handler["update_raises"] = NotFoundError(
            message=f"announcement not found: {ROW_ID}",
            resource=ROW_ID,
        )
        response = h.handler(
            _api_event(body={"id": ROW_ID, "fields": {"title": "X"}}),
            context=None,
        )
        assert response["statusCode"] == 404
        # No audit row because the actual update failed.
        assert patched_handler["audit_calls"] == []
