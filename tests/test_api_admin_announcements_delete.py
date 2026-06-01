"""
Tests for `api_admin_announcements_delete` (announcements #100).

Endpoint shape: POST /admin/announcements-delete, body { id }

Covers:
  - 403 when caller not admin.
  - 400 on missing id.
  - 404 when row missing.
  - Happy path: soft delete via update is_active=false; audit row
    written with action="announcements.delete" + before/after snapshot.
  - Audit failure does not fail the parent action.
  - Idempotent: deleting an already-inactive row still works + audits.
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
        "path": "/admin/announcements-delete",
        "headers": headers,
        "body": json.dumps(body or {}),
    }


def _existing_row(is_active: bool = True) -> dict[str, Any]:
    return {
        "id": ROW_ID,
        "title": "Going away",
        "body": "soft delete me",
        "priority": "info",
        "expires_at": None,
        "is_active": is_active,
        "display_order": 0,
        "created_at": "2026-05-01T12:00:00+00:00",
        "updated_at": "2026-05-01T12:00:00+00:00",
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_announcements_delete import handler as h

    state: dict[str, Any] = {
        "admin_row": {
            "id": "row-admin",
            "sleeper_user_id": ADMIN_ID,
            "is_admin": True,
            "is_active": True,
        },
        "existing": _existing_row(),
        "delete_calls": [],
        "audit_calls": [],
        "audit_return": {"id": "audit-row-delete"},
        "delete_raises": None,
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _get_row(table: str, column: str, value: str):
        row = state["existing"]
        if row and row.get(column) == value:
            return row
        return None

    def _delete_announcement(announcement_id: str):
        state["delete_calls"].append({"id": announcement_id})
        if state["delete_raises"] is not None:
            raise state["delete_raises"]
        merged = dict(state["existing"] or {})
        merged["is_active"] = False
        state["existing"] = merged
        return merged

    def _write_audit(**kwargs: Any):
        state["audit_calls"].append(kwargs)
        return state["audit_return"]

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "get_row", _get_row)
    monkeypatch.setattr(h, "delete_announcement", _delete_announcement)
    monkeypatch.setattr(h, "write_audit", _write_audit)
    return state


class TestAdminGate:
    def test_non_admin_returns_403(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_announcements_delete import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )
        response = h.handler(_api_event(body={"id": ROW_ID}), context=None)
        assert response["statusCode"] == 403
        assert patched_handler["delete_calls"] == []
        assert patched_handler["audit_calls"] == []


class TestInputValidation:
    def test_missing_id_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_delete import handler as h

        response = h.handler(_api_event(body={}), context=None)
        assert response["statusCode"] == 400
        assert "id" in json.loads(response["body"])["Message"]
        assert patched_handler["delete_calls"] == []

    def test_empty_id_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_delete import handler as h

        response = h.handler(_api_event(body={"id": "  "}), context=None)
        assert response["statusCode"] == 400
        assert patched_handler["delete_calls"] == []


class TestLookupFailure:
    def test_missing_row_returns_404(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_delete import handler as h

        patched_handler["existing"] = None
        response = h.handler(_api_event(body={"id": "ghost"}), context=None)
        assert response["statusCode"] == 404
        assert patched_handler["delete_calls"] == []
        assert patched_handler["audit_calls"] == []


class TestHappyPath:
    def test_soft_delete_writes_audit(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_delete import handler as h

        response = h.handler(_api_event(body={"id": ROW_ID}), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["id"] == ROW_ID
        assert body["row"]["is_active"] is False
        assert body["before"] == {"is_active": True}
        assert body["after"] == {"is_active": False}
        assert body["audit_id"] == "audit-row-delete"

        # delete_announcement called once with the right id.
        assert patched_handler["delete_calls"] == [{"id": ROW_ID}]

        # Audit row recorded.
        audit = patched_handler["audit_calls"][0]
        assert audit["action"] == "announcements.delete"
        assert audit["target_table"] == "league_announcements"
        assert audit["target_id"] == ROW_ID
        assert audit["before"] == {"is_active": True}
        assert audit["after"] == {"is_active": False}
        assert audit["actor_user_id"] == ADMIN_ID

    def test_idempotent_delete_of_already_inactive_row(
        self, patched_handler
    ) -> None:
        """Deleting an already-inactive row still succeeds + audits."""
        from lambdas.api_admin_announcements_delete import handler as h

        patched_handler["existing"] = _existing_row(is_active=False)
        response = h.handler(_api_event(body={"id": ROW_ID}), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        # before reflects the existing (already inactive) state.
        assert body["before"] == {"is_active": False}
        assert body["after"] == {"is_active": False}
        # Audit row still fires.
        assert len(patched_handler["audit_calls"]) == 1

    def test_audit_failure_does_not_break_response(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_announcements_delete import handler as h

        patched_handler["audit_return"] = None
        response = h.handler(_api_event(body={"id": ROW_ID}), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["audit_id"] is None
        # The delete still happened.
        assert len(patched_handler["delete_calls"]) == 1


class TestRaceConditionNotFound:
    def test_delete_raises_not_found_after_snapshot(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_announcements_delete import handler as h
        from lambdas.common.errors import NotFoundError

        patched_handler["delete_raises"] = NotFoundError(
            message=f"announcement not found: {ROW_ID}",
            resource=ROW_ID,
        )
        response = h.handler(_api_event(body={"id": ROW_ID}), context=None)
        assert response["statusCode"] == 404
        # No audit row — the actual delete failed.
        assert patched_handler["audit_calls"] == []
