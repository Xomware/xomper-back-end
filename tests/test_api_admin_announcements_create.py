"""
Tests for `api_admin_announcements_create` (announcements #100).

Endpoint shape: POST /admin/announcements-create

Covers:
  - 403 when caller not admin.
  - 400 on missing/empty title or body.
  - 400 on invalid priority.
  - 400 on non-bool is_active.
  - 400 on non-int display_order.
  - Happy path: create + write_audit called once each; response carries
    the inserted row + audit_id.
  - Defaults applied when optional fields are omitted.
  - Audit failure does not fail the parent action (best-effort).
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
INSERTED_ID = "11111111-2222-3333-4444-555555555555"


def _api_event(
    *,
    body: dict[str, Any] | None = None,
    sleeper_user_id: str | None = ADMIN_ID,
) -> dict[str, Any]:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "POST",
        "path": "/admin/announcements-create",
        "headers": headers,
        "body": json.dumps(body or {}),
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_announcements_create import handler as h

    state: dict[str, Any] = {
        "admin_row": {
            "id": "row-admin",
            "sleeper_user_id": ADMIN_ID,
            "is_admin": True,
            "is_active": True,
        },
        "create_calls": [],
        "audit_calls": [],
        "audit_return": {"id": "audit-row-create"},
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _create(
        title: str,
        body: str,
        priority: str = "info",
        expires_at: str | None = None,
        is_active: bool = True,
        display_order: int = 0,
    ):
        state["create_calls"].append(
            {
                "title": title,
                "body": body,
                "priority": priority,
                "expires_at": expires_at,
                "is_active": is_active,
                "display_order": display_order,
            }
        )
        return {
            "id": INSERTED_ID,
            "title": title,
            "body": body,
            "priority": priority,
            "expires_at": expires_at,
            "is_active": is_active,
            "display_order": display_order,
            "created_at": "2026-06-01T12:00:00+00:00",
            "updated_at": "2026-06-01T12:00:00+00:00",
        }

    def _write_audit(**kwargs: Any):
        state["audit_calls"].append(kwargs)
        return state["audit_return"]

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "create_announcement", _create)
    monkeypatch.setattr(h, "write_audit", _write_audit)
    return state


class TestAdminGate:
    def test_non_admin_returns_403(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_announcements_create import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )
        response = h.handler(
            _api_event(body={"title": "X", "body": "Y"}),
            context=None,
        )
        assert response["statusCode"] == 403
        assert patched_handler["create_calls"] == []
        assert patched_handler["audit_calls"] == []


class TestInputValidation:
    def test_missing_title_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_create import handler as h

        response = h.handler(_api_event(body={"body": "Y"}), context=None)
        assert response["statusCode"] == 400
        assert "title" in json.loads(response["body"])["Message"]
        assert patched_handler["create_calls"] == []

    def test_empty_title_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_create import handler as h

        response = h.handler(
            _api_event(body={"title": "   ", "body": "Y"}),
            context=None,
        )
        assert response["statusCode"] == 400
        assert patched_handler["create_calls"] == []

    def test_missing_body_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_create import handler as h

        response = h.handler(_api_event(body={"title": "X"}), context=None)
        assert response["statusCode"] == 400
        assert "body" in json.loads(response["body"])["Message"]
        assert patched_handler["create_calls"] == []

    def test_invalid_priority_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_create import handler as h

        response = h.handler(
            _api_event(body={"title": "X", "body": "Y", "priority": "urgent"}),
            context=None,
        )
        assert response["statusCode"] == 400
        assert "priority" in json.loads(response["body"])["Message"].lower()
        assert patched_handler["create_calls"] == []

    def test_non_bool_is_active_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_create import handler as h

        response = h.handler(
            _api_event(body={"title": "X", "body": "Y", "is_active": "maybe"}),
            context=None,
        )
        assert response["statusCode"] == 400
        assert patched_handler["create_calls"] == []

    def test_non_int_display_order_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_create import handler as h

        response = h.handler(
            _api_event(
                body={"title": "X", "body": "Y", "display_order": "abc"}
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        assert patched_handler["create_calls"] == []


class TestHappyPath:
    def test_happy_path_writes_row_and_audit(self, patched_handler) -> None:
        from lambdas.api_admin_announcements_create import handler as h

        response = h.handler(
            _api_event(
                body={
                    "title": "New",
                    "body": "Body",
                    "priority": "critical",
                    "expires_at": "2026-07-07T00:00:00Z",
                    "is_active": True,
                    "display_order": 3,
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["row"]["id"] == INSERTED_ID
        assert body["row"]["title"] == "New"
        assert body["audit_id"] == "audit-row-create"

        # create called once with expected args.
        assert len(patched_handler["create_calls"]) == 1
        call = patched_handler["create_calls"][0]
        assert call["title"] == "New"
        assert call["body"] == "Body"
        assert call["priority"] == "critical"
        assert call["expires_at"] == "2026-07-07T00:00:00Z"
        assert call["is_active"] is True
        assert call["display_order"] == 3

        # Audit row recorded with action=announcements.create.
        assert len(patched_handler["audit_calls"]) == 1
        audit = patched_handler["audit_calls"][0]
        assert audit["action"] == "announcements.create"
        assert audit["target_table"] == "league_announcements"
        assert audit["target_id"] == INSERTED_ID
        assert audit["actor_user_id"] == ADMIN_ID
        assert audit["before"] is None
        assert audit["after"]["title"] == "New"
        assert audit["after"]["priority"] == "critical"

    def test_defaults_applied_when_optional_fields_omitted(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_announcements_create import handler as h

        response = h.handler(
            _api_event(body={"title": "T", "body": "B"}),
            context=None,
        )
        assert response["statusCode"] == 200
        call = patched_handler["create_calls"][0]
        assert call["priority"] == "info"
        assert call["expires_at"] is None
        assert call["is_active"] is True
        assert call["display_order"] == 0

    def test_audit_failure_does_not_break_response(
        self, patched_handler
    ) -> None:
        """Best-effort audit write — when write_audit returns None,
        the parent action still succeeds and returns 200."""
        from lambdas.api_admin_announcements_create import handler as h

        patched_handler["audit_return"] = None
        response = h.handler(
            _api_event(body={"title": "T", "body": "B"}),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["audit_id"] is None
        # The create still happened.
        assert len(patched_handler["create_calls"]) == 1


class TestTableMissingGracefulPath:
    """When the Supabase table doesn't exist, the store's `create`
    raises (insert paths DO surface errors, unlike read paths). The
    @handle_errors decorator turns it into a 500/502 structured error
    response so the admin client surfaces the failure."""

    def test_create_raises_propagates_as_error_response(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_announcements_create import handler as h
        from lambdas.common.errors import SleeperAPIError

        def _create_raising(*_args: Any, **_kwargs: Any):
            raise SleeperAPIError(
                message="HTTP 404 — relation does not exist",
                function="supabase_helper._post",
            )

        monkeypatch.setattr(h, "create_announcement", _create_raising)
        response = h.handler(
            _api_event(body={"title": "T", "body": "B"}),
            context=None,
        )
        # SleeperAPIError → 502 via XomperError.to_response.
        assert response["statusCode"] in (500, 502)
        # No audit row should fire because the create failed.
        assert patched_handler["audit_calls"] == []
