"""
Tests for `api_admin_users_update` (admin-portal F4).

Endpoint shape: POST /admin/users-update, body
  { user_id, fields: { email?, display_name?, is_admin?, is_active? } }

Covers:
  - 403 when caller not admin.
  - 400 on missing user_id / empty fields.
  - 400 on unknown field name (offending key in message).
  - 400 on invalid email.
  - 400 on non-bool for is_admin / is_active.
  - Bool coercion accepts "true" / "false" strings + real bools.
  - 404 when target user missing.
  - Happy path: update_row + write_audit called once each;
    response includes audit_id + before snapshot.
  - Audit failures do not fail the parent action (best-effort write).
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
TARGET_USER_ID = "u7"


def _api_event(
    *,
    body: dict[str, Any] | None = None,
    sleeper_user_id: str | None = ADMIN_ID,
) -> dict[str, Any]:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "POST",
        "path": "/admin/users-update",
        "headers": headers,
        "body": json.dumps(body or {}),
    }


def _admin_row() -> dict[str, Any]:
    return {
        "id": "row-admin",
        "sleeper_user_id": ADMIN_ID,
        "email": "admin@example.com",
        "display_name": "Admin Dom",
        "is_active": True,
        "is_admin": True,
    }


def _target_user_row() -> dict[str, Any]:
    return {
        "id": "row-u7",
        "sleeper_user_id": TARGET_USER_ID,
        "email": "old@example.com",
        "display_name": "Old Name",
        "is_active": True,
        "is_admin": False,
        "created_at": "2026-04-01T12:00:00Z",
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    """Wire every external seam. Returns a mutable state dict so tests
    can mutate (e.g. drop the target user to trigger 404)."""
    from lambdas.api_admin_users_update import handler as h

    state: dict[str, Any] = {
        "admin_row": _admin_row(),
        "target_row": _target_user_row(),
        "update_calls": [],
        "audit_calls": [],
        "audit_return": {"id": "audit-row-xyz"},
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _get_row(table: str, column: str, value: str):
        if table == "whitelisted_users" and column == "sleeper_user_id":
            row = state["target_row"]
            if row and row.get("sleeper_user_id") == value:
                return row
        return None

    def _update_row(table: str, column: str, value: str, fields: dict[str, Any]):
        state["update_calls"].append(
            {"table": table, "column": column, "value": value, "fields": fields}
        )
        # Echo the merged row.
        merged = dict(state["target_row"] or {})
        merged.update(fields)
        state["target_row"] = merged
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
        from lambdas.api_admin_users_update import handler as h
        from lambdas.common.admin_gate import NotAdmin

        def _raise(event, body=None):
            raise NotAdmin("not authorized")

        monkeypatch.setattr(h, "require_admin", _raise)

        response = h.handler(
            _api_event(
                body={
                    "user_id": TARGET_USER_ID,
                    "fields": {"display_name": "New Name"},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 403
        body = json.loads(response["body"])
        assert body["Success"] is False
        assert patched_handler["update_calls"] == []
        assert patched_handler["audit_calls"] == []


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestInputValidation:
    def test_missing_user_id_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(body={"fields": {"email": "x@y.com"}}),
            context=None,
        )
        assert response["statusCode"] == 400
        assert "user_id" in json.loads(response["body"])["Message"]
        assert patched_handler["update_calls"] == []

    def test_missing_fields_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(body={"user_id": TARGET_USER_ID}),
            context=None,
        )
        assert response["statusCode"] == 400
        assert "fields" in json.loads(response["body"])["Message"]
        assert patched_handler["update_calls"] == []

    def test_empty_fields_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(body={"user_id": TARGET_USER_ID, "fields": {}}),
            context=None,
        )
        assert response["statusCode"] == 400
        assert patched_handler["update_calls"] == []

    def test_unknown_field_returns_400_with_key(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "user_id": TARGET_USER_ID,
                    "fields": {"is_super_user": True},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        parsed = json.loads(response["body"])
        assert "is_super_user" in parsed["Message"]
        assert patched_handler["update_calls"] == []

    def test_invalid_email_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "user_id": TARGET_USER_ID,
                    "fields": {"email": "not-an-email"},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        parsed = json.loads(response["body"])
        assert "email" in parsed["Message"].lower()
        assert patched_handler["update_calls"] == []

    def test_empty_display_name_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "user_id": TARGET_USER_ID,
                    "fields": {"display_name": "   "},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        assert patched_handler["update_calls"] == []

    def test_non_bool_is_admin_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "user_id": TARGET_USER_ID,
                    "fields": {"is_admin": "maybe"},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        assert "is_admin" in json.loads(response["body"])["Message"]
        assert patched_handler["update_calls"] == []


# ---------------------------------------------------------------------------
# Bool coercion
# ---------------------------------------------------------------------------


class TestBoolCoercion:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("false", False),
            ("TRUE", True),
            ("False", False),
        ],
    )
    def test_is_active_coerces(self, patched_handler, raw, expected) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "user_id": TARGET_USER_ID,
                    "fields": {"is_active": raw},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        write = patched_handler["update_calls"][0]
        assert write["fields"]["is_active"] is expected


# ---------------------------------------------------------------------------
# Lookup failure
# ---------------------------------------------------------------------------


class TestLookupFailure:
    def test_unknown_user_returns_404(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        patched_handler["target_row"] = None

        response = h.handler(
            _api_event(
                body={
                    "user_id": "ghost",
                    "fields": {"display_name": "Ghost"},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 404
        assert patched_handler["update_calls"] == []
        # 404 path should NOT write an audit row — nothing changed.
        assert patched_handler["audit_calls"] == []


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_email_update_returns_full_payload(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "user_id": TARGET_USER_ID,
                    "fields": {"email": "new@example.com"},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["Success"] is True
        assert parsed["user_id"] == TARGET_USER_ID
        assert parsed["before"] == {"email": "old@example.com"}
        assert parsed["after"] == {"email": "new@example.com"}
        assert parsed["audit_id"] == "audit-row-xyz"

        # update_row called exactly once.
        assert len(patched_handler["update_calls"]) == 1
        write = patched_handler["update_calls"][0]
        assert write["table"] == "whitelisted_users"
        assert write["column"] == "sleeper_user_id"
        assert write["value"] == TARGET_USER_ID
        assert write["fields"] == {"email": "new@example.com"}

        # write_audit called exactly once with the expected args.
        assert len(patched_handler["audit_calls"]) == 1
        audit_kwargs = patched_handler["audit_calls"][0]
        assert audit_kwargs["action"] == "users.update"
        assert audit_kwargs["target_table"] == "whitelisted_users"
        assert audit_kwargs["target_id"] == TARGET_USER_ID
        assert audit_kwargs["before"] == {"email": "old@example.com"}
        assert audit_kwargs["after"] == {"email": "new@example.com"}
        assert audit_kwargs["actor_user_id"] == ADMIN_ID

    def test_multi_field_update(self, patched_handler) -> None:
        from lambdas.api_admin_users_update import handler as h

        response = h.handler(
            _api_event(
                body={
                    "user_id": TARGET_USER_ID,
                    "fields": {
                        "display_name": "Renamed",
                        "is_admin": True,
                    },
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["before"] == {
            "display_name": "Old Name",
            "is_admin": False,
        }
        assert parsed["after"] == {
            "display_name": "Renamed",
            "is_admin": True,
        }

    def test_audit_write_failure_does_not_break_response(
        self, patched_handler
    ) -> None:
        """Best-effort audit write — when write_audit returns None
        (its swallow-all error path), the parent action still
        succeeds and returns 200."""
        from lambdas.api_admin_users_update import handler as h

        patched_handler["audit_return"] = None  # simulate audit write failure

        response = h.handler(
            _api_event(
                body={
                    "user_id": TARGET_USER_ID,
                    "fields": {"display_name": "Renamed"},
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["Success"] is True
        assert parsed["audit_id"] is None
        # The update still happened.
        assert len(patched_handler["update_calls"]) == 1
