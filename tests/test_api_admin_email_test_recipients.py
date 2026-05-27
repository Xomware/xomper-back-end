"""
Tests for `api_admin_email_test_recipients` (admin-portal F1).

Covers:
  - 403 when the caller is not an admin.
  - Happy path: returns the mapped `{user_id, display_name, email,
    is_admin}` rows for every active whitelisted user.
  - Drops rows missing a `sleeper_user_id` or `email` — defensive,
    keeps the iOS picker's payload clean.

External Supabase access is monkeypatched.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"


def _api_event(*, sleeper_user_id: str | None = ADMIN_ID) -> dict:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "GET",
        "path": "/admin/email-test-recipients",
        "headers": headers,
        "queryStringParameters": {"sleeper_user_id": sleeper_user_id} if sleeper_user_id else None,
    }


def _whitelisted_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for i in range(1, 13):
        rows.append(
            {
                "id": f"row-{i}",
                "sleeper_user_id": f"u{i}",
                "email": f"manager{i}@example.com",
                "display_name": f"Manager{i}",
                "sleeper_username": f"manager{i}",
                "is_active": True,
                "is_admin": i == 1,
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Auth gate
# ---------------------------------------------------------------------------


class TestAdminGate:
    def test_non_admin_returns_403(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_email_test_recipients import handler as h
        from lambdas.common.admin_gate import NotAdmin

        def _raise(event, body=None):
            raise NotAdmin("not authorized")

        monkeypatch.setattr(h, "require_admin", _raise)

        called: list[Any] = []

        def _fetch_users():
            called.append(True)
            return _whitelisted_rows()

        monkeypatch.setattr(h, "get_active_whitelisted_users", _fetch_users)

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 403
        body = json.loads(response["body"])
        assert body["Success"] is False
        # Supabase shouldn't have been queried at all.
        assert called == []


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_returns_active_recipients(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_email_test_recipients import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body=None: {"sleeper_user_id": ADMIN_ID}
        )
        monkeypatch.setattr(h, "get_active_whitelisted_users", _whitelisted_rows)

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])

        assert body["Success"] is True
        assert body["count"] == 12
        assert len(body["recipients"]) == 12

        first = body["recipients"][0]
        assert set(first.keys()) == {"user_id", "display_name", "email", "is_admin"}
        assert first["user_id"] == "u1"
        assert first["email"] == "manager1@example.com"
        assert first["is_admin"] is True

        # Non-admin row stays flagged as non-admin.
        second = body["recipients"][1]
        assert second["is_admin"] is False

    def test_drops_rows_missing_email_or_sleeper_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_email_test_recipients import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body=None: {"sleeper_user_id": ADMIN_ID}
        )

        rows = _whitelisted_rows()
        # Corrupt two rows so the handler skips them.
        rows[2] = {**rows[2], "email": None}
        rows[5] = {**rows[5], "sleeper_user_id": None}

        monkeypatch.setattr(h, "get_active_whitelisted_users", lambda: rows)

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])

        assert body["count"] == 10  # 12 - 2 dropped
        sleeper_ids = [r["user_id"] for r in body["recipients"]]
        assert "u3" not in sleeper_ids  # missing email
        assert "u6" not in sleeper_ids  # missing sleeper id

    def test_empty_users_list_returns_zero(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_email_test_recipients import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body=None: {"sleeper_user_id": ADMIN_ID}
        )
        monkeypatch.setattr(h, "get_active_whitelisted_users", lambda: [])

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["count"] == 0
        assert body["recipients"] == []
