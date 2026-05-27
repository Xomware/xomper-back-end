"""
Tests for `lambdas.common.admin_gate.is_admin` (admin-portal F3).

`is_admin` is the non-raising sibling of `require_admin`. The read-
path endpoints (`api_ai_reports_latest`, `api_ai_reports_list`) use
it to branch the `is_redacted` filter without breaking the
happy-path response for non-admin callers.

Covers:
  - Returns True when the resolved user has `is_admin = True`.
  - Returns False when the resolved user has `is_admin = False`.
  - Returns False when no caller identifier is supplied.
  - Returns False when the supabase lookup returns None.
  - Returns False when the supabase lookup raises (defensive — read
    paths should never fail because the admin lookup hiccuped).
"""
from __future__ import annotations

from typing import Any

import pytest


ADMIN_ID = "admin-id-123"
NON_ADMIN_ID = "u7"


def _event(*, sleeper_user_id: str | None = None, email: str | None = None) -> dict[str, Any]:
    headers: dict[str, str] = {}
    if sleeper_user_id:
        headers["X-Sleeper-User-Id"] = sleeper_user_id
    if email:
        headers["X-User-Email"] = email
    return {"httpMethod": "GET", "headers": headers}


class TestIsAdmin:
    def test_returns_true_for_admin_caller(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import admin_gate

        def _by_sleeper(sleeper_id: str):
            return {"id": "row-admin", "sleeper_user_id": sleeper_id, "is_admin": True}

        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_sleeper_id", _by_sleeper
        )
        assert admin_gate.is_admin(_event(sleeper_user_id=ADMIN_ID)) is True

    def test_returns_false_for_non_admin_caller(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import admin_gate

        def _by_sleeper(sleeper_id: str):
            return {"id": "row-u7", "sleeper_user_id": sleeper_id, "is_admin": False}

        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_sleeper_id", _by_sleeper
        )
        assert admin_gate.is_admin(_event(sleeper_user_id=NON_ADMIN_ID)) is False

    def test_returns_false_when_no_identifier(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import admin_gate

        # Lookup should never run with no identifier present.
        def _fail(*args, **kwargs):  # pragma: no cover — should not be called
            raise AssertionError("lookup invoked despite missing identity")

        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_sleeper_id", _fail
        )
        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_email", _fail
        )
        assert admin_gate.is_admin({"httpMethod": "GET", "headers": {}}) is False

    def test_returns_false_when_user_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import admin_gate

        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_sleeper_id", lambda sid: None
        )
        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_email", lambda em: None
        )
        assert admin_gate.is_admin(_event(sleeper_user_id="ghost")) is False

    def test_returns_false_when_lookup_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import admin_gate

        def _raise(*args, **kwargs):
            raise RuntimeError("supabase down")

        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_sleeper_id", _raise
        )
        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_email", _raise
        )
        # Read-paths must never raise on identity lookup hiccups — they
        # just degrade to non-admin behavior (filter redacted out).
        assert admin_gate.is_admin(_event(sleeper_user_id=ADMIN_ID)) is False

    def test_falls_back_to_email_when_sleeper_id_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import admin_gate

        def _by_sleeper(sleeper_id: str):  # pragma: no cover — not invoked
            return None

        def _by_email(email: str):
            return {"id": "row-admin", "email": email, "is_admin": True}

        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_sleeper_id", _by_sleeper
        )
        monkeypatch.setattr(
            admin_gate, "get_whitelisted_user_by_email", _by_email
        )
        assert admin_gate.is_admin(_event(email="admin@example.com")) is True
