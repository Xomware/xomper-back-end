"""
Tests for `api_admin_cron_settings_update` (admin-cron-settings).

Endpoint shape: POST /admin/cron-settings-update, body
  { cron_key, enabled?, test_mode? }

Covers:
  - 403 when caller not admin.
  - 400 on missing cron_key.
  - 400 on unknown cron_key (not in allowlist).
  - 400 when neither enabled nor test_mode supplied.
  - 400 on non-bool value.
  - 404 when the row doesn't exist (cron_settings raises NotFoundError).
  - Happy path: update_cron_setting + write_audit called once each.
  - Bool coercion accepts "true"/"false" strings + real bools.
  - Audit failure does not fail the parent action (best-effort).
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
KEY = "notif_weekly_recap"


def _api_event(
    *,
    body: dict[str, Any] | None = None,
    sleeper_user_id: str | None = ADMIN_ID,
) -> dict[str, Any]:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "POST",
        "path": "/admin/cron-settings-update",
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


def _existing_row() -> dict[str, Any]:
    return {
        "cron_key": KEY,
        "enabled": True,
        "test_mode": False,
        "description": "Weekly matchup recap — Tue 9am ET",
        "updated_at": "2026-06-01T10:00:00Z",
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_cron_settings_update import handler as h

    state: dict[str, Any] = {
        "admin_row": _admin_row(),
        "existing_setting": _existing_row(),
        "update_calls": [],
        "audit_calls": [],
        "audit_return": {"id": "audit-row-xyz"},
        "update_raises": None,
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _get_cron_setting(cron_key: str):
        row = state["existing_setting"]
        if row and row.get("cron_key") == cron_key:
            return {
                "cron_key": row["cron_key"],
                "enabled": bool(row["enabled"]),
                "test_mode": bool(row["test_mode"]),
                "description": row.get("description"),
            }
        # Safe default — mirrors helper behaviour
        return {
            "cron_key": cron_key,
            "enabled": True,
            "test_mode": False,
            "description": None,
        }

    def _update_cron_setting(cron_key: str, enabled, test_mode):
        state["update_calls"].append(
            {"cron_key": cron_key, "enabled": enabled, "test_mode": test_mode}
        )
        if state["update_raises"] is not None:
            raise state["update_raises"]
        row = dict(state["existing_setting"] or {})
        if enabled is not None:
            row["enabled"] = bool(enabled)
        if test_mode is not None:
            row["test_mode"] = bool(test_mode)
        state["existing_setting"] = row
        return row

    def _write_audit(**kwargs):
        state["audit_calls"].append(kwargs)
        return state["audit_return"]

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "get_cron_setting", _get_cron_setting)
    monkeypatch.setattr(h, "update_cron_setting", _update_cron_setting)
    monkeypatch.setattr(h, "write_audit", _write_audit)

    return state


# ---------------------------------------------------------------------------
# Auth gate
# ---------------------------------------------------------------------------


class TestAdminGate:
    def test_non_admin_returns_403(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )

        response = h.handler(
            _api_event(body={"cron_key": KEY, "enabled": False}),
            context=None,
        )
        assert response["statusCode"] == 403
        assert patched_handler["update_calls"] == []
        assert patched_handler["audit_calls"] == []


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestInputValidation:
    def test_missing_cron_key_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h

        response = h.handler(
            _api_event(body={"enabled": True}),
            context=None,
        )
        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "cron_key" in body["Message"]
        assert patched_handler["update_calls"] == []

    def test_unknown_cron_key_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h

        response = h.handler(
            _api_event(body={"cron_key": "notif_bogus", "enabled": False}),
            context=None,
        )
        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "notif_bogus" in body["Message"]
        assert patched_handler["update_calls"] == []

    def test_no_field_supplied_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h

        response = h.handler(
            _api_event(body={"cron_key": KEY}),
            context=None,
        )
        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "enabled" in body["Message"]
        assert "test_mode" in body["Message"]
        assert patched_handler["update_calls"] == []

    def test_non_bool_enabled_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h

        response = h.handler(
            _api_event(body={"cron_key": KEY, "enabled": "maybe"}),
            context=None,
        )
        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "enabled" in body["Message"].lower()
        assert patched_handler["update_calls"] == []

    def test_non_bool_test_mode_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h

        response = h.handler(
            _api_event(body={"cron_key": KEY, "test_mode": 42}),
            context=None,
        )
        assert response["statusCode"] == 400
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
    def test_enabled_coerces(self, patched_handler, raw, expected) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h

        response = h.handler(
            _api_event(body={"cron_key": KEY, "enabled": raw}),
            context=None,
        )
        assert response["statusCode"] == 200
        call = patched_handler["update_calls"][0]
        assert call["enabled"] is expected


# ---------------------------------------------------------------------------
# Lookup failure
# ---------------------------------------------------------------------------


class TestLookupFailure:
    def test_not_found_returns_404(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h
        from lambdas.common.errors import NotFoundError

        patched_handler["update_raises"] = NotFoundError(
            message=f"cron_key not found: {KEY}",
            resource=KEY,
        )
        response = h.handler(
            _api_event(body={"cron_key": KEY, "enabled": False}),
            context=None,
        )
        assert response["statusCode"] == 404
        assert patched_handler["audit_calls"] == []


# ---------------------------------------------------------------------------
# Happy path + audit
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_enabled_only_patch_writes_audit(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h

        response = h.handler(
            _api_event(body={"cron_key": KEY, "enabled": False}),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["cron_key"] == KEY
        assert body["enabled"] is False
        # test_mode unchanged in the response (since we only patched enabled).
        assert body["test_mode"] is False
        assert body["audit_id"] == "audit-row-xyz"

        # update_cron_setting called once
        assert len(patched_handler["update_calls"]) == 1
        call = patched_handler["update_calls"][0]
        assert call["cron_key"] == KEY
        assert call["enabled"] is False
        assert call["test_mode"] is None

        # Audit row carries the before/after diff
        assert len(patched_handler["audit_calls"]) == 1
        audit = patched_handler["audit_calls"][0]
        assert audit["action"] == "cron_settings.update"
        assert audit["target_table"] == "admin_cron_settings"
        assert audit["target_id"] == KEY
        assert audit["before"] == {"enabled": True}
        assert audit["after"] == {"enabled": False}
        assert audit["actor_user_id"] == ADMIN_ID

    def test_test_mode_only_patch(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h

        body = json.loads(
            h.handler(
                _api_event(body={"cron_key": KEY, "test_mode": True}),
                context=None,
            )["body"]
        )
        assert body["test_mode"] is True
        assert body["enabled"] is True  # unchanged
        # audit captures only the test_mode diff
        audit = patched_handler["audit_calls"][0]
        assert audit["before"] == {"test_mode": False}
        assert audit["after"] == {"test_mode": True}

    def test_both_fields_patch(self, patched_handler) -> None:
        from lambdas.api_admin_cron_settings_update import handler as h

        body = json.loads(
            h.handler(
                _api_event(
                    body={
                        "cron_key": KEY,
                        "enabled": False,
                        "test_mode": True,
                    }
                ),
                context=None,
            )["body"]
        )
        assert body["enabled"] is False
        assert body["test_mode"] is True
        audit = patched_handler["audit_calls"][0]
        assert audit["before"] == {"enabled": True, "test_mode": False}
        assert audit["after"] == {"enabled": False, "test_mode": True}

    def test_audit_write_failure_does_not_break_response(
        self, patched_handler
    ) -> None:
        """Best-effort audit write — when write_audit returns None,
        the parent action still succeeds and returns 200."""
        from lambdas.api_admin_cron_settings_update import handler as h

        patched_handler["audit_return"] = None
        response = h.handler(
            _api_event(body={"cron_key": KEY, "enabled": False}),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["audit_id"] is None
        # The update still happened.
        assert len(patched_handler["update_calls"]) == 1
