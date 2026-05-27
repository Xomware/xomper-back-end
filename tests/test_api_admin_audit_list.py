"""
Tests for `api_admin_audit_list` (admin-portal F4).

Endpoint shape: GET /admin/audit-list?limit=&cursor=&action=&actor_user_id=

Covers:
  - 403 when caller not admin.
  - Happy path returns rows ordered by created_at desc (delegated to
    list_rows — verify the rows pass through).
  - Cursor pagination: cursor query param passes through to list_rows.
  - Filter: action query param narrows the filter set.
  - Filter: actor_user_id query param narrows the filter set.
  - limit clamped: limit=500 -> 200; limit=0 -> 1.
  - default limit applied when omitted: 50.
  - GRACEFUL fallback: if list_rows raises (e.g. admin_audit table
    doesn't exist yet), returns 200 with empty rows + table_missing=true.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"


def _api_event(
    *,
    qs: dict[str, Any] | None = None,
    sleeper_user_id: str | None = ADMIN_ID,
) -> dict[str, Any]:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "GET",
        "path": "/admin/audit-list",
        "headers": headers,
        "queryStringParameters": qs or {},
    }


def _audit_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": "a3",
            "created_at": "2026-05-27T12:00:00Z",
            "actor_user_id": ADMIN_ID,
            "action": "users.update",
            "target_table": "whitelisted_users",
            "target_id": "u7",
            "before": {"email": "old@example.com"},
            "after": {"email": "new@example.com"},
            "metadata": {},
        },
        {
            "id": "a2",
            "created_at": "2026-05-27T11:00:00Z",
            "actor_user_id": ADMIN_ID,
            "action": "reports.flag",
            "target_table": "xomper-ai-reports",
            "target_id": "LEAGUE_ID|weekly|2026W04",
            "before": {"is_redacted": None},
            "after": {"is_redacted": "true"},
            "metadata": {},
        },
        {
            "id": "a1",
            "created_at": "2026-05-27T10:00:00Z",
            "actor_user_id": ADMIN_ID,
            "action": "email.test",
            "target_table": "xomper-ai-reports",
            "target_id": "LEAGUE#X|REPORT#weekly#2026W04",
            "before": None,
            "after": {"recipient_email": "user@example.com"},
            "metadata": {},
        },
    ]


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_audit_list import handler as h

    state: dict[str, Any] = {
        "admin_row": {
            "id": "row-admin",
            "sleeper_user_id": ADMIN_ID,
            "is_admin": True,
            "is_active": True,
        },
        "rows": _audit_rows(),
        "next_cursor": None,
        "list_calls": [],
        # When set, list_rows raises this to simulate missing table.
        "raise_on_list": None,
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _list_rows(table: str, **kwargs: Any):
        state["list_calls"].append({"table": table, **kwargs})
        if state["raise_on_list"] is not None:
            raise state["raise_on_list"]
        # Honour the action / actor filters in the stub so tests can
        # assert narrowing behavior end-to-end.
        rows = list(state["rows"])
        filters = kwargs.get("filters") or {}
        if "action" in filters:
            rows = [r for r in rows if r.get("action") == filters["action"]]
        if "actor_user_id" in filters:
            rows = [r for r in rows if r.get("actor_user_id") == filters["actor_user_id"]]
        return (rows, state["next_cursor"])

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "list_rows", _list_rows)

    return state


# ---------------------------------------------------------------------------
# Auth gate
# ---------------------------------------------------------------------------


class TestAdminGate:
    def test_non_admin_returns_403(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_audit_list import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 403
        assert patched_handler["list_calls"] == []


# ---------------------------------------------------------------------------
# Happy path + filters
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_default_page_returns_all_rows_descending(self, patched_handler) -> None:
        from lambdas.api_admin_audit_list import handler as h

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["Success"] is True
        assert parsed["count"] == 3
        assert parsed["table_missing"] is False
        # Order preserved (stub returns the seed list which is desc).
        assert [r["id"] for r in parsed["rows"]] == ["a3", "a2", "a1"]
        # Default limit is 50.
        call = patched_handler["list_calls"][0]
        assert call["table"] == "admin_audit"
        assert call["limit"] == 50

    def test_cursor_passes_through(self, patched_handler) -> None:
        from lambdas.api_admin_audit_list import handler as h

        response = h.handler(
            _api_event(qs={"cursor": "2026-05-27T11:00:00Z"}),
            context=None,
        )
        assert response["statusCode"] == 200
        call = patched_handler["list_calls"][0]
        assert call["cursor"] == "2026-05-27T11:00:00Z"

    def test_action_filter_narrows(self, patched_handler) -> None:
        from lambdas.api_admin_audit_list import handler as h

        response = h.handler(
            _api_event(qs={"action": "users.update"}),
            context=None,
        )
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["count"] == 1
        assert parsed["rows"][0]["action"] == "users.update"

    def test_actor_filter_narrows(self, patched_handler) -> None:
        from lambdas.api_admin_audit_list import handler as h

        # Inject a second-actor row to prove the filter works.
        patched_handler["rows"].append(
            {
                "id": "a4",
                "created_at": "2026-05-27T09:00:00Z",
                "actor_user_id": "other-admin",
                "action": "email.test",
                "target_table": None,
                "target_id": None,
                "before": None,
                "after": None,
                "metadata": {},
            }
        )

        response = h.handler(
            _api_event(qs={"actor_user_id": ADMIN_ID}),
            context=None,
        )
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["count"] == 3
        assert all(r["actor_user_id"] == ADMIN_ID for r in parsed["rows"])

    def test_next_cursor_passes_through(self, patched_handler) -> None:
        from lambdas.api_admin_audit_list import handler as h

        patched_handler["next_cursor"] = "2026-05-27T10:00:00Z"

        response = h.handler(_api_event(), context=None)
        parsed = json.loads(response["body"])
        assert parsed["next_cursor"] == "2026-05-27T10:00:00Z"


# ---------------------------------------------------------------------------
# Limit clamping
# ---------------------------------------------------------------------------


class TestLimitClamping:
    def test_limit_500_clamped_to_200(self, patched_handler) -> None:
        from lambdas.api_admin_audit_list import handler as h

        h.handler(_api_event(qs={"limit": "500"}), context=None)
        call = patched_handler["list_calls"][0]
        assert call["limit"] == 200

    def test_limit_zero_clamped_to_one(self, patched_handler) -> None:
        from lambdas.api_admin_audit_list import handler as h

        h.handler(_api_event(qs={"limit": "0"}), context=None)
        call = patched_handler["list_calls"][0]
        assert call["limit"] == 1

    def test_invalid_limit_falls_back_to_default(self, patched_handler) -> None:
        from lambdas.api_admin_audit_list import handler as h

        h.handler(_api_event(qs={"limit": "abc"}), context=None)
        call = patched_handler["list_calls"][0]
        assert call["limit"] == 50


# ---------------------------------------------------------------------------
# GRACEFUL fallback — admin_audit table missing
# ---------------------------------------------------------------------------


class TestTableMissingGracefulFallback:
    """If the Supabase migration hasn't been applied yet, the
    `admin_audit` relation doesn't exist. The endpoint must return
    HTTP 200 with empty rows + `table_missing: true` so the iOS audit
    feed renders an empty state rather than erroring."""

    def test_missing_table_returns_empty_with_flag(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_audit_list import handler as h
        from lambdas.common.errors import SleeperAPIError

        patched_handler["raise_on_list"] = SleeperAPIError(
            message="HTTP 404 — relation public.admin_audit does not exist",
            function="supabase_helper._get",
        )

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])
        assert parsed["Success"] is True
        assert parsed["table_missing"] is True
        assert parsed["count"] == 0
        assert parsed["rows"] == []
        assert parsed["next_cursor"] is None

    def test_missing_table_logs_error(
        self,
        patched_handler,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CloudWatch must surface the misconfiguration even though
        the response is intentionally graceful. Verify by patching
        the lambda's logger to a spy — the xomper logger uses
        propagate=False so caplog can't intercept it natively."""
        from lambdas.api_admin_audit_list import handler as h

        captured: list[str] = []
        original_error = h.log.error

        def _spy_error(msg, *args, **kwargs):
            captured.append(str(msg))
            return original_error(msg, *args, **kwargs)

        monkeypatch.setattr(h.log, "error", _spy_error)

        patched_handler["raise_on_list"] = Exception(
            "relation \"admin_audit\" does not exist"
        )

        response = h.handler(_api_event(), context=None)

        assert response["statusCode"] == 200
        joined = " ".join(captured)
        # The lambda logs the failure with the table_missing tag /
        # admin_audit name visible in CloudWatch output.
        assert "table_missing" in joined or "admin_audit" in joined
