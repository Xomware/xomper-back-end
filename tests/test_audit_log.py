"""
Tests for `lambdas.common.audit_log.write_audit` (admin-portal F4).

The CRITICAL invariant under test: audit failures MUST NEVER raise.
write_audit is invoked from inside the happy path of every mutating
admin lambda; a raised exception would propagate to the caller and
turn a successful primary action (email sent, flag toggled, user
updated) into an HTTP 500. The contract is to swallow + log.

Covers:
- Happy path posts the expected row to the `admin_audit` Supabase table.
- `before=None` is accepted (e.g. create-style actions like email.test).
- Any Supabase error is caught + logged + returns None (the "table
  missing" case is just one shape of this).
- All argument shapes (with + without target_*, metadata, before/after).
"""
from __future__ import annotations

from typing import Any

import pytest


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_writes_expected_row(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from lambdas.common import audit_log

        calls: list[tuple[str, dict[str, Any]]] = []

        def _fake_insert(table: str, row: dict[str, Any]) -> dict[str, Any]:
            calls.append((table, row))
            return {"id": "audit-row-1", **row}

        monkeypatch.setattr(audit_log, "insert_row", _fake_insert)

        result = audit_log.write_audit(
            actor_user_id="admin-123",
            action="users.update",
            target_table="whitelisted_users",
            target_id="u7",
            before={"email": "old@example.com"},
            after={"email": "new@example.com"},
            metadata={"source": "ios"},
        )

        assert result is not None
        assert result["id"] == "audit-row-1"
        # Exactly one Supabase write.
        assert len(calls) == 1
        table, row = calls[0]
        assert table == "admin_audit"
        assert row["actor_user_id"] == "admin-123"
        assert row["action"] == "users.update"
        assert row["target_table"] == "whitelisted_users"
        assert row["target_id"] == "u7"
        assert row["before"] == {"email": "old@example.com"}
        assert row["after"] == {"email": "new@example.com"}
        assert row["metadata"] == {"source": "ios"}

    def test_before_none_is_allowed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """email.test-style actions have no `before` snapshot."""
        from lambdas.common import audit_log

        captured: list[dict[str, Any]] = []

        def _fake_insert(table: str, row: dict[str, Any]) -> dict[str, Any]:
            captured.append(row)
            return {"id": "audit-row-2", **row}

        monkeypatch.setattr(audit_log, "insert_row", _fake_insert)

        result = audit_log.write_audit(
            actor_user_id="admin-123",
            action="email.test",
            target_table="xomper-ai-reports",
            target_id="LEAGUE#X|REPORT#weekly#2026W04",
            before=None,
            after={"recipient_email": "user@example.com"},
        )

        assert result is not None
        assert captured[0]["before"] is None
        # metadata defaults to {} when not supplied.
        assert captured[0]["metadata"] == {}

    def test_minimal_call_only_actor_and_action(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """All target_* / before / after / metadata are optional."""
        from lambdas.common import audit_log

        captured: list[dict[str, Any]] = []

        def _fake_insert(table: str, row: dict[str, Any]) -> dict[str, Any]:
            captured.append(row)
            return {"id": "audit-row-3", **row}

        monkeypatch.setattr(audit_log, "insert_row", _fake_insert)

        result = audit_log.write_audit(
            actor_user_id="admin-123",
            action="custom.event",
        )

        assert result is not None
        row = captured[0]
        assert row["target_table"] is None
        assert row["target_id"] is None
        assert row["before"] is None
        assert row["after"] is None
        assert row["metadata"] == {}


# ---------------------------------------------------------------------------
# Best-effort invariant — failures NEVER raise
# ---------------------------------------------------------------------------


class TestBestEffortSwallowsErrors:
    """The load-bearing test for the F4 invariant: a Supabase error
    inside write_audit must NEVER propagate. The helper is called from
    inside every mutating admin lambda's happy path; a raised exception
    would convert a successful primary action into an HTTP 500."""

    def test_supabase_error_returns_none_no_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import audit_log
        from lambdas.common.errors import SleeperAPIError

        def _explode(table: str, row: dict[str, Any]) -> dict[str, Any]:
            raise SleeperAPIError(
                message="HTTP 404 — relation public.admin_audit does not exist",
                function="supabase_helper._post",
            )

        monkeypatch.setattr(audit_log, "insert_row", _explode)

        # Must NOT raise. Must return None.
        result = audit_log.write_audit(
            actor_user_id="admin-123",
            action="users.update",
            target_table="whitelisted_users",
            target_id="u7",
            before={"email": "a"},
            after={"email": "b"},
        )

        assert result is None

    def test_generic_exception_returns_none_no_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-SleeperAPIError exceptions (network timeouts, JSON
        decode errors, etc.) also get swallowed."""
        from lambdas.common import audit_log

        def _explode(table: str, row: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("connection reset by peer")

        monkeypatch.setattr(audit_log, "insert_row", _explode)

        result = audit_log.write_audit(
            actor_user_id="admin-123",
            action="reports.flag",
        )

        assert result is None

    def test_table_missing_logs_error_but_returns_cleanly(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The Supabase migration apply is an out-of-band manual step.
        Until it's done, every write_audit call will fail with a
        relation-does-not-exist error. The helper must log that loudly
        but not raise.

        Verify by patching the audit_log module logger to a spy — the
        xomper logger uses propagate=False so caplog can't intercept
        it natively."""
        from lambdas.common import audit_log

        captured: list[str] = []
        original_error = audit_log.log.error

        def _spy_error(msg, *args, **kwargs):
            captured.append(str(msg))
            return original_error(msg, *args, **kwargs)

        monkeypatch.setattr(audit_log.log, "error", _spy_error)

        def _explode(table: str, row: dict[str, Any]) -> dict[str, Any]:
            raise Exception("relation \"admin_audit\" does not exist")

        monkeypatch.setattr(audit_log, "insert_row", _explode)

        result = audit_log.write_audit(
            actor_user_id="admin-123",
            action="leagues.update",
            target_table="whitelisted_leagues",
            target_id="L1",
        )

        assert result is None
        joined = " ".join(captured)
        # Verify the failure was logged with the action name so
        # CloudWatch is searchable.
        assert "leagues.update" in joined
        assert "FAILED" in joined.upper() or "failed" in joined
