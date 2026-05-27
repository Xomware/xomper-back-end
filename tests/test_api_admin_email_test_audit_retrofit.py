"""
F4 retrofit tests for `api_admin_email_test`.

These tests cover the audit-write hook the F4 backend PR added to the
F1 test-email lambda. The F1 test file already exists and covers the
happy + invariant paths; this file isolates the audit-write
expectation so the retrofit can be reviewed independently.

Contract:
  - On a successful SES send, the handler calls
    audit_log.write_audit(action="email.test", ...) exactly once.
  - On an SES delivery failure (502 response), the handler does NOT
    call write_audit — we only audit completed actions.
  - When write_audit returns None (best-effort failure), the parent
    response is still 200 with the full receipt payload.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
LEAGUE_ID = "LEAGUE_ID"
RECIPIENT_ID = "u7"


def _api_event(*, body: dict | None = None) -> dict:
    return {
        "httpMethod": "POST",
        "path": "/admin/email-test",
        "headers": {"X-Sleeper-User-Id": ADMIN_ID},
        "body": json.dumps(body or {}),
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    """Subset of the F1 fixture wiring needed for the audit-retrofit
    assertions. We mirror the seam set in test_api_admin_email_test.py
    to keep this independent of the original fixture import path."""
    from lambdas.api_admin_email_test import handler as h

    state: dict[str, Any] = {
        "admin_row": {
            "id": "row-admin",
            "sleeper_user_id": ADMIN_ID,
            "email": "admin@example.com",
            "display_name": "Admin Dom",
            "is_active": True,
            "is_admin": True,
        },
        "recipient_row": {
            "id": "row-recipient",
            "sleeper_user_id": RECIPIENT_ID,
            "email": "manager7@example.com",
            "display_name": "Manager Seven",
            "is_active": True,
        },
        "report_row": {
            "league_id": LEAGUE_ID,
            "report_type": "weekly",
            "period": "2026W04",
            "body_markdown": "# Weekly recap\n",
            "metadata": {"model": "claude-haiku-4-5"},
        },
        "active_league": {
            "sleeper_league_id": LEAGUE_ID,
            "league_name": "CLT DYNASTY",
        },
        "ses_result": {
            "success": True,
            "message_id": "ses-mid-abc-123",
            "error": None,
        },
        "audit_calls": [],
        "audit_return": {"id": "audit-row-email"},
    }

    monkeypatch.setattr(h, "require_admin", lambda event, body=None: state["admin_row"])
    monkeypatch.setattr(
        h,
        "get_whitelisted_user_by_sleeper_id",
        lambda sleeper_user_id: state["recipient_row"]
        if sleeper_user_id == RECIPIENT_ID
        else None,
    )
    monkeypatch.setattr(h, "get_active_whitelisted_league", lambda: state["active_league"])
    monkeypatch.setattr(
        h.ai_reports_store,
        "get_report",
        lambda *, league_id, report_type, period: state["report_row"],
    )
    monkeypatch.setattr(
        h,
        "send_email",
        lambda **kwargs: state["ses_result"],
    )

    def _write_audit(**kwargs: Any):
        state["audit_calls"].append(kwargs)
        return state["audit_return"]

    monkeypatch.setattr(h, "write_audit", _write_audit)

    return state


class TestAuditRetrofit:
    def test_writes_audit_on_successful_send(self, patched_handler) -> None:
        from lambdas.api_admin_email_test import handler as h

        response = h.handler(
            _api_event(
                body={
                    "recipient_user_id": RECIPIENT_ID,
                    "report_id": f"LEAGUE#{LEAGUE_ID}|REPORT#weekly#2026W04",
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200

        # Exactly one audit row written.
        assert len(patched_handler["audit_calls"]) == 1
        audit = patched_handler["audit_calls"][0]
        assert audit["action"] == "email.test"
        assert audit["actor_user_id"] == ADMIN_ID
        assert audit["target_table"] == "xomper-ai-reports"
        # target_id encodes the report's composite key.
        assert "weekly" in audit["target_id"]
        assert "2026W04" in audit["target_id"]
        assert audit["before"] is None
        # `after` carries the SES outcome metadata.
        after = audit["after"]
        assert after["recipient_email"] == "manager7@example.com"
        assert after["recipient_user_id"] == RECIPIENT_ID
        assert after["message_id"] == "ses-mid-abc-123"
        assert after["report_type"] == "weekly"
        assert after["report_period"] == "2026W04"

    def test_no_audit_on_ses_failure(self, patched_handler) -> None:
        from lambdas.api_admin_email_test import handler as h

        patched_handler["ses_result"] = {
            "success": False,
            "message_id": None,
            "error": "Throttling: rate exceeded",
        }

        response = h.handler(
            _api_event(
                body={
                    "recipient_user_id": RECIPIENT_ID,
                    "report_id": f"LEAGUE#{LEAGUE_ID}|REPORT#weekly#2026W04",
                }
            ),
            context=None,
        )
        # Send failed → 502.
        assert response["statusCode"] == 502
        # Audit NOT written — we only audit completed actions.
        assert patched_handler["audit_calls"] == []

    def test_audit_failure_does_not_break_response(self, patched_handler) -> None:
        """Best-effort write — even when write_audit returns None
        (its swallow-all path), the parent response is the full
        success receipt."""
        from lambdas.api_admin_email_test import handler as h

        patched_handler["audit_return"] = None

        response = h.handler(
            _api_event(
                body={
                    "recipient_user_id": RECIPIENT_ID,
                    "report_id": f"LEAGUE#{LEAGUE_ID}|REPORT#weekly#2026W04",
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["message_id"] == "ses-mid-abc-123"
