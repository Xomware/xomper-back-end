"""
Tests for `api_admin_email_test` (admin-portal F1).

Covers:
  - 403 when the caller is not an admin.
  - 400 when the body is missing required fields.
  - 404 when the recipient sleeper_user_id is unknown.
  - 404 when the requested report doesn't exist in `xomper-ai-reports`.
  - Happy path: single-recipient SES call + notification_log row tagged
    with `template="ai_review_test"` + response carries message_id,
    sent_at, recipient_email.
  - INVARIANT (the load-bearing one): the handler is read-only against
    `xomper-ai-reports`. We patch `ai_reports_store.update_metadata`
    to raise on call; happy path must still succeed — proving the
    test path never touches metadata / broadcast flags.

External integrations (SES, Supabase, DynamoDB) are all monkeypatched.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
LEAGUE_ID = "LEAGUE_ID"
RECIPIENT_ID = "u7"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _api_event(*, body: dict | None = None, sleeper_user_id: str | None = ADMIN_ID) -> dict:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "POST",
        "path": "/admin/email-test",
        "headers": headers,
        "body": json.dumps(body or {}),
    }


def _admin_row() -> dict[str, Any]:
    return {
        "id": "row-admin",
        "sleeper_user_id": ADMIN_ID,
        "email": "admin@example.com",
        "display_name": "Admin Dom",
        "sleeper_username": "domgiordano",
        "is_active": True,
        "is_admin": True,
    }


def _recipient_row() -> dict[str, Any]:
    return {
        "id": "row-recipient",
        "sleeper_user_id": RECIPIENT_ID,
        "email": "manager7@example.com",
        "display_name": "Manager Seven",
        "sleeper_username": "seven",
        "is_active": True,
        "is_admin": False,
    }


def _report_row() -> dict[str, Any]:
    return {
        "pk": f"LEAGUE#{LEAGUE_ID}",
        "sk": "REPORT#weekly#2026W04",
        "league_id": LEAGUE_ID,
        "report_type": "weekly",
        "period": "2026W04",
        "body_markdown": "# Weekly recap\n\nWeek 4 was wild.\n",
        "metadata": {"model": "claude-haiku-4-5"},
        "created_at": "2026-10-01T15:00:00Z",
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    """Patch every external seam the handler touches with deterministic
    stand-ins. Returns a `state` dict the tests can mutate or inspect."""
    from lambdas.api_admin_email_test import handler as h

    state: dict[str, Any] = {
        "admin_row": _admin_row(),
        "recipient_row": _recipient_row(),
        "report_row": _report_row(),
        "active_league": {
            "sleeper_league_id": LEAGUE_ID,
            "league_name": "CLT DYNASTY",
        },
        "emails_sent": [],
        "ses_result": {
            "success": True,
            "message_id": "ses-mid-abc-123",
            "error": None,
        },
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _get_recipient(sleeper_user_id: str):
        if sleeper_user_id == RECIPIENT_ID:
            return state["recipient_row"]
        return None

    def _get_active_league():
        return state["active_league"]

    def _get_report(*, league_id: str, report_type: str, period: str):
        # Return the canned report only on an exact key match so tests
        # can probe the "missing report" path by mutating state.
        canned = state["report_row"]
        if canned is None:
            return None
        if (
            league_id == canned["league_id"]
            and report_type == canned["report_type"]
            and period == canned["period"]
        ):
            return canned
        return None

    def _send_email(*, to_email, subject, html_body, text_body, template=None):
        state["emails_sent"].append(
            {
                "to_email": to_email,
                "subject": subject,
                "html_body": html_body,
                "text_body": text_body,
                "template": template,
            }
        )
        return state["ses_result"]

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h, "get_whitelisted_user_by_sleeper_id", _get_recipient)
    monkeypatch.setattr(h, "get_active_whitelisted_league", _get_active_league)
    # The handler imports `ai_reports_store` as a module; patch its
    # `get_report` attr in place so the handler-side reference resolves
    # to our stub.
    monkeypatch.setattr(h.ai_reports_store, "get_report", _get_report)
    monkeypatch.setattr(h, "send_email", _send_email)

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
        from lambdas.api_admin_email_test import handler as h
        from lambdas.common.admin_gate import NotAdmin

        def _raise(event, body=None):
            raise NotAdmin("not authorized")

        monkeypatch.setattr(h, "require_admin", _raise)

        response = h.handler(
            _api_event(
                body={
                    "recipient_user_id": RECIPIENT_ID,
                    "report_id": f"LEAGUE#{LEAGUE_ID}|REPORT#weekly#2026W04",
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 403
        body = json.loads(response["body"])
        assert body["Success"] is False
        # No SES call should have happened.
        assert patched_handler["emails_sent"] == []


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestInputValidation:
    def test_missing_recipient_user_id_returns_400(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_email_test import handler as h

        response = h.handler(
            _api_event(
                body={
                    "report_id": f"LEAGUE#{LEAGUE_ID}|REPORT#weekly#2026W04",
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert body["Success"] is False
        assert "recipient_user_id" in body["Message"]

    def test_missing_report_identifier_returns_400(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_email_test import handler as h

        response = h.handler(
            _api_event(
                body={"recipient_user_id": RECIPIENT_ID}
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert body["Success"] is False
        assert "report_id" in body["Message"]

    def test_malformed_report_id_returns_400(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_email_test import handler as h

        response = h.handler(
            _api_event(
                body={
                    "recipient_user_id": RECIPIENT_ID,
                    "report_id": "not-a-composite-key",
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert body["Success"] is False


# ---------------------------------------------------------------------------
# Recipient + report resolution
# ---------------------------------------------------------------------------


class TestLookupFailures:
    def test_unknown_recipient_returns_404(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_email_test import handler as h

        response = h.handler(
            _api_event(
                body={
                    "recipient_user_id": "ghost-user",
                    "report_id": f"LEAGUE#{LEAGUE_ID}|REPORT#weekly#2026W04",
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 404
        body = json.loads(response["body"])
        assert body["Success"] is False
        assert "ghost-user" in body["Message"]
        assert patched_handler["emails_sent"] == []

    def test_unknown_report_returns_404(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_email_test import handler as h

        patched_handler["report_row"] = None  # canned report removed

        response = h.handler(
            _api_event(
                body={
                    "recipient_user_id": RECIPIENT_ID,
                    "report_id": f"LEAGUE#{LEAGUE_ID}|REPORT#weekly#2026W04",
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 404
        body = json.loads(response["body"])
        assert body["Success"] is False
        assert "Report not found" in body["Message"]
        assert patched_handler["emails_sent"] == []


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_send_success_returns_full_receipt(
        self, patched_handler
    ) -> None:
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
        body = json.loads(response["body"])

        assert body["Success"] is True
        assert body["recipient_email"] == "manager7@example.com"
        assert body["recipient_user_id"] == RECIPIENT_ID
        assert body["message_id"] == "ses-mid-abc-123"
        assert body["template"] == "ai_review_test"
        assert body["report_type"] == "weekly"
        assert body["report_period"] == "2026W04"
        assert body["sent_at"].endswith("Z")  # ISO-8601 UTC

        # Exactly one SES call, single recipient, template tagged.
        assert len(patched_handler["emails_sent"]) == 1
        sent = patched_handler["emails_sent"][0]
        assert sent["to_email"] == "manager7@example.com"
        assert sent["template"] == "ai_review_test"

    def test_explicit_type_and_period_resolves_against_active_league(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_email_test import handler as h

        response = h.handler(
            _api_event(
                body={
                    "recipient_user_id": RECIPIENT_ID,
                    "report_type": "weekly",
                    "report_period": "2026W04",
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["report_type"] == "weekly"
        assert body["report_period"] == "2026W04"

    def test_ses_failure_returns_502(
        self, patched_handler
    ) -> None:
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
        assert response["statusCode"] == 502
        body = json.loads(response["body"])
        assert body["Success"] is False
        assert "Throttling" in body["Message"]


# ---------------------------------------------------------------------------
# INVARIANT — read-only against ai-reports
# ---------------------------------------------------------------------------


class TestReadOnlyInvariant:
    """The test-email handler MUST NEVER write to xomper-ai-reports —
    no `broadcast_at` stamp, no metadata partial, nothing. We enforce
    this by patching `ai_reports_store.update_metadata` to raise on
    call and asserting the happy path still completes. If the handler
    ever picks up an `update_metadata` reference by accident, this
    test breaks immediately."""

    def test_happy_path_does_not_touch_update_metadata(
        self,
        patched_handler,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_email_test import handler as h

        calls: list[Any] = []

        def _explode(*args, **kwargs):
            calls.append((args, kwargs))
            raise AssertionError(
                "update_metadata must not be called from api_admin_email_test"
            )

        # Patch the canonical store reference so any accidental import
        # at any depth would trip this trap.
        monkeypatch.setattr(h.ai_reports_store, "update_metadata", _explode)

        response = h.handler(
            _api_event(
                body={
                    "recipient_user_id": RECIPIENT_ID,
                    "report_id": f"LEAGUE#{LEAGUE_ID}|REPORT#weekly#2026W04",
                }
            ),
            context=None,
        )

        # The happy path completes — proving the read-only invariant.
        assert response["statusCode"] == 200
        assert calls == []
