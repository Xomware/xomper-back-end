"""
F4 retrofit tests for `api_admin_reports_flag`.

The F3 reports-flag lambda originally left a `# TODO(F4)` comment for
the audit hook. F4's backend PR replaces that with a real
`write_audit` call. These tests cover the audit-write expectations in
isolation so the retrofit is reviewable independently of the existing
F3 happy-path tests.

Contract:
  - On a successful flag toggle, write_audit is called exactly once
    with action="reports.flag" + the correct before/after blobs.
  - Before/after capture the actual transition on the flag key only —
    e.g. set is_redacted true on a report with no prior flag value
    produces before={is_redacted: None}, after={is_redacted: "true"}.
  - On a metadata write failure (update_metadata raises), the audit
    write does NOT happen — we only audit completed actions.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
LEAGUE_ID = "LEAGUE_ID"


def _api_event(*, body: dict | None = None) -> dict:
    return {
        "httpMethod": "POST",
        "path": "/admin/reports-flag",
        "headers": {"X-Sleeper-User-Id": ADMIN_ID},
        "body": json.dumps(body or {}),
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_admin_reports_flag import handler as h
    from lambdas.common.errors import DynamoDBError

    state: dict[str, Any] = {
        "admin_row": {
            "id": "row-admin",
            "sleeper_user_id": ADMIN_ID,
            "is_admin": True,
            "is_active": True,
        },
        "report_row": {
            "league_id": LEAGUE_ID,
            "report_type": "weekly",
            "period": "2026W04",
            "metadata": {"model": "claude-haiku-4-5"},
        },
        "raise_on_update": False,
        "audit_calls": [],
        "audit_return": {"id": "audit-row-flag"},
    }

    monkeypatch.setattr(h, "require_admin", lambda event, body=None: state["admin_row"])
    monkeypatch.setattr(
        h.ai_reports_store,
        "get_report",
        lambda *, league_id, report_type, period: state["report_row"],
    )

    def _update_metadata(*, league_id, report_type, period, partial):
        if state["raise_on_update"]:
            raise DynamoDBError(
                message="ConditionalCheckFailedException", table="xomper-ai-reports"
            )
        merged = dict(state["report_row"]["metadata"])
        merged.update(partial)
        state["report_row"]["metadata"] = merged
        return {
            "league_id": league_id,
            "report_type": report_type,
            "period": period,
            "metadata": merged,
        }

    monkeypatch.setattr(h.ai_reports_store, "update_metadata", _update_metadata)

    def _write_audit(**kwargs: Any):
        state["audit_calls"].append(kwargs)
        return state["audit_return"]

    monkeypatch.setattr(h, "write_audit", _write_audit)

    return state


class TestAuditRetrofit:
    def test_writes_audit_on_successful_toggle(self, patched_handler) -> None:
        from lambdas.api_admin_reports_flag import handler as h

        response = h.handler(
            _api_event(
                body={
                    "league_id": LEAGUE_ID,
                    "report_type": "weekly",
                    "period": "2026W04",
                    "flag": "is_redacted",
                    "value": True,
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200

        # Exactly one audit row written.
        assert len(patched_handler["audit_calls"]) == 1
        audit = patched_handler["audit_calls"][0]
        assert audit["action"] == "reports.flag"
        assert audit["actor_user_id"] == ADMIN_ID
        assert audit["target_table"] == "xomper-ai-reports"
        # target_id encodes the (league|type|period) tuple.
        assert audit["target_id"] == f"{LEAGUE_ID}|weekly|2026W04"
        # The previous metadata had no `is_redacted` key, so before is
        # {is_redacted: None}. After captures the new string value.
        assert audit["before"] == {"is_redacted": None}
        assert audit["after"] == {"is_redacted": "true"}

    def test_writes_audit_with_existing_prior_value(self, patched_handler) -> None:
        """When a flag is already set, before captures the prior
        string value verbatim."""
        from lambdas.api_admin_reports_flag import handler as h

        patched_handler["report_row"]["metadata"]["is_redacted"] = "true"

        response = h.handler(
            _api_event(
                body={
                    "league_id": LEAGUE_ID,
                    "report_type": "weekly",
                    "period": "2026W04",
                    "flag": "is_redacted",
                    "value": False,
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        audit = patched_handler["audit_calls"][0]
        assert audit["before"] == {"is_redacted": "true"}
        assert audit["after"] == {"is_redacted": "false"}

    def test_no_audit_on_update_metadata_failure(self, patched_handler) -> None:
        from lambdas.api_admin_reports_flag import handler as h

        patched_handler["raise_on_update"] = True

        response = h.handler(
            _api_event(
                body={
                    "league_id": LEAGUE_ID,
                    "report_type": "weekly",
                    "period": "2026W04",
                    "flag": "is_redacted",
                    "value": True,
                }
            ),
            context=None,
        )
        # The DynamoDBError surfaces as a 500.
        assert response["statusCode"] == 500
        # Audit NOT written — the metadata write failed.
        assert patched_handler["audit_calls"] == []

    def test_audit_failure_does_not_break_response(self, patched_handler) -> None:
        from lambdas.api_admin_reports_flag import handler as h

        patched_handler["audit_return"] = None

        response = h.handler(
            _api_event(
                body={
                    "league_id": LEAGUE_ID,
                    "report_type": "weekly",
                    "period": "2026W04",
                    "flag": "do_not_broadcast",
                    "value": True,
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        # The flag was still toggled.
        assert body["metadata"]["do_not_broadcast"] == "true"
