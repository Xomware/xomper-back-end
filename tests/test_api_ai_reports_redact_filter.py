"""
Read-path redact-filter tests (admin-portal F3).

Covers:
  - `api_ai_reports_latest` returns the row to an admin caller even
    when `metadata.is_redacted == "true"`.
  - `api_ai_reports_latest` returns `report = None` (200 OK, the iOS
    empty-state path) to a non-admin caller when the row is redacted.
  - `api_ai_reports_list` includes redacted rows for admin callers.
  - `api_ai_reports_list` filters redacted rows out for non-admin
    callers, leaving the rest of the response shape stable.

External integrations (Supabase, DynamoDB ai_reports_store) are
monkeypatched.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
NON_ADMIN_ID = "u7"
LEAGUE_ID = "LEAGUE_ID"


def _event(*, sleeper_user_id: str | None = None, qs: dict[str, str] | None = None) -> dict[str, Any]:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "GET",
        "headers": headers,
        "queryStringParameters": qs or {},
    }


def _row(
    *,
    sk: str = "REPORT#weekly#2026W04",
    report_type: str = "weekly",
    period: str = "2026W04",
    redacted: bool = False,
    extra_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    meta: dict[str, Any] = {"model": "claude-haiku-4-5"}
    if redacted:
        meta["is_redacted"] = "true"
    if extra_meta:
        meta.update(extra_meta)
    return {
        "pk": f"LEAGUE#{LEAGUE_ID}",
        "sk": sk,
        "league_id": LEAGUE_ID,
        "report_type": report_type,
        "period": period,
        "body_markdown": f"# {report_type} {period}\n",
        "metadata": meta,
        "created_at": "2026-10-01T15:00:00Z",
    }


# ---------------------------------------------------------------------------
# `api_ai_reports_latest`
# ---------------------------------------------------------------------------


@pytest.fixture
def patched_latest(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_ai_reports_latest import handler as h

    state: dict[str, Any] = {
        "report": _row(redacted=True),
        "is_admin_response": False,
    }

    monkeypatch.setattr(
        h,
        "get_active_whitelisted_league",
        lambda: {"sleeper_league_id": LEAGUE_ID, "league_name": "CLT"},
    )
    monkeypatch.setattr(h, "get_latest", lambda lid, rt: state["report"])
    monkeypatch.setattr(h, "is_admin", lambda event: state["is_admin_response"])

    return state


class TestLatestRedactFilter:
    def test_admin_sees_redacted_report(self, patched_latest) -> None:
        from lambdas.api_ai_reports_latest import handler as h

        patched_latest["is_admin_response"] = True
        response = h.handler(
            _event(sleeper_user_id=ADMIN_ID, qs={"type": "weekly"}),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["report"] is not None
        assert body["report"]["metadata"]["is_redacted"] == "true"

    def test_non_admin_gets_null_for_redacted_report(
        self, patched_latest
    ) -> None:
        from lambdas.api_ai_reports_latest import handler as h

        patched_latest["is_admin_response"] = False
        response = h.handler(
            _event(sleeper_user_id=NON_ADMIN_ID, qs={"type": "weekly"}),
            context=None,
        )
        # 200 + null `report` so the iOS empty-state path is shared
        # with the legitimate "no row exists yet" case. No info leak.
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["report"] is None

    def test_non_admin_sees_non_redacted_report(
        self, patched_latest
    ) -> None:
        from lambdas.api_ai_reports_latest import handler as h

        patched_latest["report"] = _row(redacted=False)
        patched_latest["is_admin_response"] = False
        response = h.handler(
            _event(sleeper_user_id=NON_ADMIN_ID, qs={"type": "weekly"}),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["report"] is not None
        assert body["report"]["report_type"] == "weekly"


# ---------------------------------------------------------------------------
# `api_ai_reports_list`
# ---------------------------------------------------------------------------


@pytest.fixture
def patched_list(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_ai_reports_list import handler as h

    # 4 rows total; #2 and #4 are redacted.
    rows = [
        _row(sk="REPORT#weekly#2026W04", report_type="weekly", period="2026W04", redacted=False),
        _row(sk="REPORT#weekly#2026W03", report_type="weekly", period="2026W03", redacted=True),
        _row(sk="REPORT#weekly#2026W02", report_type="weekly", period="2026W02", redacted=False),
        _row(sk="REPORT#weekly#2026W01", report_type="weekly", period="2026W01", redacted=True),
    ]

    state: dict[str, Any] = {
        "rows": rows,
        "is_admin_response": False,
    }

    monkeypatch.setattr(
        h,
        "get_active_whitelisted_league",
        lambda: {"sleeper_league_id": LEAGUE_ID, "league_name": "CLT"},
    )

    def _list_recent(*, league_id, limit, cursor=None):
        return list(state["rows"]), None

    monkeypatch.setattr(h, "list_recent", _list_recent)
    monkeypatch.setattr(h, "is_admin", lambda event: state["is_admin_response"])

    return state


class TestListRedactFilter:
    def test_admin_sees_all_rows_including_redacted(self, patched_list) -> None:
        from lambdas.api_ai_reports_list import handler as h

        patched_list["is_admin_response"] = True
        response = h.handler(
            _event(sleeper_user_id=ADMIN_ID, qs={"limit": "10"}),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert len(body["rows"]) == 4
        # The two redacted rows are present.
        redacted = [r for r in body["rows"] if r["metadata"].get("is_redacted") == "true"]
        assert len(redacted) == 2

    def test_non_admin_redacted_rows_filtered_out(self, patched_list) -> None:
        from lambdas.api_ai_reports_list import handler as h

        patched_list["is_admin_response"] = False
        response = h.handler(
            _event(sleeper_user_id=NON_ADMIN_ID, qs={"limit": "10"}),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        # Only the 2 non-redacted rows.
        assert len(body["rows"]) == 2
        for row in body["rows"]:
            assert row["metadata"].get("is_redacted") != "true"

    def test_non_admin_type_filter_combines_with_redact_filter(
        self, patched_list
    ) -> None:
        from lambdas.api_ai_reports_list import handler as h

        # Add a post-draft row to confirm type filter still narrows.
        patched_list["rows"].append(
            _row(
                sk="REPORT#postDraft#2026",
                report_type="postDraft",
                period="2026",
                redacted=False,
            )
        )
        patched_list["is_admin_response"] = False
        response = h.handler(
            _event(
                sleeper_user_id=NON_ADMIN_ID,
                qs={"limit": "10", "type": "weekly"},
            ),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        # Only non-redacted weekly rows.
        assert len(body["rows"]) == 2
        assert all(r["report_type"] == "weekly" for r in body["rows"])
        assert all(
            r["metadata"].get("is_redacted") != "true" for r in body["rows"]
        )
