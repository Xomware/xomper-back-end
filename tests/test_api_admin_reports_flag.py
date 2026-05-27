"""
Tests for `api_admin_reports_flag` (admin-portal F3).

Endpoint shape: POST /admin/reports-flag, body
  { league_id, report_type, period, flag, value }

Covers:
  - 403 when the caller is not an admin.
  - 400 on missing fields (league_id, report_type, period, flag, value).
  - 400 on invalid flag name (anything other than `is_redacted` /
    `do_not_broadcast`).
  - 400 when `value` is not a real bool (e.g. "yes" string, int).
  - 404 when the target report doesn't exist.
  - Happy path: both allowed flags toggle through and the response
    carries the FULL updated metadata blob.
  - Round-trip: set then unset writes the expected string values.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


ADMIN_ID = "594625531702460416"
LEAGUE_ID = "LEAGUE_ID"


def _api_event(
    *,
    body: dict[str, Any] | None = None,
    sleeper_user_id: str | None = ADMIN_ID,
) -> dict[str, Any]:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "POST",
        "path": "/admin/reports-flag",
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


def _report_row() -> dict[str, Any]:
    return {
        "pk": f"LEAGUE#{LEAGUE_ID}",
        "sk": "REPORT#weekly#2026W04",
        "league_id": LEAGUE_ID,
        "report_type": "weekly",
        "period": "2026W04",
        "body_markdown": "# Weekly recap\n\nWeek 4.\n",
        "metadata": {"model": "claude-haiku-4-5"},
        "created_at": "2026-10-01T15:00:00Z",
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    """Wire every external seam the handler touches. Returns the
    mutable `state` dict so tests can flip individual hooks (e.g.
    drop the report to hit the 404 path)."""
    from lambdas.api_admin_reports_flag import handler as h

    state: dict[str, Any] = {
        "admin_row": _admin_row(),
        "report_row": _report_row(),
        # Each invocation of `update_metadata` appends one entry here.
        "metadata_writes": [],
    }

    def _require_admin(event, body=None):
        return state["admin_row"]

    def _get_report(*, league_id, report_type, period):
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

    def _update_metadata(*, league_id, report_type, period, partial):
        # Mutate the canned row's metadata so the round-trip test can
        # observe the merged shape.
        if state["report_row"] is not None:
            state["report_row"]["metadata"].update(partial)
        state["metadata_writes"].append(
            {
                "league_id": league_id,
                "report_type": report_type,
                "period": period,
                "partial": partial,
            }
        )
        return {
            "pk": f"LEAGUE#{league_id}",
            "sk": f"REPORT#{report_type}#{period}",
            "league_id": league_id,
            "report_type": report_type,
            "period": period,
            "metadata": dict(
                (state["report_row"] or {}).get("metadata") or {}
            ),
        }

    monkeypatch.setattr(h, "require_admin", _require_admin)
    monkeypatch.setattr(h.ai_reports_store, "get_report", _get_report)
    monkeypatch.setattr(h.ai_reports_store, "update_metadata", _update_metadata)

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
        from lambdas.api_admin_reports_flag import handler as h
        from lambdas.common.admin_gate import NotAdmin

        def _raise(event, body=None):
            raise NotAdmin("not authorized")

        monkeypatch.setattr(h, "require_admin", _raise)

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
        assert response["statusCode"] == 403
        body = json.loads(response["body"])
        assert body["Success"] is False
        # No metadata write occurred.
        assert patched_handler["metadata_writes"] == []


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestInputValidation:
    @pytest.mark.parametrize(
        "missing_field,body",
        [
            (
                "league_id",
                {"report_type": "weekly", "period": "2026W04", "flag": "is_redacted", "value": True},
            ),
            (
                "report_type",
                {"league_id": LEAGUE_ID, "period": "2026W04", "flag": "is_redacted", "value": True},
            ),
            (
                "period",
                {"league_id": LEAGUE_ID, "report_type": "weekly", "flag": "is_redacted", "value": True},
            ),
            (
                "flag",
                {"league_id": LEAGUE_ID, "report_type": "weekly", "period": "2026W04", "value": True},
            ),
        ],
    )
    def test_missing_field_returns_400(
        self, patched_handler, missing_field: str, body: dict[str, Any]
    ) -> None:
        from lambdas.api_admin_reports_flag import handler as h

        response = h.handler(_api_event(body=body), context=None)
        assert response["statusCode"] == 400
        parsed = json.loads(response["body"])
        assert parsed["Success"] is False
        assert missing_field in parsed["Message"]
        assert patched_handler["metadata_writes"] == []

    def test_missing_value_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_reports_flag import handler as h

        response = h.handler(
            _api_event(
                body={
                    "league_id": LEAGUE_ID,
                    "report_type": "weekly",
                    "period": "2026W04",
                    "flag": "is_redacted",
                    # value omitted -> None -> 400
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        parsed = json.loads(response["body"])
        assert parsed["Success"] is False
        assert "value" in parsed["Message"].lower()

    def test_unknown_flag_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_reports_flag import handler as h

        response = h.handler(
            _api_event(
                body={
                    "league_id": LEAGUE_ID,
                    "report_type": "weekly",
                    "period": "2026W04",
                    "flag": "bogus_flag",
                    "value": True,
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        parsed = json.loads(response["body"])
        assert parsed["Success"] is False
        assert "bogus_flag" in parsed["Message"]
        assert patched_handler["metadata_writes"] == []

    def test_non_bool_value_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_reports_flag import handler as h

        response = h.handler(
            _api_event(
                body={
                    "league_id": LEAGUE_ID,
                    "report_type": "weekly",
                    "period": "2026W04",
                    "flag": "is_redacted",
                    "value": "yes",  # not a real bool
                }
            ),
            context=None,
        )
        assert response["statusCode"] == 400
        parsed = json.loads(response["body"])
        assert parsed["Success"] is False
        assert "bool" in parsed["Message"].lower()
        assert patched_handler["metadata_writes"] == []


# ---------------------------------------------------------------------------
# Lookup failures
# ---------------------------------------------------------------------------


class TestLookupFailures:
    def test_unknown_report_returns_404(self, patched_handler) -> None:
        from lambdas.api_admin_reports_flag import handler as h

        patched_handler["report_row"] = None  # drop the canned row

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
        assert response["statusCode"] == 404
        parsed = json.loads(response["body"])
        assert parsed["Success"] is False
        assert "not found" in parsed["Message"].lower()
        assert patched_handler["metadata_writes"] == []


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_set_is_redacted_true(self, patched_handler) -> None:
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
        parsed = json.loads(response["body"])
        assert parsed["Success"] is True
        assert parsed["league_id"] == LEAGUE_ID
        assert parsed["report_type"] == "weekly"
        assert parsed["period"] == "2026W04"
        assert parsed["flag"] == "is_redacted"
        assert parsed["value"] is True
        # Persists as string "true" so the iOS flat-map accessor reads
        # it back identically to the Dynamo BOOL collapse.
        assert parsed["metadata"]["is_redacted"] == "true"
        # Existing metadata keys preserved through the merge.
        assert parsed["metadata"]["model"] == "claude-haiku-4-5"

        # Exactly one update_metadata call with the expected partial.
        assert len(patched_handler["metadata_writes"]) == 1
        write = patched_handler["metadata_writes"][0]
        assert write["partial"] == {"is_redacted": "true"}

    def test_set_do_not_broadcast_true(self, patched_handler) -> None:
        from lambdas.api_admin_reports_flag import handler as h

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
        parsed = json.loads(response["body"])
        assert parsed["Success"] is True
        assert parsed["flag"] == "do_not_broadcast"
        assert parsed["metadata"]["do_not_broadcast"] == "true"
        write = patched_handler["metadata_writes"][0]
        assert write["partial"] == {"do_not_broadcast": "true"}

    def test_round_trip_set_then_unset(self, patched_handler) -> None:
        from lambdas.api_admin_reports_flag import handler as h

        # Set
        resp1 = h.handler(
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
        assert resp1["statusCode"] == 200
        parsed1 = json.loads(resp1["body"])
        assert parsed1["metadata"]["is_redacted"] == "true"

        # Unset
        resp2 = h.handler(
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
        assert resp2["statusCode"] == 200
        parsed2 = json.loads(resp2["body"])
        assert parsed2["metadata"]["is_redacted"] == "false"

        # Two writes; first set, second unset.
        writes = patched_handler["metadata_writes"]
        assert len(writes) == 2
        assert writes[0]["partial"] == {"is_redacted": "true"}
        assert writes[1]["partial"] == {"is_redacted": "false"}
