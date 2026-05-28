"""
Tests for ``api_admin_logs_query`` (admin-portal F5).

Endpoint shape: GET /admin/logs-query
    ?log_group=&level=&search=&limit=&next_token=

Covers:
- 403 when caller is not admin.
- 400 when ``log_group`` is missing.
- 400 when ``log_group`` is not in ``ADMIN_LOG_GROUP_ALLOWLIST``
  (defense-in-depth; IAM also enforces).
- Happy path: mocked boto3 ``filter_log_events`` returns events;
  handler redacts PII in the returned ``message`` field; response
  shape matches the wire contract.
- ``search`` query param is wrapped in a quoted ``filterPattern`` and
  passed through to boto3.
- ``level`` filter is applied AFTER fetch via the heuristic substring
  scan — only matching events are returned.
- Cache hit: two identical first-page calls within 60s → boto3 is
  called once.
- Cache miss after TTL: a synthetic time jump past 60s → boto3 is
  called again.
- ``next_token`` bypasses the cache entirely.
- ``limit`` parsing: > 200 clamps to 200, < 1 clamps to 1,
  unparseable falls back to the default 50.
- Heuristic level detection: messages containing ``ERROR`` /
  ``WARN`` / ``INFO`` substrings get the expected level tag.
"""
from __future__ import annotations

import json
import time
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
        "path": "/admin/logs-query",
        "headers": headers,
        "queryStringParameters": qs or {},
    }


def _cw_event(
    *,
    event_id: str,
    timestamp_ms: int,
    message: str,
) -> dict[str, Any]:
    return {
        "eventId": event_id,
        "timestamp": timestamp_ms,
        "message": message,
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    """Stubs the admin gate to a fixed admin row, replaces the boto3
    CloudWatch Logs client with an in-memory stub, and clears the
    module-level cache to isolate runs."""
    from lambdas.api_admin_logs_query import handler as h

    h._cache_clear()

    state: dict[str, Any] = {
        "admin_row": {
            "id": "row-admin",
            "sleeper_user_id": ADMIN_ID,
            "is_admin": True,
            "is_active": True,
        },
        # The script of what boto3 returns. The fixture sets a default;
        # individual tests override it.
        "cw_response": {
            "events": [
                _cw_event(
                    event_id="e1",
                    timestamp_ms=1748340000000,  # 2025-05-27T...
                    message="INFO Starting weekly recap for league xyz.",
                ),
                _cw_event(
                    event_id="e2",
                    timestamp_ms=1748340060000,
                    message=(
                        "ERROR failed for user@example.com "
                        "(id 594625531702460416)"
                    ),
                ),
            ],
            "nextToken": None,
        },
        "boto_calls": [],
        # Monkey-patched time source for cache TTL tests.
        "now": time.time(),
    }

    class _LogsClientStub:
        def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:
            state["boto_calls"].append(kwargs)
            return state["cw_response"]

    def _require_admin(event: dict[str, Any], body: Any = None) -> dict[str, Any]:
        return state["admin_row"]

    monkeypatch.setattr(h, "require_admin", _require_admin)
    # Inject the stub by overriding the lazy boto3 client accessor.
    monkeypatch.setattr(h, "_LOGS_CLIENT", _LogsClientStub())
    # Time control for cache tests.
    monkeypatch.setattr(h.time, "time", lambda: state["now"])

    return state


# ---------------------------------------------------------------------------
# Auth gate
# ---------------------------------------------------------------------------


class TestAdminGate:
    def test_non_admin_returns_403(
        self, patched_handler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_logs_query import handler as h
        from lambdas.common.admin_gate import NotAdmin

        monkeypatch.setattr(
            h,
            "require_admin",
            lambda event, body=None: (_ for _ in ()).throw(NotAdmin("nope")),
        )

        response = h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}),
            context=None,
        )
        assert response["statusCode"] == 403
        assert patched_handler["boto_calls"] == []


# ---------------------------------------------------------------------------
# Allowlist validation (defense in depth)
# ---------------------------------------------------------------------------


class TestAllowlistValidation:
    def test_missing_log_group_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        response = h.handler(_api_event(qs={}), context=None)
        assert response["statusCode"] == 400
        parsed = json.loads(response["body"])
        assert parsed["Success"] is False
        assert "log_group" in parsed["Message"].lower()
        assert "allowed" in parsed
        assert "ai-review-weekly" in parsed["allowed"]
        assert patched_handler["boto_calls"] == []

    def test_unknown_log_group_returns_400(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        response = h.handler(
            _api_event(qs={"log_group": "not-a-real-group"}),
            context=None,
        )
        assert response["statusCode"] == 400
        parsed = json.loads(response["body"])
        assert parsed["Success"] is False
        assert "not allowlisted" in parsed["Message"]
        assert patched_handler["boto_calls"] == []

    def test_all_10_allowlisted_keys_pass(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h
        from lambdas.common.constants import ADMIN_LOG_GROUP_ALLOWLIST

        # Sanity: the allowlist hasn't drifted from 10.
        assert len(ADMIN_LOG_GROUP_ALLOWLIST) == 10

        for slug in ADMIN_LOG_GROUP_ALLOWLIST:
            h._cache_clear()
            response = h.handler(
                _api_event(qs={"log_group": slug}),
                context=None,
            )
            assert response["statusCode"] == 200, slug


# ---------------------------------------------------------------------------
# Happy path + wire contract + redaction
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_returns_redacted_events_with_iso_timestamp_and_level(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_logs_query import handler as h

        response = h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}),
            context=None,
        )
        assert response["statusCode"] == 200
        parsed = json.loads(response["body"])

        assert parsed["Success"] is True
        assert parsed["log_group"] == "ai-review-weekly"
        assert parsed["next_token"] is None
        assert len(parsed["events"]) == 2

        # Wire contract.
        first = parsed["events"][0]
        assert set(first.keys()) == {"id", "timestamp", "level", "message"}
        assert first["id"] == "e1"
        assert first["timestamp"].startswith("20")  # ISO-8601
        assert first["level"] == "INFO"
        assert first["message"].startswith("INFO Starting")

        # Redaction applied to the second event.
        second = parsed["events"][1]
        assert second["level"] == "ERROR"
        assert "user@example.com" not in second["message"]
        assert "594625531702460416" not in second["message"]
        assert "***@***" in second["message"]
        assert "[uid]" in second["message"]

    def test_boto_called_with_resolved_log_group_name(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(_api_event(qs={"log_group": "weekly-recap"}), context=None)
        call = patched_handler["boto_calls"][0]
        assert call["logGroupName"] == "/aws/lambda/xomper-notif-weekly-recap"
        assert call["limit"] == 50
        # No search → no filterPattern.
        assert "filterPattern" not in call
        # No pagination → no nextToken.
        assert "nextToken" not in call


# ---------------------------------------------------------------------------
# Search + level filters
# ---------------------------------------------------------------------------


class TestSearchFilter:
    def test_search_passes_through_as_quoted_filter_pattern(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly", "search": "draft"}),
            context=None,
        )
        call = patched_handler["boto_calls"][0]
        assert call["filterPattern"] == '"draft"'

    def test_search_with_embedded_quote_is_escaped(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(
                qs={"log_group": "ai-review-weekly", "search": 'foo "bar" baz'}
            ),
            context=None,
        )
        call = patched_handler["boto_calls"][0]
        assert call["filterPattern"] == '"foo \\"bar\\" baz"'


class TestLevelFilter:
    def test_level_error_excludes_info_events(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        response = h.handler(
            _api_event(
                qs={"log_group": "ai-review-weekly", "level": "error"}
            ),
            context=None,
        )
        parsed = json.loads(response["body"])
        # Only the ERROR event survives the post-fetch cut.
        assert len(parsed["events"]) == 1
        assert parsed["events"][0]["level"] == "ERROR"

    def test_level_warning_alias_normalises_to_warn(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_logs_query import handler as h

        patched_handler["cw_response"] = {
            "events": [
                _cw_event(
                    event_id="w1",
                    timestamp_ms=1748340000000,
                    message="WARNING something fishy",
                ),
                _cw_event(
                    event_id="i1",
                    timestamp_ms=1748340000001,
                    message="INFO normal flow",
                ),
            ],
            "nextToken": None,
        }

        response = h.handler(
            _api_event(
                qs={"log_group": "ai-review-weekly", "level": "warning"}
            ),
            context=None,
        )
        parsed = json.loads(response["body"])
        assert len(parsed["events"]) == 1
        assert parsed["events"][0]["id"] == "w1"
        assert parsed["events"][0]["level"] == "WARN"

    def test_invalid_level_value_is_silently_ignored(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_logs_query import handler as h

        response = h.handler(
            _api_event(
                qs={"log_group": "ai-review-weekly", "level": "fatal"}
            ),
            context=None,
        )
        parsed = json.loads(response["body"])
        # Both events come back — invalid level means "no filter".
        assert len(parsed["events"]) == 2


class TestLevelDetectionHeuristic:
    def test_error_substring(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        patched_handler["cw_response"] = {
            "events": [
                _cw_event(
                    event_id="e1",
                    timestamp_ms=1748340000000,
                    message="[ERROR] something blew up",
                )
            ],
            "nextToken": None,
        }

        response = h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}),
            context=None,
        )
        parsed = json.loads(response["body"])
        assert parsed["events"][0]["level"] == "ERROR"

    def test_warn_and_warning_both_detected(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        patched_handler["cw_response"] = {
            "events": [
                _cw_event(
                    event_id="w1",
                    timestamp_ms=1748340000000,
                    message="WARNING about a thing",
                ),
                _cw_event(
                    event_id="w2",
                    timestamp_ms=1748340000001,
                    message="[WARN] another thing",
                ),
            ],
            "nextToken": None,
        }

        response = h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}),
            context=None,
        )
        parsed = json.loads(response["body"])
        assert parsed["events"][0]["level"] == "WARN"
        assert parsed["events"][1]["level"] == "WARN"

    def test_unknown_level_returns_null(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        patched_handler["cw_response"] = {
            "events": [
                _cw_event(
                    event_id="u1",
                    timestamp_ms=1748340000000,
                    message="just some text without any level marker",
                )
            ],
            "nextToken": None,
        }

        response = h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}),
            context=None,
        )
        parsed = json.loads(response["body"])
        assert parsed["events"][0]["level"] is None


# ---------------------------------------------------------------------------
# Cache behaviour
# ---------------------------------------------------------------------------


class TestCache:
    def test_two_identical_calls_within_ttl_hit_boto_once(
        self, patched_handler
    ) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}), context=None
        )
        # Advance time within the TTL window.
        patched_handler["now"] += 30
        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}), context=None
        )

        assert len(patched_handler["boto_calls"]) == 1

    def test_call_after_ttl_refetches(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}), context=None
        )
        # Past TTL.
        patched_handler["now"] += 61
        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}), context=None
        )

        assert len(patched_handler["boto_calls"]) == 2

    def test_different_keys_cache_independently(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}), context=None
        )
        h.handler(
            _api_event(qs={"log_group": "weekly-recap"}), context=None
        )

        assert len(patched_handler["boto_calls"]) == 2

    def test_next_token_bypasses_cache(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        # Prime first page in cache.
        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}), context=None
        )
        assert len(patched_handler["boto_calls"]) == 1

        # Paginated call (same keys EXCEPT next_token) must go to boto3
        # every time, even though the cache key (log_group, level,
        # search, limit) is identical.
        h.handler(
            _api_event(
                qs={"log_group": "ai-review-weekly", "next_token": "abc"}
            ),
            context=None,
        )
        h.handler(
            _api_event(
                qs={"log_group": "ai-review-weekly", "next_token": "def"}
            ),
            context=None,
        )
        assert len(patched_handler["boto_calls"]) == 3
        # The paginated calls included nextToken in the boto args.
        assert patched_handler["boto_calls"][1]["nextToken"] == "abc"
        assert patched_handler["boto_calls"][2]["nextToken"] == "def"

    def test_search_variant_does_not_share_cache(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}), context=None
        )
        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly", "search": "x"}),
            context=None,
        )
        assert len(patched_handler["boto_calls"]) == 2


# ---------------------------------------------------------------------------
# Limit bounds
# ---------------------------------------------------------------------------


class TestLimitBounds:
    def test_limit_above_max_clamps_to_200(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly", "limit": "500"}),
            context=None,
        )
        assert patched_handler["boto_calls"][0]["limit"] == 200

    def test_limit_below_min_clamps_to_1(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly", "limit": "0"}),
            context=None,
        )
        assert patched_handler["boto_calls"][0]["limit"] == 1

    def test_invalid_limit_falls_back_to_default(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly", "limit": "abc"}),
            context=None,
        )
        assert patched_handler["boto_calls"][0]["limit"] == 50

    def test_limit_at_max_passes_through(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        h.handler(
            _api_event(qs={"log_group": "ai-review-weekly", "limit": "200"}),
            context=None,
        )
        assert patched_handler["boto_calls"][0]["limit"] == 200


# ---------------------------------------------------------------------------
# Pagination passthrough
# ---------------------------------------------------------------------------


class TestPagination:
    def test_next_token_in_response_is_surfaced(self, patched_handler) -> None:
        from lambdas.api_admin_logs_query import handler as h

        patched_handler["cw_response"] = {
            "events": [
                _cw_event(
                    event_id="p1",
                    timestamp_ms=1748340000000,
                    message="INFO page 1",
                )
            ],
            "nextToken": "cursor-abc",
        }

        response = h.handler(
            _api_event(qs={"log_group": "ai-review-weekly"}), context=None
        )
        parsed = json.loads(response["body"])
        assert parsed["next_token"] == "cursor-abc"
