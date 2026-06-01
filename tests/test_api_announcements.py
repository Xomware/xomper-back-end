"""
Tests for `api_announcements` (public-read announcements endpoint).

Endpoint shape: GET /announcements/list

Covers:
  - Happy path: returns rows from `list_active` with count.
  - Empty payload: gracefully returns 200 + empty list (iOS falls
    back to its hardcoded array).
  - Module-level 5-min cache: first request hits the store, second
    request within TTL reuses the cached payload.
  - Cache miss after TTL expires (fast-forward via monkeypatched
    `time.monotonic`).
  - Public endpoint — NO admin gate. (The authorizer is at the API
    Gateway level; the lambda itself doesn't check is_admin.)
"""
from __future__ import annotations

import json
from typing import Any

import pytest


def _api_event() -> dict[str, Any]:
    return {
        "httpMethod": "GET",
        "path": "/announcements/list",
        "headers": {},
        "queryStringParameters": {},
    }


def _row(id_: str, title: str = "T") -> dict[str, Any]:
    return {
        "id": id_,
        "title": title,
        "body": "body",
        "priority": "info",
        "expires_at": None,
        "is_active": True,
        "display_order": 0,
        "created_at": "2026-05-01T12:00:00+00:00",
    }


@pytest.fixture
def patched_handler(monkeypatch: pytest.MonkeyPatch):
    from lambdas.api_announcements import handler as h

    state: dict[str, Any] = {
        "rows": [_row("a"), _row("b")],
        "list_calls": 0,
        "now": 1000.0,  # mock monotonic clock
    }

    def _list_active():
        state["list_calls"] += 1
        return list(state["rows"])

    def _monotonic():
        return state["now"]

    monkeypatch.setattr(h, "list_active", _list_active)
    monkeypatch.setattr(h.time, "monotonic", _monotonic)

    # Reset module cache between tests.
    h._reset_cache_for_tests()
    return state


class TestHappyPath:
    def test_returns_rows_from_store(self, patched_handler) -> None:
        from lambdas.api_announcements import handler as h

        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["count"] == 2
        assert [r["id"] for r in body["rows"]] == ["a", "b"]
        assert patched_handler["list_calls"] == 1

    def test_empty_payload_returns_200_with_zero_count(
        self, patched_handler
    ) -> None:
        """Best-effort: when the store returns [] (Supabase down or
        table missing), the endpoint still returns 200 so iOS gets a
        clean signal to use its hardcoded fallback."""
        from lambdas.api_announcements import handler as h

        patched_handler["rows"] = []
        response = h.handler(_api_event(), context=None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["count"] == 0
        assert body["rows"] == []


class TestModuleCache:
    def test_second_call_within_ttl_uses_cache(self, patched_handler) -> None:
        from lambdas.api_announcements import handler as h

        # First call — cache miss.
        h.handler(_api_event(), context=None)
        # Second call shortly after — cache hit (no new list_active call).
        patched_handler["now"] += 60.0  # 60s later
        h.handler(_api_event(), context=None)

        assert patched_handler["list_calls"] == 1

    def test_call_after_ttl_refreshes_cache(self, patched_handler) -> None:
        from lambdas.api_announcements import handler as h

        # First call — cache miss.
        h.handler(_api_event(), context=None)
        # Fast-forward past the 5-min TTL.
        patched_handler["now"] += 301.0
        h.handler(_api_event(), context=None)

        assert patched_handler["list_calls"] == 2

    def test_empty_rows_skip_cache(self, patched_handler) -> None:
        """If list_active returns [] (e.g. Supabase outage), we don't
        cache the empty payload — the next request retries the store
        so a transient failure doesn't pin us to empty for 5 min."""
        from lambdas.api_announcements import handler as h

        patched_handler["rows"] = []
        h.handler(_api_event(), context=None)
        # Even within the TTL window, second call should re-fetch.
        patched_handler["now"] += 30.0
        h.handler(_api_event(), context=None)
        assert patched_handler["list_calls"] == 2
