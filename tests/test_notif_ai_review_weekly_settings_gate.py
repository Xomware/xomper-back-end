"""
Tests for the admin-cron-settings gate inside `notif_ai_review_weekly`.

This lambda is structurally different from the other four — its
recipient filter lives inside the F3 weekly orchestrator's `dry_run`
path. The gate's job here is:
  - enabled=False → no-op skip.
  - test_mode=True → force `dry_run=True` on the orchestrator call,
    even if the inbound event passed `dry_run=False`.
"""
from __future__ import annotations

from typing import Any

import pytest


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    from lambdas.notif_ai_review_weekly import handler as h

    state: dict[str, Any] = {
        "cron_setting": {
            "cron_key": "notif_ai_review_weekly",
            "enabled": True,
            "test_mode": False,
            "description": "AI",
        },
        "run_calls": [],
    }

    def _get_cron_setting(cron_key: str):
        return dict(state["cron_setting"])

    def _run_weekly(**kwargs: Any) -> dict[str, Any]:
        state["run_calls"].append(kwargs)
        return {
            "status": "broadcast",
            "dry_run": kwargs.get("dry_run", False),
            "delivery_count": 1 if kwargs.get("dry_run") else 12,
            "model": "claude-haiku-4-5",
            "token_usage": {"input_tokens": 100, "output_tokens": 50},
            "week": kwargs.get("week") or 4,
            "period": "2026W04",
            "memory_count_in": 0,
            "memory_count_out": 0,
            "envelope_parsed": True,
        }

    monkeypatch.setattr(h, "get_cron_setting", _get_cron_setting)
    monkeypatch.setattr(h, "run_weekly", _run_weekly)
    return state


class TestDisabledShortCircuit:
    def test_disabled_returns_skipped_without_calling_orchestrator(
        self, patched
    ) -> None:
        from lambdas.notif_ai_review_weekly.handler import handler

        patched["cron_setting"]["enabled"] = False
        response = handler({}, context=None)

        assert response["statusCode"] == 200
        body = response["body"]
        if isinstance(body, str):
            import json

            body = json.loads(body)
        assert body["skipped"] is True
        assert body["reason"] == "disabled"
        # Orchestrator should not be invoked.
        assert patched["run_calls"] == []


class TestTestModeForcesDryRun:
    def test_test_mode_true_forces_dry_run(self, patched) -> None:
        from lambdas.notif_ai_review_weekly.handler import handler

        patched["cron_setting"]["test_mode"] = True
        handler({"dry_run": False}, context=None)

        assert len(patched["run_calls"]) == 1
        call = patched["run_calls"][0]
        # Even though the event explicitly passed dry_run=False, the
        # cron_settings test_mode flag must override to True.
        assert call["dry_run"] is True

    def test_test_mode_false_preserves_event_dry_run(self, patched) -> None:
        from lambdas.notif_ai_review_weekly.handler import handler

        patched["cron_setting"]["test_mode"] = False
        handler({"dry_run": True}, context=None)
        # Event-supplied dry_run still flows through.
        assert patched["run_calls"][0]["dry_run"] is True

    def test_default_settings_no_dry_run(self, patched) -> None:
        from lambdas.notif_ai_review_weekly.handler import handler

        handler({}, context=None)
        assert patched["run_calls"][0]["dry_run"] is False
