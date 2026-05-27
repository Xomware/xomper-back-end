"""
Cross-orchestrator broadcast_at-stamp tests (admin-portal F3).

Asserts the canonical broadcast-success stamp pattern across all
three orchestrators (post-draft, preseason, weekly):

  - `dry_run=False` + successful SES fan-out -> `stamp_broadcast_at`
    is called exactly once.
  - `dry_run=False` + SES raises -> NO stamp written. Partial-failure
    safety: a fan-out that blew up halfway through must NOT mark the
    report as broadcast.
  - `dry_run=True` -> stamp helper is never called regardless of
    delivery success (dry-run isn't a broadcast).

Re-uses the existing fixtures via pytest import (same shape as
`test_orchestrator_dnb_abort.py`).
"""
from __future__ import annotations

import pytest

from tests.test_api_admin_ai_review_postdraft_trigger import (  # noqa: F401
    patched_orchestrator as patched_postdraft,
)
from tests.test_api_admin_ai_review_preseason_trigger import (  # noqa: F401
    patched_orchestrator as patched_preseason,
)
from tests.test_api_admin_ai_review_weekly_trigger import (  # noqa: F401
    patched_orchestrator as patched_weekly,
)


def _broadcast_at_updates(state) -> list[dict]:
    """Filter `metadata_updates` down to just the broadcast_at writes."""
    return [u for u in state["metadata_updates"] if "broadcast_at" in u["partial"]]


# ---------------------------------------------------------------------------
# Post-draft
# ---------------------------------------------------------------------------


class TestPostdraftStamp:
    def test_stamps_on_broadcast_success(self, patched_postdraft) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

        run(dry_run=False, force=True)
        stamps = _broadcast_at_updates(patched_postdraft)
        assert len(stamps) == 1
        assert stamps[0]["partial"]["broadcast_at"]
        # Stamp goes to the right key.
        assert stamps[0]["report_type"] == "postDraft"

    def test_no_stamp_on_dry_run(self, patched_postdraft) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

        run(dry_run=True, force=False)
        assert _broadcast_at_updates(patched_postdraft) == []

    def test_no_stamp_on_ses_failure(
        self, patched_postdraft, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger import orchestrator

        def _raise(tasks):
            raise RuntimeError("SES is on fire")

        monkeypatch.setattr(orchestrator, "send_emails_concurrently", _raise)

        with pytest.raises(RuntimeError):
            orchestrator.run(dry_run=False, force=True)

        # No broadcast_at stamp was written because the SES call raised
        # before the stamp helper would run.
        assert _broadcast_at_updates(patched_postdraft) == []


# ---------------------------------------------------------------------------
# Preseason
# ---------------------------------------------------------------------------


class TestPreseasonStamp:
    def test_stamps_on_broadcast_success(self, patched_preseason) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        run(dry_run=False, force=True)
        stamps = _broadcast_at_updates(patched_preseason)
        assert len(stamps) == 1
        assert stamps[0]["partial"]["broadcast_at"]
        assert stamps[0]["report_type"] == "preseason"

    def test_no_stamp_on_dry_run(self, patched_preseason) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        run(dry_run=True, force=False)
        assert _broadcast_at_updates(patched_preseason) == []

    def test_no_stamp_on_ses_failure(
        self, patched_preseason, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger import orchestrator

        def _raise(tasks):
            raise RuntimeError("SES is on fire")

        monkeypatch.setattr(orchestrator, "send_emails_concurrently", _raise)

        with pytest.raises(RuntimeError):
            orchestrator.run(dry_run=False, force=True)

        assert _broadcast_at_updates(patched_preseason) == []


# ---------------------------------------------------------------------------
# Weekly
# ---------------------------------------------------------------------------


class TestWeeklyStamp:
    def test_stamps_on_broadcast_success(self, patched_weekly) -> None:
        from lambdas.common.weekly_orchestrator import run_weekly

        run_weekly(dry_run=False, force=True)
        stamps = _broadcast_at_updates(patched_weekly)
        assert len(stamps) == 1
        assert stamps[0]["partial"]["broadcast_at"]
        assert stamps[0]["report_type"] == "weekly"

    def test_no_stamp_on_dry_run(self, patched_weekly) -> None:
        from lambdas.common.weekly_orchestrator import run_weekly

        run_weekly(dry_run=True, force=False)
        assert _broadcast_at_updates(patched_weekly) == []

    def test_no_stamp_on_ses_failure(
        self, patched_weekly, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import weekly_orchestrator

        def _raise(tasks):
            raise RuntimeError("SES is on fire")

        monkeypatch.setattr(weekly_orchestrator, "send_emails_concurrently", _raise)

        with pytest.raises(RuntimeError):
            weekly_orchestrator.run_weekly(dry_run=False, force=True)

        assert _broadcast_at_updates(patched_weekly) == []
