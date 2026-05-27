"""
Cross-orchestrator DNB-abort tests (admin-portal F3).

Asserts that all three broadcast orchestrators (post-draft, preseason,
weekly) re-read the report's metadata immediately before SES fan-out
and abort with `DoNotBroadcastError` (HTTP 409) when
`metadata.do_not_broadcast == "true"`.

Also covers the inverse: the dry-run path is unaffected, and the
normal broadcast path runs end-to-end when the flag is absent.

Re-uses the existing test fixtures via pytest's fixture system so we
don't rebuild the entire patched-orchestrator surface. Each fixture
sets a `fresh_metadata` override on its `state` dict that the
fixture's `_get_report` stub uses to inject the DNB flag on the
post-write re-read.
"""
from __future__ import annotations

import pytest

# Re-export the existing fixtures so this module's tests can reuse them.
from tests.test_api_admin_ai_review_postdraft_trigger import (  # noqa: F401
    patched_orchestrator as patched_postdraft,
)
from tests.test_api_admin_ai_review_preseason_trigger import (  # noqa: F401
    patched_orchestrator as patched_preseason,
)
from tests.test_api_admin_ai_review_weekly_trigger import (  # noqa: F401
    patched_orchestrator as patched_weekly,
)


# ---------------------------------------------------------------------------
# Post-draft
# ---------------------------------------------------------------------------


class TestPostdraftDNB:
    def test_dnb_aborts_broadcast(self, patched_postdraft) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run
        from lambdas.common.errors import DoNotBroadcastError

        # Override the post-write re-read to surface do_not_broadcast=true.
        patched_postdraft["fresh_metadata"] = {"do_not_broadcast": "true"}

        with pytest.raises(DoNotBroadcastError):
            run(dry_run=False, force=False)

        # SES fan-out never ran.
        assert patched_postdraft["emails_sent"] == []
        assert patched_postdraft["pushes_sent"] == []
        # broadcast_at was NEVER stamped because abort happened first.
        assert all(
            "broadcast_at" not in u["partial"]
            for u in patched_postdraft["metadata_updates"]
        )

    def test_dnb_does_not_abort_dry_run(self, patched_postdraft) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

        patched_postdraft["fresh_metadata"] = {"do_not_broadcast": "true"}

        # Dry-run path skips the DNB check entirely.
        result = run(dry_run=True, force=False)
        assert result["status"] == "dry_run_sent"
        # Admin still gets the dry-run email.
        assert len(patched_postdraft["emails_sent"]) == 1

    def test_no_dnb_flag_runs_broadcast_normally(self, patched_postdraft) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

        # No fresh_metadata override -> get_report returns the write
        # as-is (no do_not_broadcast key).
        result = run(dry_run=False, force=True)

        assert result["status"] == "broadcast"
        assert result["delivery_count"] == 12
        # broadcast_at was stamped.
        assert any(
            "broadcast_at" in u["partial"]
            for u in patched_postdraft["metadata_updates"]
        )


# ---------------------------------------------------------------------------
# Preseason
# ---------------------------------------------------------------------------


class TestPreseasonDNB:
    def test_dnb_aborts_broadcast(self, patched_preseason) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run
        from lambdas.common.errors import DoNotBroadcastError

        patched_preseason["fresh_metadata"] = {"do_not_broadcast": "true"}

        with pytest.raises(DoNotBroadcastError):
            run(dry_run=False, force=False)

        assert patched_preseason["emails_sent"] == []
        assert patched_preseason["pushes_sent"] == []
        assert all(
            "broadcast_at" not in u["partial"]
            for u in patched_preseason["metadata_updates"]
        )

    def test_dnb_does_not_abort_dry_run(self, patched_preseason) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        patched_preseason["fresh_metadata"] = {"do_not_broadcast": "true"}

        result = run(dry_run=True, force=False)
        assert result["status"] == "dry_run_sent"
        assert len(patched_preseason["emails_sent"]) == 1

    def test_no_dnb_flag_runs_broadcast_normally(self, patched_preseason) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        result = run(dry_run=False, force=True)
        assert result["status"] == "broadcast"
        assert result["delivery_count"] == 12
        assert any(
            "broadcast_at" in u["partial"]
            for u in patched_preseason["metadata_updates"]
        )


# ---------------------------------------------------------------------------
# Weekly
# ---------------------------------------------------------------------------


class TestWeeklyDNB:
    def test_dnb_aborts_broadcast(self, patched_weekly) -> None:
        from lambdas.common.errors import DoNotBroadcastError
        from lambdas.common.weekly_orchestrator import run_weekly

        patched_weekly["fresh_metadata"] = {"do_not_broadcast": "true"}

        with pytest.raises(DoNotBroadcastError):
            run_weekly(dry_run=False, force=False)

        assert patched_weekly["emails_sent"] == []
        assert patched_weekly["pushes_sent"] == []
        # Memory_count_out update can happen pre-DNB-check, but the
        # broadcast_at stamp must not.
        assert all(
            "broadcast_at" not in u["partial"]
            for u in patched_weekly["metadata_updates"]
        )

    def test_dnb_does_not_abort_dry_run(self, patched_weekly) -> None:
        from lambdas.common.weekly_orchestrator import run_weekly

        patched_weekly["fresh_metadata"] = {"do_not_broadcast": "true"}

        result = run_weekly(dry_run=True, force=False)
        assert result["status"] == "dry_run_sent"
        assert len(patched_weekly["emails_sent"]) == 1

    def test_no_dnb_flag_runs_broadcast_normally(self, patched_weekly) -> None:
        from lambdas.common.weekly_orchestrator import run_weekly

        result = run_weekly(dry_run=False, force=True)
        assert result["status"] == "broadcast"
        assert result["delivery_count"] == 12
        assert any(
            "broadcast_at" in u["partial"]
            for u in patched_weekly["metadata_updates"]
        )
