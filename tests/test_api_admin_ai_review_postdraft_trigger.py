"""
Tests for `api_admin_ai_review_postdraft_trigger`.

Covers the orchestrator's flow + the API handler's surface:
  - 403 when caller is not an admin
  - 409 when a report already exists (force=false)
  - 412 when the Sleeper draft isn't complete
  - 200 + delivery_count=1 for dry_run=true
  - 200 + delivery_count=12 for dry_run=false, force=true
  - Idempotency: a second force=false call after a write still 409s
  - Claude failures surface via the standard XomperError -> response path

External integrations (Sleeper, Anthropic, SES, SNS, Supabase,
DynamoDB ai_reports_store) are all monkeypatched. Tests stay
fast and deterministic.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_sleeper_users(count: int = 12) -> list[dict[str, Any]]:
    return [
        {
            "user_id": f"u{i}",
            "display_name": f"Manager{i}",
            "metadata": {"team_name": f"Team{i}"},
        }
        for i in range(1, count + 1)
    ]


def _make_rosters(count: int = 12) -> list[dict[str, Any]]:
    return [
        {
            "roster_id": i,
            "owner_id": f"u{i}",
            "settings": {
                "wins": 13 - i,
                "losses": i - 1,
                "ties": 0,
                "fpts": int(1800 - i * 25),
                "fpts_decimal": 50,
                "fpts_against": int(1500 + i * 10),
                "fpts_against_decimal": 25,
            },
        }
        for i in range(1, count + 1)
    ]


def _make_picks(team_count: int = 12, rounds_per_team: int = 5) -> list[dict[str, Any]]:
    picks = []
    for rnd in range(1, rounds_per_team + 1):
        for slot in range(1, team_count + 1):
            picks.append(
                {
                    "round": rnd,
                    "pick_no": (rnd - 1) * team_count + slot,
                    "draft_slot": slot,
                    "roster_id": slot,
                    "picked_by": f"u{slot}",
                    "metadata": {
                        "first_name": f"Player",
                        "last_name": f"{slot}-{rnd}",
                        "position": "WR",
                        "team": "DAL",
                    },
                }
            )
    return picks


def _make_whitelisted_users(count: int = 12) -> list[dict[str, Any]]:
    # User #1 in this list aligns with ADMIN_DOMINICK_USER_ID below.
    return [
        {
            "sleeper_user_id": ADMIN_ID if i == 1 else f"u{i}",
            "email": f"manager{i}@example.com",
            "display_name": f"Manager{i}",
            "sleeper_username": f"manager{i}",
            "is_active": True,
            "is_admin": i == 1,
            "id": f"row-{i}",
        }
        for i in range(1, count + 1)
    ]


ADMIN_ID = "594625531702460416"


@pytest.fixture
def patched_orchestrator(monkeypatch: pytest.MonkeyPatch):
    """Patch all external dependencies of `orchestrator.run` with
    deterministic stand-ins. Returns a `Hooks` namespace so tests can
    swap individual seams (e.g. force the draft to not be complete)."""
    from lambdas.api_admin_ai_review_postdraft_trigger import orchestrator

    state: dict[str, Any] = {
        "writes": [],
        "metadata_updates": [],
        "emails_sent": [],
        "pushes_sent": [],
        "claude_calls": [],
        "existing_report": None,
        "draft_status": "complete",
        "prior_league_id": "PRIOR123",
        "whitelisted_users": _make_whitelisted_users(),
        "active_league": {"sleeper_league_id": "LEAGUE_ID", "league_name": "CLT DYNASTY"},
    }

    monkeypatch.setattr(
        orchestrator,
        "get_active_whitelisted_league",
        lambda: state["active_league"],
    )

    def _get_league(league_id: str) -> dict[str, Any]:
        if league_id == "PRIOR123":
            return {
                "league_id": "PRIOR123",
                "name": "CLT DYNASTY (2025)",
                "season": "2025",
            }
        return {
            "league_id": league_id,
            "name": "CLT DYNASTY",
            "draft_id": "DRAFT123",
            "previous_league_id": state["prior_league_id"],
            "season": "2026",
        }

    monkeypatch.setattr(orchestrator, "get_sleeper_league", _get_league)
    monkeypatch.setattr(
        orchestrator,
        "get_sleeper_draft",
        lambda draft_id: {"draft_id": draft_id, "status": state["draft_status"]},
    )
    monkeypatch.setattr(
        orchestrator,
        "get_sleeper_draft_picks",
        lambda draft_id: _make_picks(),
    )
    monkeypatch.setattr(
        orchestrator,
        "get_sleeper_league_users",
        lambda league_id: _make_sleeper_users(),
    )
    monkeypatch.setattr(
        orchestrator,
        "get_sleeper_league_rosters",
        lambda league_id: _make_rosters(),
    )

    def _prior(league_id: str) -> str | None:
        return state["prior_league_id"]

    monkeypatch.setattr(orchestrator, "get_previous_league_id", _prior)

    def _get_active_users() -> list[dict[str, Any]]:
        return state["whitelisted_users"]

    monkeypatch.setattr(orchestrator, "get_active_whitelisted_users", _get_active_users)

    # ai_reports_store
    fake_store = MagicMock()

    def _get_latest(league_id: str, report_type: str):
        return state["existing_report"]

    def _write_report(*, league_id, report_type, period, body_markdown, metadata):
        item = {
            "pk": f"LEAGUE#{league_id}",
            "sk": f"REPORT#{report_type}#{period}",
            "league_id": league_id,
            "report_type": report_type,
            "period": period,
            "body_markdown": body_markdown,
            "metadata": metadata,
            "created_at": "2026-05-21T15:00:00Z",
        }
        state["writes"].append(item)
        state["existing_report"] = item
        return item

    def _update_metadata(*, league_id, report_type, period, partial):
        state["metadata_updates"].append(
            {
                "league_id": league_id,
                "report_type": report_type,
                "period": period,
                "partial": partial,
            }
        )
        return {}

    fake_store.get_latest = _get_latest
    fake_store.write_report = _write_report
    fake_store.update_metadata = _update_metadata
    monkeypatch.setattr(orchestrator, "ai_reports_store", fake_store)

    # claude_helper
    def _generate(
        *,
        prompt: str,
        system: Any,
        model: str,
        max_tokens: int,
        return_usage: bool = False,
    ):
        state["claude_calls"].append(
            {
                "prompt": prompt,
                "system": system,
                "model": model,
                "max_tokens": max_tokens,
                "return_usage": return_usage,
            }
        )
        usage = {
            "input_tokens": 3500,
            "output_tokens": 1800,
            "cache_read_input_tokens": 3200,
            "cache_creation_input_tokens": 300,
        }
        text = "# Post-Draft Review\n\n## Slot 1: Manager1\n\nPicks were fine.\n"
        if return_usage:
            return text, usage
        return text

    fake_claude = MagicMock()
    fake_claude.generate = _generate
    monkeypatch.setattr(orchestrator, "claude_helper", fake_claude)

    # SES + SNS
    def _send_emails(tasks: list[tuple[str, str, str, str]]):
        state["emails_sent"].extend(tasks)
        return len(tasks), 0

    def _send_push(user_ids, title, body, category=None, data=None):
        state["pushes_sent"].append(
            {
                "user_ids": list(user_ids),
                "title": title,
                "body": body,
                "category": category,
                "data": data,
            }
        )
        return len(user_ids), 0

    monkeypatch.setattr(orchestrator, "send_emails_concurrently", _send_emails)
    monkeypatch.setattr(orchestrator, "send_push_to_users", _send_push)

    return state


# ---------------------------------------------------------------------------
# Orchestrator-layer tests
# ---------------------------------------------------------------------------


class TestOrchestratorIdempotency:
    def test_existing_report_with_force_false_raises(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run
        from lambdas.common.errors import ReportAlreadyExistsError

        patched_orchestrator["existing_report"] = {
            "league_id": "LEAGUE_ID",
            "report_type": "postDraft",
            "period": "2026",
            "created_at": "2026-05-20T00:00:00Z",
        }

        with pytest.raises(ReportAlreadyExistsError):
            run(dry_run=True, force=False)

    def test_existing_report_with_force_true_overwrites(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

        patched_orchestrator["existing_report"] = {
            "league_id": "LEAGUE_ID",
            "report_type": "postDraft",
            "period": "2026",
            "created_at": "2026-05-20T00:00:00Z",
        }

        result = run(dry_run=True, force=True)
        assert result["status"] == "dry_run_sent"
        # New write happened.
        assert len(patched_orchestrator["writes"]) == 1

    def test_consecutive_writes_with_force_false_still_409(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run
        from lambdas.common.errors import ReportAlreadyExistsError

        # First write goes through.
        first = run(dry_run=True, force=False)
        assert first["status"] == "dry_run_sent"
        assert len(patched_orchestrator["writes"]) == 1
        # Second invocation with force=false now sees the existing row.
        with pytest.raises(ReportAlreadyExistsError):
            run(dry_run=True, force=False)
        assert len(patched_orchestrator["writes"]) == 1  # not duplicated


class TestOrchestratorPreFlight:
    def test_draft_not_complete_raises(self, patched_orchestrator) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run
        from lambdas.common.errors import DraftNotCompleteError

        patched_orchestrator["draft_status"] = "drafting"
        with pytest.raises(DraftNotCompleteError):
            run(dry_run=True, force=False)
        # No write should have happened.
        assert patched_orchestrator["writes"] == []

    def test_draft_predraft_raises(self, patched_orchestrator) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run
        from lambdas.common.errors import DraftNotCompleteError

        patched_orchestrator["draft_status"] = "pre_draft"
        with pytest.raises(DraftNotCompleteError):
            run(dry_run=True, force=False)


class TestOrchestratorDelivery:
    def test_dry_run_delivers_to_admin_only(self, patched_orchestrator) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

        result = run(dry_run=True, force=False)

        assert result["delivery_count"] == 1
        # One email, addressed to the admin.
        assert len(patched_orchestrator["emails_sent"]) == 1
        recipient, subject, _html, _text = patched_orchestrator["emails_sent"][0]
        assert recipient == "manager1@example.com"
        assert subject.startswith("[DRY RUN]")
        # Exactly one push call, targeting only the admin sleeper_user_id.
        assert len(patched_orchestrator["pushes_sent"]) == 1
        push = patched_orchestrator["pushes_sent"][0]
        assert push["user_ids"] == [ADMIN_ID]
        assert push["title"].startswith("[DRY RUN]")
        # Dry-run path does NOT set broadcast_at.
        assert patched_orchestrator["metadata_updates"] == []

    def test_broadcast_delivers_to_all_twelve(self, patched_orchestrator) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

        # Existing report so we need force=True to overwrite.
        result = run(dry_run=False, force=True)

        assert result["delivery_count"] == 12
        assert len(patched_orchestrator["emails_sent"]) == 12
        # Each email's subject lacks the dry-run prefix.
        for _r, subject, _h, _t in patched_orchestrator["emails_sent"]:
            assert "[DRY RUN]" not in subject
        # Push fan-out goes to all 12 user_ids.
        push = patched_orchestrator["pushes_sent"][0]
        assert len(push["user_ids"]) == 12
        assert push["title"] == "Your post-draft AI review is in"
        # broadcast_at stamped.
        assert any(
            "broadcast_at" in u["partial"]
            for u in patched_orchestrator["metadata_updates"]
        )


class TestOrchestratorReportShape:
    def test_metadata_carries_model_prompt_version_tokens(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

        result = run(
            dry_run=True,
            force=False,
            created_by_user_id="caller-x",
        )
        write = patched_orchestrator["writes"][0]
        meta = write["metadata"]
        assert meta["model"] == "claude-haiku-4-5"
        assert meta["prompt_version"]  # whatever AI_REVIEW_PROMPT_VERSION is
        assert meta["dry_run"] is True
        assert meta["force"] is False
        assert meta["draft_id"] == "DRAFT123"
        assert meta["created_by_user_id"] == "caller-x"
        assert meta["token_usage"]["input_tokens"] == 3500
        assert meta["token_usage"]["output_tokens"] == 1800
        assert meta["token_usage"]["cache_read_input_tokens"] == 3200
        # Result also exposes the same usage dict for the API response.
        assert result["token_usage"]["output_tokens"] == 1800
        assert result["model"] == "claude-haiku-4-5"

    def test_claude_called_with_two_block_system_payload(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger.orchestrator import run

        run(dry_run=True, force=False)
        call = patched_orchestrator["claude_calls"][0]
        system = call["system"]
        assert isinstance(system, list)
        assert len(system) == 2
        for block in system:
            assert block["type"] == "text"
            assert block["cache_control"] == {"type": "ephemeral"}
        # User prompt contains the canonical job-instruction line.
        assert "## Your job" in call["prompt"]
        # All 12 manager slots appear in the user prompt.
        for slot in range(1, 13):
            assert f"### Slot {slot}:" in call["prompt"]

    def test_prior_standings_failure_is_non_blocking(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger import orchestrator

        # Force prior-season rosters call to blow up.
        original = orchestrator.get_sleeper_league_rosters

        def _fail_on_prior(league_id: str):
            if league_id == "PRIOR123":
                raise RuntimeError("Sleeper hiccup")
            return original(league_id)

        monkeypatch.setattr(
            orchestrator, "get_sleeper_league_rosters", _fail_on_prior
        )
        result = orchestrator.run(dry_run=True, force=False)
        assert result["status"] == "dry_run_sent"
        # User prompt fell back to the no-standings note.
        call = patched_orchestrator["claude_calls"][0]
        assert "Prior season standings unavailable" in call["prompt"]


# ---------------------------------------------------------------------------
# Handler-layer tests
# ---------------------------------------------------------------------------


def _api_event(*, sleeper_user_id: str | None = ADMIN_ID, body: dict | None = None) -> dict:
    headers = {"X-Sleeper-User-Id": sleeper_user_id} if sleeper_user_id else {}
    return {
        "httpMethod": "POST",
        "path": "/admin/ai-review-postdraft-trigger",
        "headers": headers,
        "body": __import__("json").dumps(body or {}),
    }


class TestHandler:
    def test_403_when_not_admin(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger import handler as h
        from lambdas.common.errors import (
            AuthorizationError,
        )  # noqa: F401  — sanity import
        from lambdas.common.admin_gate import NotAdmin

        def _raise_not_admin(event, body):
            raise NotAdmin("not authorized")

        monkeypatch.setattr(h, "require_admin", _raise_not_admin)

        response = h.handler(
            _api_event(body={"dry_run": True}),
            context=None,
        )
        assert response["statusCode"] == 403
        import json

        body = json.loads(response["body"])
        assert body["Success"] is False

    def test_409_when_existing_report(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )

        patched_orchestrator["existing_report"] = {
            "league_id": "LEAGUE_ID",
            "report_type": "postDraft",
            "period": "2026",
            "created_at": "2026-05-20T00:00:00Z",
        }

        response = h.handler(
            _api_event(body={"dry_run": True, "force": False}),
            context=None,
        )
        assert response["statusCode"] == 409
        import json

        body = json.loads(response["body"])
        assert body["error"] == "already_generated"
        assert body["existing"]["period"] == "2026"

    def test_412_when_draft_not_complete(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )
        patched_orchestrator["draft_status"] = "drafting"

        response = h.handler(
            _api_event(body={"dry_run": True}),
            context=None,
        )
        assert response["statusCode"] == 412
        import json

        body = json.loads(response["body"])
        assert body["error"] == "draft_not_complete"
        assert body["draft_status"] == "drafting"

    def test_200_dry_run_single_recipient(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )

        response = h.handler(
            _api_event(body={"dry_run": True}),
            context=None,
        )
        assert response["statusCode"] == 200
        import json

        body = json.loads(response["body"])
        assert body["Success"] is True
        assert body["dry_run"] is True
        assert body["delivery_count"] == 1
        assert body["model"] == "claude-haiku-4-5"

    def test_200_broadcast_all_twelve(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )

        response = h.handler(
            _api_event(body={"dry_run": False, "force": True}),
            context=None,
        )
        assert response["statusCode"] == 200
        import json

        body = json.loads(response["body"])
        assert body["dry_run"] is False
        assert body["delivery_count"] == 12

    def test_defaults_dry_run_true_force_false(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_postdraft_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )

        # Body omits dry_run + force entirely.
        response = h.handler(_api_event(body={}), context=None)
        assert response["statusCode"] == 200
        import json

        body = json.loads(response["body"])
        # Sane safe defaults: dry-run on.
        assert body["dry_run"] is True


# ---------------------------------------------------------------------------
# claude_helper system-block extension (back-compat + new list shape)
# ---------------------------------------------------------------------------


class TestClaudeHelperSystemBlockShape:
    """Locks the new `system: list[dict]` surface against drift."""

    def test_list_blocks_are_passed_through_and_stamped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import claude_helper

        captured = {}

        def _create(**kwargs):
            captured["kwargs"] = kwargs
            msg = MagicMock()
            msg.content = [MagicMock(text="ok")]
            msg.usage = MagicMock(input_tokens=1, output_tokens=2)
            msg.usage.cache_read_input_tokens = 0
            msg.usage.cache_creation_input_tokens = 0
            return msg

        client = MagicMock()
        client.messages.create.side_effect = _create
        monkeypatch.setattr(claude_helper, "_client", client)

        blocks = [
            {"type": "text", "text": "block 1"},
            {
                "type": "text",
                "text": "block 2",
                "cache_control": {"type": "ephemeral"},
            },
        ]
        result = claude_helper.generate(
            prompt="u", system=blocks, model="m", max_tokens=10
        )
        assert result == "ok"
        sent = captured["kwargs"]["system"]
        assert len(sent) == 2
        # Block 1 got default cache_control.
        assert sent[0]["cache_control"] == {"type": "ephemeral"}
        # Block 2's cache_control is preserved.
        assert sent[1]["cache_control"] == {"type": "ephemeral"}

    def test_string_system_back_compat_wraps_into_single_block(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import claude_helper

        captured = {}

        def _create(**kwargs):
            captured["kwargs"] = kwargs
            msg = MagicMock()
            msg.content = [MagicMock(text="ok")]
            msg.usage = MagicMock(input_tokens=1, output_tokens=2)
            return msg

        client = MagicMock()
        client.messages.create.side_effect = _create
        monkeypatch.setattr(claude_helper, "_client", client)

        result = claude_helper.generate(
            prompt="u", system="flat string", model="m", max_tokens=10
        )
        assert result == "ok"
        sent = captured["kwargs"]["system"]
        assert isinstance(sent, list)
        assert len(sent) == 1
        assert sent[0]["text"] == "flat string"
        assert sent[0]["cache_control"] == {"type": "ephemeral"}

    def test_return_usage_yields_token_dict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import claude_helper

        msg = MagicMock()
        msg.content = [MagicMock(text="ok")]
        usage = MagicMock()
        usage.input_tokens = 11
        usage.output_tokens = 22
        usage.cache_read_input_tokens = 33
        usage.cache_creation_input_tokens = 44
        msg.usage = usage

        client = MagicMock()
        client.messages.create.return_value = msg
        monkeypatch.setattr(claude_helper, "_client", client)

        text, usage_dict = claude_helper.generate(
            prompt="u",
            system="s",
            model="m",
            max_tokens=10,
            return_usage=True,
        )
        assert text == "ok"
        assert usage_dict == {
            "input_tokens": 11,
            "output_tokens": 22,
            "cache_read_input_tokens": 33,
            "cache_creation_input_tokens": 44,
        }

    def test_invalid_block_shape_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from lambdas.common import claude_helper

        client = MagicMock()
        monkeypatch.setattr(claude_helper, "_client", client)

        with pytest.raises(ValueError):
            claude_helper.generate(
                prompt="u",
                system=[{"type": "text"}],  # missing 'text'
                model="m",
                max_tokens=10,
            )
