"""
Tests for `api_admin_ai_review_preseason_trigger`.

Covers the orchestrator's flow + the API handler's surface:
  - 403 when caller is not an admin
  - 409 when a report already exists (force=false)
  - 412 when the regular season has already started
  - 200 + delivery_count=1 for dry_run=true
  - 200 + delivery_count=12 for dry_run=false, force=true
  - Idempotency: a second force=false call after a write still 409s
  - Prior-standings failure is non-blocking

External integrations (Sleeper, Anthropic, SES, SNS, Supabase,
DynamoDB ai_reports_store) are all monkeypatched. Tests stay
fast and deterministic.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest


ADMIN_ID = "594625531702460416"


# ---------------------------------------------------------------------------
# Fixture builders
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


def _make_rosters(
    count: int = 12,
    *,
    with_history: bool = True,
) -> list[dict[str, Any]]:
    rosters = []
    for i in range(1, count + 1):
        settings = {
            "wins": 13 - i if with_history else 0,
            "losses": i - 1 if with_history else 0,
            "ties": 0,
            "fpts": int(1800 - i * 25) if with_history else 0,
            "fpts_decimal": 50 if with_history else 0,
            "fpts_against": int(1500 + i * 10) if with_history else 0,
            "fpts_against_decimal": 25 if with_history else 0,
        }
        players = [f"pid-{i}-{j}" for j in range(1, 16)]
        starters = players[:9]
        rosters.append(
            {
                "roster_id": i,
                "owner_id": f"u{i}",
                "settings": settings,
                "players": players,
                "starters": starters,
            }
        )
    return rosters


def _make_whitelisted_users(count: int = 12) -> list[dict[str, Any]]:
    # User #1 in this list aligns with ADMIN_ID below.
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


@pytest.fixture
def patched_orchestrator(monkeypatch: pytest.MonkeyPatch):
    """Patch all external dependencies of `orchestrator.run` with
    deterministic stand-ins. Returns a state dict so tests can swap
    individual seams."""
    from lambdas.api_admin_ai_review_preseason_trigger import orchestrator

    state: dict[str, Any] = {
        "writes": [],
        "metadata_updates": [],
        "emails_sent": [],
        "pushes_sent": [],
        "claude_calls": [],
        "existing_report": None,
        "nfl_state": {"season": "2026", "season_type": "pre", "week": 0},
        "prior_league_id": "PRIOR123",
        "whitelisted_users": _make_whitelisted_users(),
        "active_league": {
            "sleeper_league_id": "LEAGUE_ID",
            "league_name": "CLT DYNASTY",
        },
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
            "previous_league_id": state["prior_league_id"],
            "season": "2026",
        }

    monkeypatch.setattr(orchestrator, "get_sleeper_league", _get_league)
    monkeypatch.setattr(orchestrator, "get_nfl_state", lambda: state["nfl_state"])

    def _users(league_id: str) -> list[dict[str, Any]]:
        return _make_sleeper_users()

    def _rosters(league_id: str) -> list[dict[str, Any]]:
        return _make_rosters(with_history=(league_id == "PRIOR123"))

    monkeypatch.setattr(orchestrator, "get_sleeper_league_users", _users)
    monkeypatch.setattr(orchestrator, "get_sleeper_league_rosters", _rosters)

    def _prior(league_id: str) -> str | None:
        return state["prior_league_id"]

    monkeypatch.setattr(orchestrator, "get_previous_league_id", _prior)

    monkeypatch.setattr(
        orchestrator,
        "get_active_whitelisted_users",
        lambda: state["whitelisted_users"],
    )

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
            "created_at": "2026-08-30T15:00:00Z",
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
            "input_tokens": 4200,
            "output_tokens": 2400,
            "cache_read_input_tokens": 3800,
            "cache_creation_input_tokens": 400,
        }
        text = (
            "# Preseason Review\n\n## Manager1\n\nLast year was a disaster.\n"
        )
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
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run
        from lambdas.common.errors import ReportAlreadyExistsError

        patched_orchestrator["existing_report"] = {
            "league_id": "LEAGUE_ID",
            "report_type": "preseason",
            "period": "2026-PRESEASON",
            "created_at": "2026-08-30T00:00:00Z",
        }

        with pytest.raises(ReportAlreadyExistsError):
            run(dry_run=True, force=False)

    def test_existing_report_with_force_true_overwrites(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        patched_orchestrator["existing_report"] = {
            "league_id": "LEAGUE_ID",
            "report_type": "preseason",
            "period": "2026-PRESEASON",
            "created_at": "2026-08-30T00:00:00Z",
        }

        result = run(dry_run=True, force=True)
        assert result["status"] == "dry_run_sent"
        assert len(patched_orchestrator["writes"]) == 1

    def test_consecutive_writes_with_force_false_still_409(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run
        from lambdas.common.errors import ReportAlreadyExistsError

        # First write goes through.
        first = run(dry_run=True, force=False)
        assert first["status"] == "dry_run_sent"
        assert len(patched_orchestrator["writes"]) == 1
        # Second invocation with force=false now sees the existing row.
        with pytest.raises(ReportAlreadyExistsError):
            run(dry_run=True, force=False)
        assert len(patched_orchestrator["writes"]) == 1


class TestOrchestratorPreFlight:
    def test_regular_season_underway_raises(self, patched_orchestrator) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run
        from lambdas.common.errors import PreseasonWindowPassedError

        patched_orchestrator["nfl_state"] = {
            "season": "2026",
            "season_type": "regular",
            "week": 3,
        }
        with pytest.raises(PreseasonWindowPassedError) as excinfo:
            run(dry_run=True, force=False)
        # The error carries the NFL season + season_type for the
        # 412 response body.
        assert excinfo.value.details["season_type"] == "regular"
        assert excinfo.value.details["nfl_season"] == "2026"
        # No write should have happened.
        assert patched_orchestrator["writes"] == []

    def test_postseason_raises(self, patched_orchestrator) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run
        from lambdas.common.errors import PreseasonWindowPassedError

        patched_orchestrator["nfl_state"] = {
            "season": "2026",
            "season_type": "post",
            "week": 19,
        }
        with pytest.raises(PreseasonWindowPassedError):
            run(dry_run=True, force=False)

    def test_off_season_is_allowed(self, patched_orchestrator) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        patched_orchestrator["nfl_state"] = {
            "season": "2026",
            "season_type": "off",
            "week": 0,
        }
        result = run(dry_run=True, force=False)
        assert result["status"] == "dry_run_sent"


class TestOrchestratorDelivery:
    def test_dry_run_delivers_to_admin_only(self, patched_orchestrator) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

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
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        result = run(dry_run=False, force=True)

        assert result["delivery_count"] == 12
        assert len(patched_orchestrator["emails_sent"]) == 12
        for _r, subject, _h, _t in patched_orchestrator["emails_sent"]:
            assert "[DRY RUN]" not in subject
        push = patched_orchestrator["pushes_sent"][0]
        assert len(push["user_ids"]) == 12
        assert push["title"] == "Your preseason AI review is in"
        # broadcast_at stamped.
        assert any(
            "broadcast_at" in u["partial"]
            for u in patched_orchestrator["metadata_updates"]
        )


class TestOrchestratorReportShape:
    def test_metadata_carries_model_prompt_version_tokens(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        result = run(
            dry_run=True,
            force=False,
            created_by_user_id="caller-x",
        )
        write = patched_orchestrator["writes"][0]
        meta = write["metadata"]
        assert meta["model"] == "claude-haiku-4-5"
        assert meta["prompt_version"].startswith("f2-preseason-")
        assert meta["dry_run"] is True
        assert meta["force"] is False
        assert meta["nfl_season"] == "2026"
        assert meta["nfl_season_type"] == "pre"
        assert meta["prior_league_id"] == "PRIOR123"
        assert meta["created_by_user_id"] == "caller-x"
        assert meta["token_usage"]["input_tokens"] == 4200
        assert meta["token_usage"]["output_tokens"] == 2400
        assert meta["token_usage"]["cache_read_input_tokens"] == 3800
        # Result also exposes the same usage dict for the API response.
        assert result["token_usage"]["output_tokens"] == 2400
        assert result["model"] == "claude-haiku-4-5"

    def test_write_uses_preseason_period(self, patched_orchestrator) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        run(dry_run=True, force=False)
        write = patched_orchestrator["writes"][0]
        assert write["report_type"] == "preseason"
        assert write["period"] == "2026-PRESEASON"

    def test_claude_called_with_two_block_system_payload(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        run(dry_run=True, force=False)
        call = patched_orchestrator["claude_calls"][0]
        system = call["system"]
        assert isinstance(system, list)
        assert len(system) == 2
        for block in system:
            assert block["type"] == "text"
            assert block["cache_control"] == {"type": "ephemeral"}
        # User prompt contains the task footer.
        assert "## Your task" in call["prompt"]
        # All 12 manager roster headers appear in roster_id order.
        for i in range(1, 13):
            assert f"Roster #{i}" in call["prompt"]
        assert call["prompt"].index("Roster #1") < call["prompt"].index(
            "Roster #12"
        )

    def test_prior_standings_failure_is_non_blocking(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger import orchestrator

        # Force prior-season rosters call to blow up, current league
        # call still succeeds.
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
        "path": "/admin/ai-review-preseason-trigger",
        "headers": headers,
        "body": __import__("json").dumps(body or {}),
    }


class TestHandler:
    def test_403_when_not_admin(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger import handler as h
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
        from lambdas.api_admin_ai_review_preseason_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )

        patched_orchestrator["existing_report"] = {
            "league_id": "LEAGUE_ID",
            "report_type": "preseason",
            "period": "2026-PRESEASON",
            "created_at": "2026-08-30T00:00:00Z",
        }

        response = h.handler(
            _api_event(body={"dry_run": True, "force": False}),
            context=None,
        )
        assert response["statusCode"] == 409
        import json

        body = json.loads(response["body"])
        assert body["error"] == "already_generated"
        assert body["existing"]["period"] == "2026-PRESEASON"

    def test_412_when_regular_season_started(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )
        patched_orchestrator["nfl_state"] = {
            "season": "2026",
            "season_type": "regular",
            "week": 4,
        }

        response = h.handler(
            _api_event(body={"dry_run": True}),
            context=None,
        )
        assert response["statusCode"] == 412
        import json

        body = json.loads(response["body"])
        assert body["error"] == "preseason_window_passed"
        assert body["season_type"] == "regular"
        assert body["nfl_season"] == "2026"

    def test_200_dry_run_single_recipient(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger import handler as h

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
        from lambdas.api_admin_ai_review_preseason_trigger import handler as h

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
        from lambdas.api_admin_ai_review_preseason_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )

        response = h.handler(_api_event(body={}), context=None)
        assert response["statusCode"] == 200
        import json

        body = json.loads(response["body"])
        # Safe defaults: dry-run on.
        assert body["dry_run"] is True


# ---------------------------------------------------------------------------
# Admin Portal F2 — email previews on dry-run responses
# ---------------------------------------------------------------------------


class TestOrchestratorPreviewsDryRun:
    """Locks the dry-run preview surface added in Admin Portal F2."""

    def test_dry_run_returns_one_preview_per_active_user(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        result = run(dry_run=True, force=False)

        previews = result["previews"]
        assert isinstance(previews, list)
        assert len(previews) == 12

    def test_each_preview_has_required_fields(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        result = run(dry_run=True, force=False)

        required = {
            "recipient_user_id",
            "recipient_email",
            "display_name",
            "subject",
            "text_body",
            "html_body_excerpt",
        }
        for preview in result["previews"]:
            assert required.issubset(preview.keys())
            assert "html_body" not in preview

    def test_previews_sorted_alphabetically_by_display_name(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        patched_orchestrator["whitelisted_users"] = [
            {
                "sleeper_user_id": ADMIN_ID,
                "email": "zach@example.com",
                "display_name": "Zach",
                "sleeper_username": "zach",
                "is_active": True,
                "is_admin": True,
                "id": "row-1",
            },
            {
                "sleeper_user_id": "u2",
                "email": "adam@example.com",
                "display_name": "Adam",
                "sleeper_username": "adam",
                "is_active": True,
                "is_admin": False,
                "id": "row-2",
            },
            {
                "sleeper_user_id": "u3",
                "email": "mike@example.com",
                "display_name": "Mike",
                "sleeper_username": "mike",
                "is_active": True,
                "is_admin": False,
                "id": "row-3",
            },
            {
                "sleeper_user_id": "u4",
                "email": "beth@example.com",
                "display_name": "Beth",
                "sleeper_username": "beth",
                "is_active": True,
                "is_admin": False,
                "id": "row-4",
            },
        ]

        result = run(dry_run=True, force=False)
        names = [p["display_name"] for p in result["previews"]]
        assert names == ["Adam", "Beth", "Mike", "Zach"]

    def test_text_body_capped_at_4096_chars(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger import orchestrator

        long_markdown = "X" * 10_000

        def _long_generate(
            *, prompt, system, model, max_tokens, return_usage=False
        ):
            usage = {
                "input_tokens": 1,
                "output_tokens": 1,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
            }
            if return_usage:
                return long_markdown, usage
            return long_markdown

        fake_claude = MagicMock()
        fake_claude.generate = _long_generate
        monkeypatch.setattr(orchestrator, "claude_helper", fake_claude)

        result = orchestrator.run(dry_run=True, force=False)
        for preview in result["previews"]:
            assert len(preview["text_body"]) <= 4096

    def test_html_body_excerpt_capped_at_500_chars(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        result = run(dry_run=True, force=False)
        for preview in result["previews"]:
            assert len(preview["html_body_excerpt"]) <= 500


class TestOrchestratorPreviewsBroadcast:
    """Broadcast (dry_run=False) responses must NOT include previews."""

    def test_broadcast_returns_none_previews(
        self, patched_orchestrator
    ) -> None:
        from lambdas.api_admin_ai_review_preseason_trigger.orchestrator import run

        result = run(dry_run=False, force=True)

        assert result["previews"] is None


class TestHandlerPreviews:
    """End-to-end through the lambda handler."""

    def test_handler_dry_run_response_includes_previews(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import json
        from lambdas.api_admin_ai_review_preseason_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )
        response = h.handler(
            _api_event(body={"dry_run": True}),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert isinstance(body["previews"], list)
        assert len(body["previews"]) == 12

    def test_handler_broadcast_response_previews_null(
        self,
        patched_orchestrator,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import json
        from lambdas.api_admin_ai_review_preseason_trigger import handler as h

        monkeypatch.setattr(
            h, "require_admin", lambda event, body: {"sleeper_user_id": ADMIN_ID}
        )
        response = h.handler(
            _api_event(body={"dry_run": False, "force": True}),
            context=None,
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["previews"] is None
