"""
Tests for lambdas.common.claude_helper.

The real Anthropic SDK is not installed in CI by default — we mock
both `anthropic` and `ssm_helpers.get_parameter` so the helper's
retry / caching / cache_control behavior is testable in isolation.
"""
from __future__ import annotations

import sys
import types
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Test-side anthropic SDK shim
# ---------------------------------------------------------------------------
# claude_helper imports `anthropic` lazily inside `_get_client`. The
# real package isn't installed in this test environment, so we
# register a minimal shim in `sys.modules` before importing
# claude_helper. Individual tests further patch the shim's behavior.
# ---------------------------------------------------------------------------


class _FakeAPIConnectionError(Exception):
    pass


class _FakeAPIStatusError(Exception):
    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code


def _install_anthropic_shim() -> types.ModuleType:
    """(Re-)install the anthropic shim before each test so the
    constructor is freshly mockable."""
    shim = types.ModuleType("anthropic")
    shim.Anthropic = MagicMock(name="anthropic.Anthropic")  # type: ignore[attr-defined]
    shim.APIConnectionError = _FakeAPIConnectionError  # type: ignore[attr-defined]
    shim.APIStatusError = _FakeAPIStatusError  # type: ignore[attr-defined]
    sys.modules["anthropic"] = shim
    return shim


@pytest.fixture(autouse=True)
def _reset_helper_state(monkeypatch: pytest.MonkeyPatch):
    """Each test starts with a fresh anthropic shim, a fresh
    `_client` cache, and zero SSM call counts."""
    shim = _install_anthropic_shim()

    # Import lazily inside the fixture so we pick up the shim.
    from lambdas.common import claude_helper, ssm_helpers

    monkeypatch.setattr(claude_helper, "_client", None)
    monkeypatch.setattr(claude_helper, "_BASE_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(ssm_helpers, "_cache", {})
    monkeypatch.setattr(ssm_helpers, "_ssm_client", None)

    # SSM `get_parameter` returns a deterministic key.
    ssm_call_counter = {"count": 0}

    def _fake_get_parameter(name: str, *, decrypt: bool = True) -> str:
        ssm_call_counter["count"] += 1
        return "sk-test-anthropic-key"

    monkeypatch.setattr(claude_helper, "get_parameter", _fake_get_parameter)

    yield shim, ssm_call_counter


def _make_message_mock(text: str, input_tokens: int = 10, output_tokens: int = 20) -> Any:
    """Build an SDK-like response object with a `.content` list and
    a `.usage` block."""
    block = MagicMock()
    block.text = text
    usage = MagicMock()
    usage.input_tokens = input_tokens
    usage.output_tokens = output_tokens
    message = MagicMock()
    message.content = [block]
    message.usage = usage
    return message


class TestGenerateHappyPath:
    def test_returns_concatenated_text(self, _reset_helper_state) -> None:
        shim, _ = _reset_helper_state
        client = MagicMock()
        client.messages.create.return_value = _make_message_mock("hello world")
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate

        result = generate("user prompt", system="lore + tone")
        assert result == "hello world"

    def test_passes_model_and_max_tokens(self, _reset_helper_state) -> None:
        shim, _ = _reset_helper_state
        client = MagicMock()
        client.messages.create.return_value = _make_message_mock("ok")
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate

        generate("u", system="s", model="claude-sonnet-4-5", max_tokens=1234)
        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["model"] == "claude-sonnet-4-5"
        assert call_kwargs["max_tokens"] == 1234

    def test_user_message_is_uncached(self, _reset_helper_state) -> None:
        shim, _ = _reset_helper_state
        client = MagicMock()
        client.messages.create.return_value = _make_message_mock("ok")
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate

        generate("the user prompt", system="lore")
        kwargs = client.messages.create.call_args.kwargs
        assert kwargs["messages"] == [
            {"role": "user", "content": "the user prompt"}
        ]


class TestCacheControlInjection:
    def test_system_blocks_carry_ephemeral_cache_control(self, _reset_helper_state) -> None:
        shim, _ = _reset_helper_state
        client = MagicMock()
        client.messages.create.return_value = _make_message_mock("ok")
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate

        generate("u", system="lore + tone scaffolding")
        kwargs = client.messages.create.call_args.kwargs
        system_blocks = kwargs.get("system")
        assert isinstance(system_blocks, list)
        assert len(system_blocks) == 1
        block = system_blocks[0]
        assert block["type"] == "text"
        assert block["text"] == "lore + tone scaffolding"
        assert block["cache_control"] == {"type": "ephemeral"}

    def test_no_system_means_no_system_kwarg(self, _reset_helper_state) -> None:
        shim, _ = _reset_helper_state
        client = MagicMock()
        client.messages.create.return_value = _make_message_mock("ok")
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate

        generate("u")
        kwargs = client.messages.create.call_args.kwargs
        assert "system" not in kwargs


class TestRetryBehavior:
    def test_retries_then_succeeds_on_connection_error(self, _reset_helper_state) -> None:
        shim, _ = _reset_helper_state
        client = MagicMock()
        client.messages.create.side_effect = [
            shim.APIConnectionError("network blip"),
            shim.APIConnectionError("network blip"),
            _make_message_mock("recovered"),
        ]
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate

        result = generate("u", system="s")
        assert result == "recovered"
        assert client.messages.create.call_count == 3

    def test_retries_on_5xx_status(self, _reset_helper_state) -> None:
        shim, _ = _reset_helper_state
        client = MagicMock()
        client.messages.create.side_effect = [
            shim.APIStatusError("server down", status_code=503),
            _make_message_mock("recovered after 5xx"),
        ]
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate

        result = generate("u", system="s")
        assert result == "recovered after 5xx"
        assert client.messages.create.call_count == 2

    def test_does_not_retry_on_4xx(self, _reset_helper_state) -> None:
        shim, _ = _reset_helper_state
        client = MagicMock()
        client.messages.create.side_effect = shim.APIStatusError(
            "bad request", status_code=400,
        )
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate
        from lambdas.common.errors import ClaudeAPIError

        with pytest.raises(ClaudeAPIError):
            generate("u", system="s")
        # 4xx is terminal — only one attempt.
        assert client.messages.create.call_count == 1

    def test_raises_after_max_attempts(self, _reset_helper_state) -> None:
        shim, _ = _reset_helper_state
        client = MagicMock()
        client.messages.create.side_effect = shim.APIConnectionError("flaky")
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate
        from lambdas.common.errors import ClaudeAPIError

        with pytest.raises(ClaudeAPIError):
            generate("u", system="s")
        assert client.messages.create.call_count == 3


class TestSSMAndClientCaching:
    def test_ssm_called_once_across_two_generate_calls(self, _reset_helper_state) -> None:
        shim, ssm_counter = _reset_helper_state
        client = MagicMock()
        client.messages.create.return_value = _make_message_mock("ok")
        shim.Anthropic.return_value = client

        from lambdas.common.claude_helper import generate

        generate("a")
        generate("b")
        assert ssm_counter["count"] == 1
        # Anthropic constructor likewise only fires once.
        assert shim.Anthropic.call_count == 1
