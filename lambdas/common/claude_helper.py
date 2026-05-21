"""
Claude / Anthropic Helper
=========================
Shared wrapper around the Anthropic Python SDK for the AI Review
feature (F1 post-draft / F2 preseason / F3 weekly). Other lambdas
should call `generate(...)` and never touch the SDK directly — this
module owns the API key lookup, prompt caching, retries, and
structured error handling.

Behavior contract
-----------------
- API key is fetched lazily from SSM
  (`/xomper/api/ANTHROPIC_API_KEY`) on the first call and cached at
  module scope. A console-side key update is picked up on the next
  cold start without requiring a redeploy.
- The SDK client itself is also cached at module scope.
- When a system prompt is provided, each system block is sent with
  `cache_control = {"type": "ephemeral"}` so Anthropic caches the
  lore + tone scaffolding across calls. User content stays uncached.
- Retries on `anthropic.APIConnectionError` and 5xx-style
  `anthropic.APIStatusError` with exponential backoff (max 3
  attempts, base 1s). On terminal failure raises `ClaudeAPIError`.
- Logs input + output token counts via the project logger so cost
  is visible in CloudWatch.
"""
from __future__ import annotations

import time
from typing import Any

from lambdas.common.constants import PRODUCT
from lambdas.common.errors import ClaudeAPIError
from lambdas.common.logger import get_logger
from lambdas.common.ssm_helpers import get_parameter

log = get_logger(__file__)

_SSM_API_KEY_PATH = f"/{PRODUCT}/api/ANTHROPIC_API_KEY"
_DEFAULT_MODEL = "claude-haiku-4-5"
_DEFAULT_MAX_TOKENS = 4000
_MAX_ATTEMPTS = 3
_BASE_BACKOFF_SECONDS = 1.0

# Module-level caches (warm across invocations within a single
# lambda container).
_client: Any | None = None


def _get_client() -> Any:
    """Lazy-load + cache the Anthropic client. Imports the SDK at
    call time so test code can monkeypatch `anthropic` before
    construction, and so the import cost isn't paid at module import.
    """
    global _client
    if _client is not None:
        return _client

    try:
        import anthropic  # type: ignore
    except ImportError as err:
        raise ClaudeAPIError(
            message=(
                "anthropic SDK is not installed in this lambda "
                "environment. Add `anthropic` to requirements.txt and "
                "redeploy the shared layer."
            ),
        ) from err

    api_key = get_parameter(_SSM_API_KEY_PATH)
    _client = anthropic.Anthropic(api_key=api_key)
    return _client


def _system_blocks(system: str | list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize the `system` arg into a list of Anthropic content
    blocks tagged with `cache_control: ephemeral`.

    Two input shapes are supported:

    - `str`: wrap into a single ephemeral text block (back-compat
      with F0 callers that pass one flat system string).
    - `list[dict]`: a pre-built multi-block payload, e.g. F1's
      `[tone+safety, lore]` split. Each block is shallow-copied and
      stamped with `cache_control: ephemeral` if the caller didn't
      already provide one. Each block must carry `type` + `text`.
    """
    if isinstance(system, str):
        return [
            {
                "type": "text",
                "text": system,
                "cache_control": {"type": "ephemeral"},
            }
        ]

    normalized: list[dict[str, Any]] = []
    for block in system:
        if not isinstance(block, dict):
            raise TypeError(
                "claude_helper: system blocks must be dicts; "
                f"got {type(block).__name__}"
            )
        if "type" not in block or "text" not in block:
            raise ValueError(
                "claude_helper: system block requires 'type' and 'text' keys"
            )
        copy = dict(block)
        copy.setdefault("cache_control", {"type": "ephemeral"})
        normalized.append(copy)
    return normalized


def _is_retriable(err: Exception) -> bool:
    """Decide whether to retry a given Anthropic SDK error."""
    try:
        import anthropic  # type: ignore
    except ImportError:
        return False

    if isinstance(err, anthropic.APIConnectionError):
        return True

    # 5xx → retry. 4xx → don't.
    api_status_error = getattr(anthropic, "APIStatusError", None)
    if api_status_error is not None and isinstance(err, api_status_error):
        status = getattr(err, "status_code", None)
        if isinstance(status, int) and 500 <= status < 600:
            return True

    return False


def _extract_text(message: Any) -> str:
    """Pull the assistant's text out of a Messages API response.
    Handles both the SDK's typed objects and dict-shaped mocks used
    in tests."""
    content = getattr(message, "content", None)
    if content is None and isinstance(message, dict):
        content = message.get("content")

    if not content:
        return ""

    chunks: list[str] = []
    for block in content:
        text = getattr(block, "text", None)
        if text is None and isinstance(block, dict):
            text = block.get("text")
        if text:
            chunks.append(text)
    return "".join(chunks)


def _log_usage(message: Any, model: str) -> None:
    """Best-effort token usage log. Safe against missing `usage`
    on mocked responses."""
    usage = getattr(message, "usage", None)
    if usage is None and isinstance(message, dict):
        usage = message.get("usage")
    if usage is None:
        return
    in_tokens = getattr(usage, "input_tokens", None)
    out_tokens = getattr(usage, "output_tokens", None)
    if in_tokens is None and isinstance(usage, dict):
        in_tokens = usage.get("input_tokens")
        out_tokens = usage.get("output_tokens")
    log.info(
        f"claude_helper.generate: model={model} "
        f"input_tokens={in_tokens} output_tokens={out_tokens}"
    )


def generate(
    prompt: str,
    system: str | list[dict[str, Any]] | None = None,
    model: str = _DEFAULT_MODEL,
    max_tokens: int = _DEFAULT_MAX_TOKENS,
    return_usage: bool = False,
) -> str | tuple[str, dict[str, int | None]]:
    """Send a single user prompt to Claude and return the text reply.

    Args:
        prompt: The user message (per-report data + instructions).
        system: Optional system prompt. Accepts either a flat string
            (back-compat — wrapped into a single ephemeral block) or a
            pre-built `list[dict]` of Anthropic content blocks for
            multi-block caching (e.g. F1's `[tone+safety, lore]`
            split). All blocks are tagged with `cache_control:
            ephemeral` unless the caller set their own value.
        model: Anthropic model id. Defaults to Haiku 4.5.
        max_tokens: Cap on output tokens.
        return_usage: When True, returns `(text, usage_dict)` where
            `usage_dict` carries `input_tokens`, `output_tokens`,
            `cache_read_input_tokens`, `cache_creation_input_tokens`
            for downstream cost accounting + report metadata. When
            False (default), returns just the text for back-compat.

    Returns:
        The assistant's text response, concatenated across blocks
        (or the `(text, usage)` tuple when `return_usage=True`).

    Raises:
        ClaudeAPIError: On terminal failure after retries.
    """
    client = _get_client()

    request_kwargs: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system is not None:
        request_kwargs["system"] = _system_blocks(system)

    last_err: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            message = client.messages.create(**request_kwargs)
            _log_usage(message, model)
            text = _extract_text(message)
            if return_usage:
                return text, _usage_dict(message)
            return text
        except Exception as err:  # noqa: BLE001 — funnel to retry / wrap
            last_err = err
            if not _is_retriable(err) or attempt == _MAX_ATTEMPTS:
                break
            backoff = _BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log.warning(
                f"claude_helper.generate: attempt {attempt} failed "
                f"({err.__class__.__name__}: {err}); retrying in {backoff:.1f}s"
            )
            time.sleep(backoff)

    raise ClaudeAPIError(
        message=f"Anthropic call failed after {_MAX_ATTEMPTS} attempts: {last_err}",
        model=model,
    ) from last_err


def _usage_dict(message: Any) -> dict[str, int | None]:
    """Pull a structured usage dict from a Messages API response.
    Tolerates both SDK objects and dict-shaped mocks. Keys mirror the
    Anthropic SDK field names so downstream telemetry stays portable."""
    usage = getattr(message, "usage", None)
    if usage is None and isinstance(message, dict):
        usage = message.get("usage")

    def _read(field: str) -> int | None:
        if usage is None:
            return None
        value = getattr(usage, field, None)
        if value is None and isinstance(usage, dict):
            value = usage.get(field)
        return value

    return {
        "input_tokens": _read("input_tokens"),
        "output_tokens": _read("output_tokens"),
        "cache_read_input_tokens": _read("cache_read_input_tokens"),
        "cache_creation_input_tokens": _read("cache_creation_input_tokens"),
    }
