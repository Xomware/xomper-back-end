"""
Tests for ``lambdas.common.log_redact`` (admin-portal F5).

Locks the server-side PII redaction contract applied to every
CloudWatch log event before it leaves the backend. The three pinned
patterns (email, Sleeper user id, Anthropic API key) cover every
known leak shape; regressing them would be a privacy bug.
"""
from __future__ import annotations

import pytest

from lambdas.common.log_redact import (
    ANTHROPIC_KEY_RE,
    EMAIL_RE,
    SLEEPER_ID_RE,
    redact,
)


class TestRedactEmail:
    def test_simple_email(self) -> None:
        assert redact("user@example.com") == "***@***"

    def test_email_in_sentence(self) -> None:
        assert (
            redact("Sent welcome email to dominickj.giordano@gmail.com today.")
            == "Sent welcome email to ***@*** today."
        )

    def test_plus_and_dot_local_part(self) -> None:
        assert redact("foo.bar+tag@sub.example.co") == "***@***"

    def test_email_in_json_escaped_string(self) -> None:
        raw = '{"recipient_email": "alice@example.com"}'
        assert "alice@example.com" not in redact(raw)
        assert "***@***" in redact(raw)

    def test_multiple_emails_in_one_line(self) -> None:
        raw = "from a@b.io to c@d.io"
        out = redact(raw)
        assert "a@b.io" not in out
        assert "c@d.io" not in out
        assert out.count("***@***") == 2

    def test_regex_isolation(self) -> None:
        # Defense-in-depth: the EMAIL_RE alone matches as we expect.
        assert EMAIL_RE.fullmatch("user@example.com") is not None


class TestRedactSleeperId:
    def test_18_digit_id(self) -> None:
        assert redact("594625531702460416") == "[uid]"

    def test_id_in_sentence(self) -> None:
        assert (
            redact("admin_gate: caller 594625531702460416 not found")
            == "admin_gate: caller [uid] not found"
        )

    def test_15_digit_low_bound(self) -> None:
        assert redact("123456789012345") == "[uid]"

    def test_20_digit_high_bound(self) -> None:
        assert redact("12345678901234567890") == "[uid]"

    def test_14_digit_too_short_untouched(self) -> None:
        """Sleeper IDs are always >= 15 digits today. 14-digit numerics
        (e.g. unix epoch in microseconds) must NOT be matched."""
        out = redact("ts=12345678901234 done")
        assert "12345678901234" in out
        assert "[uid]" not in out

    def test_21_digit_too_long_untouched(self) -> None:
        """Anything longer than 20 should not be classified as a
        Sleeper id either (avoids gobbling hash-like blobs)."""
        out = redact("hash=123456789012345678901 done")
        assert "[uid]" not in out

    def test_word_boundary_protects_alphanumeric_runs(self) -> None:
        # `abc594625531702460416` should NOT redact — the ID isn't
        # standing alone, it's a suffix on something else.
        out = redact("abc594625531702460416")
        assert "[uid]" not in out

    def test_regex_isolation(self) -> None:
        assert SLEEPER_ID_RE.fullmatch("594625531702460416") is not None


class TestRedactAnthropicKey:
    def test_canonical_key(self) -> None:
        key = "sk-ant-api03-" + "A1B2C3D4E5F6G7H8I9J0"
        assert redact(key) == "[key]"

    def test_key_in_sentence(self) -> None:
        key = "sk-ant-api03-" + "X" * 40
        raw = f"using key={key} now"
        out = redact(raw)
        assert key not in out
        assert "[key]" in out

    def test_short_placeholder_untouched(self) -> None:
        # `sk-ant-test` is too short for the 20+ tail; leave alone.
        assert redact("placeholder sk-ant-test") == "placeholder sk-ant-test"

    def test_regex_isolation(self) -> None:
        assert ANTHROPIC_KEY_RE.fullmatch("sk-ant-" + "x" * 25) is not None


class TestRedactComposite:
    def test_all_three_in_one_line(self) -> None:
        raw = (
            "user dominickj.giordano@gmail.com (id 594625531702460416) "
            "called with key sk-ant-api03-" + "z" * 30
        )
        out = redact(raw)
        assert "dominickj.giordano@gmail.com" not in out
        assert "594625531702460416" not in out
        assert "sk-ant-api03-" not in out
        assert "***@***" in out
        assert "[uid]" in out
        assert "[key]" in out

    def test_clean_line_untouched(self) -> None:
        raw = "INFO Starting weekly recap for league xyz."
        assert redact(raw) == raw

    def test_empty_string(self) -> None:
        assert redact("") == ""


class TestRedactDefensive:
    """``redact`` is best-effort and must never raise — the lambda
    pipes every CloudWatch event through it and a crash there would
    break the entire tail."""

    @pytest.mark.parametrize("bad", [None, 123, 4.5, {"a": 1}, ["x"], object()])
    def test_non_string_inputs_return_empty(self, bad: object) -> None:
        assert redact(bad) == ""  # type: ignore[arg-type]
