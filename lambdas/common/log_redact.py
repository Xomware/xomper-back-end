"""
PII redaction for the admin CloudWatch log tail (F5)
====================================================
Best-effort regex sweep applied to every log event message before it
leaves the backend. The redactions are:

- Email addresses → ``***@***``
- Sleeper user IDs (long numeric, 15-20 digits) → ``[uid]``
- Anthropic API keys (``sk-ant-...``) → ``[key]``

These three patterns cover every known leak shape in xomper logs. A
generic "JSON field ending in `_key`/`_token`" sweep was rejected in
the F5 BRAINSTORM — too noisy in trace dumps.

The helper is intentionally tiny + side-effect-free so it stays
straightforward to unit-test (`tests/test_log_redact.py` locks the
contract).
"""
from __future__ import annotations

import re

# Email — local part + `@` + domain. Permissive on the local part so we
# catch `foo.bar+tag@example.com`. The TLD is required (`\w+` after the
# final dot) to avoid matching short numeric runs like `1@2`.
EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.\w+")

# Sleeper user IDs are 18-19 digits today. We accept 15-20 to be safe
# against historical short / future long IDs. Word boundaries on both
# sides so we don't chew through a 30-digit blob.
SLEEPER_ID_RE = re.compile(r"\b\d{15,20}\b")

# Anthropic API keys are prefixed `sk-ant-` and contain word chars +
# hyphens. The 20+ tail length keeps us from matching short
# placeholders like `sk-ant-test`.
ANTHROPIC_KEY_RE = re.compile(r"sk-ant-[\w-]{20,}")


def redact(text: str) -> str:
    """Apply all PII redactions to ``text``. Best-effort; doesn't raise.

    Non-string input (None, ints, dicts that snuck through) returns the
    empty string so the handler can always emit a stringy message
    field without TypeError noise.
    """
    if not isinstance(text, str):
        return ""
    text = EMAIL_RE.sub("***@***", text)
    text = SLEEPER_ID_RE.sub("[uid]", text)
    text = ANTHROPIC_KEY_RE.sub("[key]", text)
    return text
