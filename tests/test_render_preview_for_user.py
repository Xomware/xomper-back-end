"""
Tests for `lambdas.common.email_templates.ai_review.render_preview_for_user`.

Locks Admin Portal F2 cap discipline:
  * `text_body` <= 4096 chars
  * `html_body_excerpt` <= 500 chars
  * full `html_body` key is dropped (admin-only payload stays mobile-friendly)
  * happy-path shape matches the iOS `EmailPreview` decoder

No network / SES / Anthropic mocking required — `build_email_payload`
is pure template substitution.
"""
from __future__ import annotations

import pytest

from lambdas.common.email_templates.ai_review import (
    PREVIEW_HTML_EXCERPT_CAP,
    PREVIEW_TEXT_BODY_CAP,
    render_preview_for_user,
)


def _user(
    *,
    user_id: str = "u1",
    email: str = "manager1@example.com",
    display_name: str = "Manager1",
    sleeper_username: str = "manager1",
) -> dict:
    return {
        "sleeper_user_id": user_id,
        "email": email,
        "display_name": display_name,
        "sleeper_username": sleeper_username,
    }


class TestRenderPreviewForUserHappyPath:
    def test_returns_expected_keys(self) -> None:
        result = render_preview_for_user(
            user=_user(),
            report_type="postDraft",
            body_markdown="# Post-Draft Recap\n\nManager1 cooked.\n",
            period_label="Post-Draft 2026",
            league_name="CLT DYNASTY",
        )

        # Wire shape the iOS EmailPreview decoder expects.
        assert set(result.keys()) == {
            "recipient_user_id",
            "recipient_email",
            "display_name",
            "subject",
            "text_body",
            "html_body_excerpt",
        }
        # Full html_body is intentionally dropped.
        assert "html_body" not in result

    def test_fields_carry_user_identity(self) -> None:
        result = render_preview_for_user(
            user=_user(
                user_id="u42",
                email="zach@example.com",
                display_name="Zach",
            ),
            report_type="weekly",
            body_markdown="Body",
            period_label="Week 4",
        )
        assert result["recipient_user_id"] == "u42"
        assert result["recipient_email"] == "zach@example.com"
        assert result["display_name"] == "Zach"

    def test_subject_threads_display_name_and_period(self) -> None:
        result = render_preview_for_user(
            user=_user(display_name="Beth"),
            report_type="weekly",
            body_markdown="Body",
            period_label="Week 7",
        )
        # Subject is rendered by `build_email_payload` — confirm the
        # preview surface inherits the same line untouched.
        assert "Beth" in result["subject"]
        assert "Week 7" in result["subject"]
        assert "Weekly Review" in result["subject"]

    def test_text_body_under_cap_passes_through_unchanged(self) -> None:
        body = "# Tiny recap\n\nNothing notable happened.\n"
        result = render_preview_for_user(
            user=_user(),
            report_type="postDraft",
            body_markdown=body,
            period_label="Post-Draft 2026",
        )
        # Whole markdown body shows up in the plain-text section.
        assert "Tiny recap" in result["text_body"]
        assert len(result["text_body"]) <= PREVIEW_TEXT_BODY_CAP

    def test_html_excerpt_under_cap_passes_through(self) -> None:
        result = render_preview_for_user(
            user=_user(),
            report_type="postDraft",
            body_markdown="# Recap",
            period_label="Post-Draft 2026",
        )
        assert len(result["html_body_excerpt"]) <= PREVIEW_HTML_EXCERPT_CAP
        # `wrap_email_html` always produces SOMETHING — never blank.
        assert result["html_body_excerpt"]

    def test_missing_display_name_falls_back_to_sleeper_username(self) -> None:
        user = {
            "sleeper_user_id": "u9",
            "email": "ghost@example.com",
            "display_name": "",
            "sleeper_username": "ghosthunter",
        }
        result = render_preview_for_user(
            user=user,
            report_type="postDraft",
            body_markdown="Body",
            period_label="Post-Draft 2026",
        )
        # display_name preserved as-is (empty)
        assert result["display_name"] == ""
        # subject still produced (uses fallback first-name)
        assert "ghosthunter" in result["subject"]


class TestRenderPreviewForUserCapDiscipline:
    def test_text_body_capped_at_4096(self) -> None:
        # Build a markdown body so long that the plain-text rendering
        # blows past the 4KB cap. The plain-text section concatenates
        # greeting + label + body + footer; using 10K of body content
        # guarantees a >4096 char result before the cap.
        big_body = "X" * 10_000
        result = render_preview_for_user(
            user=_user(),
            report_type="postDraft",
            body_markdown=big_body,
            period_label="Post-Draft 2026",
        )
        assert len(result["text_body"]) == PREVIEW_TEXT_BODY_CAP

    def test_html_body_excerpt_capped_at_500(self) -> None:
        # `wrap_email_html` is HTML-heavy by construction — even a short
        # body produces multi-KB output. Test the cap directly.
        big_body = "Hello" * 1_000
        result = render_preview_for_user(
            user=_user(),
            report_type="postDraft",
            body_markdown=big_body,
            period_label="Post-Draft 2026",
        )
        assert len(result["html_body_excerpt"]) == PREVIEW_HTML_EXCERPT_CAP

    def test_full_html_body_not_returned(self) -> None:
        """Drift-proof: even with a body that renders to a huge HTML
        envelope, the helper never leaks the full payload."""
        big_body = "Bigger\n" * 5_000
        result = render_preview_for_user(
            user=_user(),
            report_type="weekly",
            body_markdown=big_body,
            period_label="Week 12",
        )
        assert "html_body" not in result
        assert len(result["html_body_excerpt"]) <= PREVIEW_HTML_EXCERPT_CAP


class TestRenderPreviewForUserDefensive:
    def test_missing_email_yields_empty_recipient_email(self) -> None:
        user = {
            "sleeper_user_id": "u1",
            "display_name": "Manager1",
        }
        result = render_preview_for_user(
            user=user,
            report_type="postDraft",
            body_markdown="Body",
            period_label="Post-Draft 2026",
        )
        assert result["recipient_email"] == ""
        # Subject is still rendered, even with no email address.
        assert "Manager1" in result["subject"]

    def test_missing_user_id_yields_empty_string(self) -> None:
        user = {
            "email": "no-id@example.com",
            "display_name": "Limbo",
        }
        result = render_preview_for_user(
            user=user,
            report_type="postDraft",
            body_markdown="Body",
            period_label="Post-Draft 2026",
        )
        assert result["recipient_user_id"] == ""
