"""
AI Review email template
========================
Renders the AI Review (post-draft / preseason / weekly) markdown
body produced by Claude into an HTML email + plain-text fallback,
with a per-user greeting on top of the shared body.

The body Claude produces is markdown — `render_html` converts it to
HTML and `build_email_payload` wraps that HTML in the standard
Xomper email chrome with a personalized subject + greeting line.

Used by F1/F2/F3 generator lambdas (after this F0 helper ships).
"""
from __future__ import annotations

from typing import Any

import markdown as _markdown

from lambdas.common.email_templates.base import (
    wrap_email_html,
    generate_section_title,
    generate_league_badge,
    generate_button,
    _escape,
    CHAMPION_GOLD, TEXT_PRIMARY, TEXT_SECONDARY,
    FONT_BODY, XOMPER_URL,
)


_REPORT_TYPE_DISPLAY: dict[str, str] = {
    "postDraft": "Post-Draft Review",
    "preseason": "Preseason Review",
    "weekly": "Weekly Review",
}


def render_html(body_markdown: str) -> str:
    """Convert the markdown body Claude produced into HTML.

    Uses the `markdown` library's `extra` extension so we get tables,
    fenced code, and definition lists if Claude ever leans on them.
    Returns an empty string if the body is empty.
    """
    if not body_markdown:
        return ""
    return _markdown.markdown(body_markdown, extensions=["extra"])


def _report_type_label(report_type: str) -> str:
    return _REPORT_TYPE_DISPLAY.get(report_type, "AI Review")


def _wrap_body_content(
    user_first_name: str,
    report_type: str,
    period_label: str,
    body_markdown: str,
    league_name: str | None,
) -> str:
    """Build the inner content block (section title, greeting,
    rendered markdown, CTA) that sits inside the email chrome."""
    safe_name = _escape(user_first_name) if user_first_name else "there"
    body_html = render_html(body_markdown)
    section_title = f"{_report_type_label(report_type)} — {period_label}"

    league_badge_html = generate_league_badge(league_name) if league_name else ""

    return f"""
    {generate_section_title(section_title)}
    {league_badge_html}

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px; font-family: {FONT_BODY}; font-size: 16px; color: {TEXT_PRIMARY}; line-height: 1.6;">
                <p style="margin: 0 0 12px;">Hey {safe_name},</p>
            </td>
        </tr>
    </table>

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px; font-family: {FONT_BODY}; font-size: 15px; color: {TEXT_PRIMARY}; line-height: 1.6;">
                {body_html}
            </td>
        </tr>
    </table>

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td align="center" style="padding: 0 24px 24px;">
                {generate_button("Open Xomper", XOMPER_URL, CHAMPION_GOLD)}
            </td>
        </tr>
        <tr>
            <td align="center" style="padding: 0 24px 24px; font-family: {FONT_BODY}; font-size: 12px; color: {TEXT_SECONDARY};">
                You're getting this because you're in the league. Roasts are part of the package.
            </td>
        </tr>
    </table>
    """


def _plain_text(
    user_first_name: str,
    report_type: str,
    period_label: str,
    body_markdown: str,
) -> str:
    name = user_first_name or "there"
    label = _report_type_label(report_type)
    return (
        f"Hey {name},\n\n"
        f"{label} — {period_label}\n\n"
        f"{body_markdown.strip()}\n\n"
        f"Open Xomper: {XOMPER_URL}\n"
    )


def build_email_payload(
    user_email: str,
    user_first_name: str,
    report_type: str,
    body_markdown: str,
    period_label: str,
    league_name: str | None = None,
) -> dict[str, Any]:
    """Render a per-user email payload for a single AI Review report.

    Args:
        user_email: Recipient email — included in the returned dict
            so the F1/F2/F3 fan-out can hand the payload straight to
            `ses_helper.send_emails_concurrently` without re-deriving
            the address.
        user_first_name: Used in the subject + greeting line.
        report_type: One of `postDraft` | `preseason` | `weekly`.
        body_markdown: The shared report body produced by Claude.
        period_label: Human-readable period (e.g. "Week 4",
            "Preseason 2026", "Post-Draft 2026"). Goes in the subject
            and the section title.
        league_name: Optional league name to badge inside the email.

    Returns:
        Dict with `recipient`, `subject`, `html_body`, `text_body`.
    """
    name = user_first_name or "there"
    label = _report_type_label(report_type)
    subject = f"{name}, your {period_label} {label}"

    content = _wrap_body_content(
        user_first_name=user_first_name,
        report_type=report_type,
        period_label=period_label,
        body_markdown=body_markdown,
        league_name=league_name,
    )
    html_body = wrap_email_html(
        content,
        preheader_text=f"{label} — {period_label}",
    )
    text_body = _plain_text(
        user_first_name=user_first_name,
        report_type=report_type,
        period_label=period_label,
        body_markdown=body_markdown,
    )

    return {
        "recipient": user_email,
        "subject": subject,
        "html_body": html_body,
        "text_body": text_body,
    }
