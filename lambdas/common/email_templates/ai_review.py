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

from lambdas.common.email_templates.base import (
    wrap_email_html,
    generate_section_title,
    generate_league_badge,
    generate_button,
    generate_toc,
    render_markdown_body,
    extract_h2_sections,
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
    """Convert the markdown body Claude produced into HTML using the
    shared styled renderer from `base.py`. Every email template
    (week_preview, weekly_recap, ai_review) flows through the same
    renderer so heading + paragraph styles stay consistent across the
    inbox. Returns an empty string if the body is empty."""
    if not body_markdown:
        return ""
    return render_markdown_body(body_markdown)


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

    # Pull out the AI body's `## Section` headings for the TOC card.
    # Only emit the TOC when the body actually has 2+ sections so a
    # short one-pager doesn't grow an awkward 1-row outline.
    toc_labels = extract_h2_sections(body_markdown or "")
    toc_html = generate_toc(toc_labels) if len(toc_labels) >= 2 else ""

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

    {toc_html}

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


# ---------------------------------------------------------------------------
# Preview helper (Admin Portal F2)
# ---------------------------------------------------------------------------
#
# The admin pre-broadcast preview surface needs the *same* rendered email
# every recipient would receive on a real broadcast — single render path
# so the preview can't drift from `send_emails_concurrently`.
#
# Caps applied here (NOT in `build_email_payload` — the wire payload to
# SES stays full-fidelity):
#   * `text_body`         -> first 4096 chars (≈ 4KB hard cap)
#   * `html_body`         -> dropped; replaced with `html_body_excerpt`
#                            of the first 500 chars only
#
# 12 previews × ~4.5KB ≈ 54KB worst case — well under API GW's 10MB limit.

PREVIEW_TEXT_BODY_CAP = 4096
PREVIEW_HTML_EXCERPT_CAP = 500


def render_preview_for_user(
    user: dict[str, Any],
    report_type: str,
    body_markdown: str,
    period_label: str,
    league_name: str | None = None,
) -> dict[str, Any]:
    """Render an email preview row for a single whitelisted user.

    Wraps `build_email_payload` so the preview shape is produced by the
    exact same template path used by the real broadcast — no drift risk.

    Args:
        user: A row from `get_active_whitelisted_users()`. Must carry
            `sleeper_user_id`, `email`, and `display_name`.
            `sleeper_username` is used as a fallback first-name source.
        report_type: One of `postDraft` | `preseason` | `weekly`.
        body_markdown: The Claude-produced report body.
        period_label: Human-readable period (e.g. "Week 4",
            "Preseason 2026-PRESEASON", "Post-Draft 2026"). Threaded
            into the subject + section title via `build_email_payload`.
        league_name: Optional league name to badge inside the email.

    Returns:
        Dict with `recipient_user_id`, `recipient_email`, `display_name`,
        `subject`, `text_body` (≤4096 chars), and `html_body_excerpt`
        (≤500 chars). The full `html_body` is intentionally dropped to
        keep the response payload mobile-friendly.
    """
    email = user.get("email") or ""
    display_name = user.get("display_name") or ""
    first_name = (
        display_name
        or user.get("sleeper_username")
        or "there"
    )

    payload = build_email_payload(
        user_email=email,
        user_first_name=first_name,
        report_type=report_type,
        body_markdown=body_markdown,
        period_label=period_label,
        league_name=league_name,
    )

    text_body = payload.get("text_body") or ""
    html_body = payload.get("html_body") or ""

    return {
        "recipient_user_id": user.get("sleeper_user_id") or "",
        "recipient_email": email,
        "display_name": display_name,
        "subject": payload.get("subject") or "",
        "text_body": text_body[:PREVIEW_TEXT_BODY_CAP],
        "html_body_excerpt": html_body[:PREVIEW_HTML_EXCERPT_CAP],
    }
