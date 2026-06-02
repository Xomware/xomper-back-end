"""
Week Preview - Wednesday morning forward-looking newsletter
============================================================
Fires Wed 9am ET. Hybrid layout: AI-generated markdown body up top,
HTML standings + World Cup standings tables below. Preview is one
email per league (not personalized per recipient) since the content
is forward-looking and identical for everyone.

Same recipient (one-per-manager) plumbing as the weekly recap to keep
audit + notification_log shapes consistent.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.email_templates.base import (
    wrap_email_html,
    generate_section_title,
    generate_league_badge,
    generate_button,
    _escape,
    CHAMPION_GOLD, TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED,
    DARK_NAVY, SURFACE_LIGHT, SUCCESS_GREEN, ACCENT_RED,
    FONT_BODY, FONT_DISPLAY, XOMPER_URL,
)


def generate_week_preview_email(
    *,
    manager_name: str,
    league_name: str,
    week: int,
    body_markdown: str,
    standings: list[tuple[str, int, int, int, float]] | None = None,
    wc_divisions: list[tuple[str, list[dict[str, Any]]]] | None = None,
) -> str:
    """HTML email for the Week Preview newsletter.

    Args:
        manager_name: Recipient's display name (used in the salutation).
        league_name: Sleeper league name.
        week: Upcoming week N.
        body_markdown: AI-generated markdown — rendered server-side as
            a stripped-down HTML view (we don't want to pull a full
            markdown parser into the lambda layer, so we treat the body
            as already-formatted text with `\\n\\n` paragraph breaks +
            `#`/`##`/`###` headings).
        standings: cumulative standings — (team, wins, losses, ties, pf).
        wc_divisions: World Cup standings by division.
    """
    safe_manager = _escape(manager_name)
    safe_league = _escape(league_name)

    # Two-pass render. First extract every `##` heading from the AI
    # body. Then append our HTML-side sections (standings + WC) so the
    # TOC reflects EVERY section of the email, including ones rendered
    # outside the markdown body. Each pill links to an `id=` slug
    # injected on the matching header below.
    h2_sections = list(_extract_h2_sections(body_markdown or ""))
    if standings:
        h2_sections.append("League Pulse")
    if wc_divisions:
        h2_sections.append("World Cup Standings")
    toc_html = _toc_section(h2_sections) if h2_sections else ""
    body_html = _markdown_to_email_html(body_markdown or "")

    standings_html = _standings_section(standings) if standings else ""
    wc_html = _wc_section(wc_divisions) if wc_divisions else ""

    content = f"""
    {generate_section_title(f"Week {week} Preview")}
    {generate_league_badge(safe_league) if safe_league else ""}

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px; font-family: {FONT_BODY}; font-size: 16px; color: {TEXT_PRIMARY}; line-height: 1.6;">
                <p style="margin: 0 0 12px;">Hey {safe_manager},</p>
                <p style="margin: 0; color: {TEXT_SECONDARY}; font-size: 14px;">
                    Sunday's slate is locked. Here's what's at stake this week.
                </p>
            </td>
        </tr>
    </table>

    {toc_html}

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px; font-family: {FONT_BODY}; font-size: 15px; color: {TEXT_PRIMARY}; line-height: 1.55;">
                {body_html}
            </td>
        </tr>
    </table>

    {standings_html}
    {wc_html}

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td align="center" style="padding: 0 24px 24px;">
                {generate_button("Open Xomper", XOMPER_URL)}
            </td>
        </tr>
    </table>
    """

    preheader = f"Week {week} preview — bracket, World Cup, game by game."
    return wrap_email_html(content, preheader_text=preheader)


def generate_week_preview_email_plain_text(
    *,
    manager_name: str,
    league_name: str,
    week: int,
    body_markdown: str,
    standings: list[tuple[str, int, int, int, float]] | None = None,
    wc_divisions: list[tuple[str, list[dict[str, Any]]]] | None = None,
) -> str:
    parts: list[str] = [
        f"Hey {manager_name},",
        "",
        f"Week {week} {league_name} preview.",
        "",
        body_markdown.strip(),
    ]
    if standings:
        parts.append("")
        parts.append("Standings:")
        for i, (team, w, l, t, pf) in enumerate(standings):
            record = f"{w}-{l}-{t}" if t else f"{w}-{l}"
            parts.append(f"{i + 1}. {team}: {record}, {pf:.1f} PF")
    if wc_divisions:
        parts.append("")
        parts.append("World Cup:")
        for div_name, rows in wc_divisions:
            parts.append(f"  {div_name}")
            for r in rows:
                w = r.get("wins", 0); l = r.get("losses", 0); t = r.get("ties", 0)
                record = f"{w}-{l}-{t}" if t else f"{w}-{l}"
                parts.append(
                    f"    {r.get('team_name', '')}: {record}, "
                    f"{r.get('points_for', 0):.1f} PF · {r.get('status', 'alive')}"
                )
    parts.append("")
    parts.append(f"Open Xomper: {XOMPER_URL}")
    return "\n".join(parts) + "\n"


# ---------------------------------------------------------------------------
# Lightweight markdown → HTML pass tailored to the AI body's shape.
#
# We DON'T pull a real markdown parser into the lambda layer (extra
# weight, more deps to keep on cp313). The AI's output is constrained
# to a small subset by the prompt — `#`/`##`/`###` headings, blank-line
# paragraphs, `**bold**`, list dashes — so a 20-line stub covers it.
# ---------------------------------------------------------------------------


def _markdown_to_email_html(md: str) -> str:
    if not md.strip():
        return ""
    blocks = [b.strip() for b in md.split("\n\n") if b.strip()]
    out_parts: list[str] = []
    for block in blocks:
        out_parts.append(_render_block(block))
    return "\n".join(out_parts)


def _render_block(block: str) -> str:
    # Headings
    if block.startswith("### "):
        raw = block[4:].strip()
        text = _inline(raw)
        return f'<h3 style="margin: 18px 0 6px; font-family: {FONT_DISPLAY}; font-size: 15px; color: {CHAMPION_GOLD}; font-weight: 700;">{text}</h3>'
    if block.startswith("## "):
        raw = block[3:].strip()
        text = _inline(raw)
        slug = _slug(raw)
        # Red color + uppercase + divider strip above act as visual
        # bookmarks. `id={slug}` lets the TOC anchor links jump here
        # in clients that respect them (Gmail web, Apple Mail).
        return (
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin: 28px 0 8px;">'
            f'<tr><td style="border-top: 2px solid {ACCENT_RED}; padding: 12px 0 0;">'
            f'<h2 id="{slug}" style="margin: 0; font-family: {FONT_DISPLAY}; font-size: 19px; color: {ACCENT_RED}; font-weight: 800; text-transform: uppercase; letter-spacing: 1.5px;">{text}</h2>'
            f'</td></tr></table>'
        )
    if block.startswith("# "):
        text = _inline(block[2:].strip())
        return f'<h1 style="margin: 8px 0 12px; font-family: {FONT_DISPLAY}; font-size: 22px; color: {TEXT_PRIMARY}; font-weight: 700;">{text}</h1>'
    # Quote
    if block.startswith("> "):
        text = _inline(block[2:].strip())
        return f'<blockquote style="margin: 8px 0; padding: 8px 14px; border-left: 3px solid {CHAMPION_GOLD}; color: {TEXT_SECONDARY}; font-style: italic;">{text}</blockquote>'
    # Plain paragraph — inline bold + bare line
    text = _inline(block.replace("\n", "<br/>"))
    return f'<p style="margin: 0 0 10px; line-height: 1.55;">{text}</p>'


# ---------------------------------------------------------------------------
# Table of contents + anchor helpers
# ---------------------------------------------------------------------------


def _extract_h2_sections(md: str) -> list[str]:
    """Return the raw label for each `## Heading` in source order.
    Used to build both the TOC pills and the `id=` slugs on the
    rendered h2 tags."""
    sections: list[str] = []
    for line in md.splitlines():
        stripped = line.strip()
        if stripped.startswith("## ") and not stripped.startswith("### "):
            sections.append(stripped[3:].strip())
    return sections


def _slug(text: str) -> str:
    """URL-safe id for anchor links. Lowercase, alphanumerics + dashes."""
    out: list[str] = []
    for ch in text.lower():
        if ch.isalnum():
            out.append(ch)
        elif ch in (" ", "-", "_"):
            out.append("-")
    slug = "".join(out).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "section"


def _toc_section(headings: list[str]) -> str:
    """Pill outline of the email's sections. Anchor-links to each `##`
    section. Gmail strips the jump behavior (best-effort across clients)
    — when anchors don't fire, the row still works as a visible "in
    this email" outline at the top. Apple Mail honors the jumps."""
    if not headings:
        return ""
    pills = ""
    for label in headings:
        slug = _slug(label)
        safe_label = _escape(label)
        pills += (
            f'<a href="#{slug}" '
            f'style="display: inline-block; margin: 4px 6px 4px 0; '
            f'padding: 6px 12px; font-family: {FONT_DISPLAY}; font-size: 11px; '
            f'color: {ACCENT_RED}; text-decoration: none; '
            f'background-color: {SURFACE_LIGHT}; border-radius: 999px; '
            f'border: 1px solid {ACCENT_RED}; '
            f'letter-spacing: 1px; text-transform: uppercase; font-weight: 700;">'
            f'{safe_label}'
            f'</a>'
        )
    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px;">
                <div style="padding: 8px 0 4px;">
                    <div style="font-family: {FONT_DISPLAY}; font-size: 10px; color: {TEXT_MUTED}; letter-spacing: 1px; text-transform: uppercase; margin-bottom: 6px;">In this email</div>
                    {pills}
                </div>
            </td>
        </tr>
    </table>
    """


def _h2_section_header(label: str) -> str:
    """Red h2 banner matching `_render_block`'s `## ...` output. Used
    for the HTML-side sections (standings, WC) so they match the
    AI-body section headers visually + carry an anchor id."""
    safe_label = _escape(label)
    slug = _slug(label)
    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin: 28px 0 8px;">
        <tr>
            <td style="padding: 12px 24px 0; border-top: 2px solid {ACCENT_RED};">
                <h2 id="{slug}" style="margin: 0; font-family: {FONT_DISPLAY}; font-size: 19px; color: {ACCENT_RED}; font-weight: 800; text-transform: uppercase; letter-spacing: 1.5px;">{safe_label}</h2>
            </td>
        </tr>
    </table>
    """


def _inline(text: str) -> str:
    # Escape first, then re-introduce bold spans.
    escaped = _escape(text)
    # Naive **bold** pass — handles the AI's standard `**X**` usage.
    out = ""
    i = 0
    while i < len(escaped):
        if escaped[i:i + 2] == "**":
            end = escaped.find("**", i + 2)
            if end == -1:
                out += escaped[i:]
                break
            out += f'<strong style="color: {TEXT_PRIMARY};">{escaped[i + 2:end]}</strong>'
            i = end + 2
        else:
            out += escaped[i]
            i += 1
    return out


def _standings_section(
    standings: list[tuple[str, int, int, int, float]],
) -> str:
    rows = ""
    for idx, (team, w, l, t, pf) in enumerate(standings):
        bg = SURFACE_LIGHT if (idx % 2 == 1) else "transparent"
        record = f"{w}-{l}-{t}" if t else f"{w}-{l}"
        rows += f"""
        <tr>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_MUTED}; width: 28px;">{idx + 1}</td>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 14px; color: {TEXT_PRIMARY};">{_escape(team)}</td>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_SECONDARY}; text-align: right;" align="right">{record}</td>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_SECONDARY}; text-align: right;" align="right">{pf:.1f}</td>
        </tr>
        """
    return f"""
    {_h2_section_header("League Pulse")}
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                       style="border: 1px solid {SURFACE_LIGHT}; border-radius: 8px; overflow: hidden;">
                    <tr style="background-color: {DARK_NAVY};">
                        <td style="padding: 10px 12px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {TEXT_MUTED}; letter-spacing: 1px; text-transform: uppercase;">#</td>
                        <td style="padding: 10px 12px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {TEXT_MUTED}; letter-spacing: 1px; text-transform: uppercase;">Team</td>
                        <td style="padding: 10px 12px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {TEXT_MUTED}; letter-spacing: 1px; text-transform: uppercase; text-align: right;" align="right">Record</td>
                        <td style="padding: 10px 12px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {TEXT_MUTED}; letter-spacing: 1px; text-transform: uppercase; text-align: right;" align="right">PF</td>
                    </tr>
                    {rows}
                </table>
            </td>
        </tr>
    </table>
    """


def _wc_section(
    wc_divisions: list[tuple[str, list[dict[str, Any]]]],
) -> str:
    div_blocks = ""
    for div_name, teams in wc_divisions:
        rows = ""
        for idx, t in enumerate(teams):
            status = (t.get("status") or "alive").lower()
            marker = " ⭐" if status == "clinched" else (" ✕" if status == "eliminated" else "")
            bg = SURFACE_LIGHT if (idx % 2 == 1) else "transparent"
            name_color = TEXT_PRIMARY if status == "clinched" else TEXT_SECONDARY
            w = t.get("wins", 0); l = t.get("losses", 0); tie = t.get("ties", 0)
            record = f"{w}-{l}-{tie}" if tie else f"{w}-{l}"
            rows += f"""
            <tr>
                <td style="padding: 6px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {name_color};">{_escape(t.get('team_name', ''))}{marker}</td>
                <td style="padding: 6px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_SECONDARY}; text-align: right;" align="right">{record}</td>
                <td style="padding: 6px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_SECONDARY}; text-align: right;" align="right">{t.get('points_for', 0):.1f}</td>
            </tr>
            """
        div_blocks += f"""
        <tr>
            <td style="padding: 6px 24px 4px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {CHAMPION_GOLD}; letter-spacing: 1px; text-transform: uppercase;">{_escape(div_name)}</td>
        </tr>
        <tr>
            <td style="padding: 0 24px 12px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                       style="border: 1px solid {SURFACE_LIGHT}; border-radius: 8px; overflow: hidden;">
                    {rows}
                </table>
            </td>
        </tr>
        """
    return f"""
    {_h2_section_header("World Cup Standings")}
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        {div_blocks}
    </table>
    """
