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
    generate_h2_red_header,
    generate_toc,
    render_markdown_body,
    extract_h2_sections,
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
    h2_sections = list(extract_h2_sections(body_markdown or ""))
    if standings:
        h2_sections.append("League Pulse")
    if wc_divisions:
        h2_sections.append("World Cup Standings")
    toc_html = generate_toc(h2_sections) if h2_sections else ""
    body_html = render_markdown_body(body_markdown or "")

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
# All markdown rendering + TOC + h2 helpers now live in base.py
# (`render_markdown_body`, `generate_h2_red_header`, `generate_toc`,
# `extract_h2_sections`) so every email template can share them.
# ---------------------------------------------------------------------------


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
    {generate_h2_red_header("League Pulse")}
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
    {generate_h2_red_header("World Cup Standings")}
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        {div_blocks}
    </table>
    """
