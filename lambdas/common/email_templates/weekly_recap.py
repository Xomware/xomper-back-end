"""
Weekly Recap - Tuesday morning per-manager summary
==================================================
Sent Tuesday morning summarizing the just-completed week. Each manager
gets their own personalized recap (their result + league context).
"""

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


def generate_weekly_recap_email(
    manager_name: str,
    league_name: str,
    week: int,
    user_team_name: str,
    user_points: float,
    opponent_team_name: str,
    opponent_points: float,
    user_won: bool,
    is_tie: bool,
    league_high_team: str,
    league_high_points: float,
    all_scores: list[tuple[str, float]],
    standings: list[tuple[str, int, int, int, float]] | None = None,
    bracket_rounds: list[tuple[str, list[tuple[str, float, str, float]]]] | None = None,
    wc_personal: dict | None = None,
    wc_divisions: list[tuple[str, list[dict]]] | None = None,
) -> str:
    """Generate HTML email for weekly recap.

    `all_scores` is a list of (team_name, points) sorted desc — renders
    as the THIS WEEK scoreboard.

    `standings` is the cumulative season table, list of
    (team_name, wins, losses, ties, points_for) sorted in ranking order.
    Rendered always when present.

    `bracket_rounds` is the playoff bracket as
    (round_label, [(team_a, score_a, team_b, score_b)]) per round.
    Only rendered when week >= 12 AND non-empty.

    `wc_personal` (optional) is the recipient's World Cup standing —
    keys: `division`, `position` (1..N), `status` (clinched|alive|
    eliminated), `wins`, `points_for`, `points_back` (vs cutoff, None
    if clinched or N/A).

    `wc_divisions` (optional) is the full World Cup standings table
    grouped by division — list of (division_name, [team_dict]) where
    each team_dict has keys: `team_name`, `wins`, `losses`, `ties`,
    `points_for`, `status`, `is_user`.
    """
    safe_manager = _escape(manager_name)
    safe_team = _escape(user_team_name)
    safe_opp = _escape(opponent_team_name)
    safe_high_team = _escape(league_high_team)

    if is_tie:
        result_color = TEXT_SECONDARY
        result_text = "Tied"
        result_emoji = "🤝"
    elif user_won:
        result_color = SUCCESS_GREEN
        result_text = "Won"
        result_emoji = "✅"
    else:
        result_color = ACCENT_RED
        result_text = "Lost"
        result_emoji = "❌"

    score_rows = ""
    for idx, (team, pts) in enumerate(all_scores):
        is_mine = team == user_team_name
        is_high = team == league_high_team
        bg = SURFACE_LIGHT if (idx % 2 == 1) else "transparent"
        team_color = CHAMPION_GOLD if is_high else (TEXT_PRIMARY if is_mine else TEXT_SECONDARY)
        weight = "700" if (is_mine or is_high) else "400"
        score_rows += f"""
        <tr>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_MUTED}; width: 28px;">
                {idx + 1}
            </td>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 14px; color: {team_color}; font-weight: {weight};">
                {_escape(team)}{' 👑' if is_high else ''}
            </td>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 14px; color: {team_color}; font-weight: 700; text-align: right;">
                {pts:.2f}
            </td>
        </tr>
        """

    standings_html = _standings_section(standings, user_team_name) if standings else ""
    bracket_html = _bracket_section(bracket_rounds) if (bracket_rounds and week >= 12) else ""
    wc_html = _worldcup_section(wc_personal, wc_divisions) if (wc_personal or wc_divisions) else ""

    content = f"""
    {generate_section_title(f"Week {week} Recap")}
    {generate_league_badge(league_name) if league_name else ""}

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px; font-family: {FONT_BODY}; font-size: 16px; color: {TEXT_PRIMARY}; line-height: 1.6;">
                <p style="margin: 0 0 12px;">Hey {safe_manager},</p>
                <p style="margin: 0 0 8px;">
                    <span style="color: {result_color}; font-weight: 700;">
                        {result_emoji} {result_text}
                    </span>
                    — {safe_team} <strong>{user_points:.2f}</strong>
                    vs {safe_opp} <strong>{opponent_points:.2f}</strong>.
                </p>
                <p style="margin: 0; color: {TEXT_SECONDARY}; font-size: 14px;">
                    League high: <span style="color: {CHAMPION_GOLD}; font-weight: 700;">{safe_high_team}</span> · {league_high_points:.2f}
                </p>
            </td>
        </tr>
    </table>

    {_section_header("This week's scores")}

    <!-- This week's scoreboard -->
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                       style="border: 1px solid {SURFACE_LIGHT}; border-radius: 8px; overflow: hidden;">
                    <tr style="background-color: {DARK_NAVY};">
                        <td style="padding: 10px 12px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {TEXT_MUTED}; letter-spacing: 1px; text-transform: uppercase;">
                            #
                        </td>
                        <td style="padding: 10px 12px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {TEXT_MUTED}; letter-spacing: 1px; text-transform: uppercase;">
                            Team
                        </td>
                        <td style="padding: 10px 12px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {TEXT_MUTED}; letter-spacing: 1px; text-transform: uppercase; text-align: right;">
                            Pts
                        </td>
                    </tr>
                    {score_rows}
                </table>
            </td>
        </tr>
    </table>

    {standings_html}
    {bracket_html}
    {wc_html}

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td align="center" style="padding: 0 24px 24px;">
                {generate_button("Open Xomper", XOMPER_URL)}
            </td>
        </tr>
    </table>
    """

    preheader = f"{result_text} {user_points:.1f}–{opponent_points:.1f}. League high: {league_high_team} {league_high_points:.1f}."
    return wrap_email_html(content, preheader_text=preheader)


def generate_weekly_recap_email_plain_text(
    manager_name: str,
    league_name: str,
    week: int,
    user_team_name: str,
    user_points: float,
    opponent_team_name: str,
    opponent_points: float,
    user_won: bool,
    is_tie: bool,
    league_high_team: str,
    league_high_points: float,
    all_scores: list[tuple[str, float]],
    standings: list[tuple[str, int, int, int, float]] | None = None,
    bracket_rounds: list[tuple[str, list[tuple[str, float, str, float]]]] | None = None,
    wc_personal: dict | None = None,
    wc_divisions: list[tuple[str, list[dict]]] | None = None,
) -> str:
    if is_tie:
        result = "Tied"
    else:
        result = "Won" if user_won else "Lost"
    score_lines = [f"{i + 1}. {team}: {pts:.2f}" for i, (team, pts) in enumerate(all_scores)]

    parts: list[str] = [
        f"Hey {manager_name},",
        "",
        f"Week {week} {league_name} recap.",
        "",
        f"{result}: {user_team_name} {user_points:.2f} vs "
        f"{opponent_team_name} {opponent_points:.2f}",
        f"League high: {league_high_team} ({league_high_points:.2f})",
        "",
        "This week's scores:",
        *score_lines,
    ]

    if standings:
        parts.append("")
        parts.append("Standings (cumulative):")
        for i, (team, w, l, t, pf) in enumerate(standings):
            record = f"{w}-{l}-{t}" if t else f"{w}-{l}"
            parts.append(f"{i + 1}. {team}: {record}, {pf:.1f} PF")

    if bracket_rounds and week >= 12:
        parts.append("")
        parts.append("Playoff bracket:")
        for round_label, pairs in bracket_rounds:
            parts.append(f"  {round_label}")
            for a, sa, b, sb in pairs:
                if sa is not None and sb is not None:
                    parts.append(f"    {a} {sa:.1f} vs {b} {sb:.1f}")
                else:
                    parts.append(f"    {a} vs {b}")

    if wc_personal:
        parts.append("")
        parts.append("World Cup:")
        div = wc_personal.get("division") or ""
        pos = wc_personal.get("position") or "?"
        status = wc_personal.get("status") or "alive"
        wins = wc_personal.get("wins")
        pf = wc_personal.get("points_for")
        line = f"  You are #{pos} in {div} — {status}"
        if wins is not None and pf is not None:
            line += f" ({wins} divisional wins, {pf:.1f} PF)"
        parts.append(line)
        pb = wc_personal.get("points_back")
        if pb is not None:
            parts.append(f"  {pb:.1f} pts back from a qualifying spot")

    if wc_divisions:
        parts.append("")
        parts.append("Full World Cup standings:")
        for div_name, teams in wc_divisions:
            parts.append(f"  {div_name}")
            for t in teams:
                marker = "  ← you" if t.get("is_user") else ""
                w = t.get("wins", 0)
                l = t.get("losses", 0)
                tie = t.get("ties", 0)
                record = f"{w}-{l}-{tie}" if tie else f"{w}-{l}"
                parts.append(
                    f"    {t.get('team_name','')}: {record}, "
                    f"{t.get('points_for', 0):.1f} PF · {t.get('status','alive')}{marker}"
                )

    parts.append("")
    parts.append(f"Open Xomper: {XOMPER_URL}")
    return "\n".join(parts) + "\n"


# ---------------------------------------------------------------------------
# Section renderers — HTML helpers kept private to this module
# ---------------------------------------------------------------------------


def _section_header(label: str) -> str:
    """Inline section divider with uppercase label. Matches the
    `generate_section_title` look-and-feel but smaller (sub-sections)."""
    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 8px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {TEXT_MUTED}; letter-spacing: 1px; text-transform: uppercase;">
                {_escape(label)}
            </td>
        </tr>
    </table>
    """


def _standings_section(
    standings: list[tuple[str, int, int, int, float]],
    user_team_name: str,
) -> str:
    rows = ""
    for idx, (team, w, l, t, pf) in enumerate(standings):
        is_mine = team == user_team_name
        bg = SURFACE_LIGHT if (idx % 2 == 1) else "transparent"
        weight = "700" if is_mine else "400"
        team_color = TEXT_PRIMARY if is_mine else TEXT_SECONDARY
        record = f"{w}-{l}-{t}" if t else f"{w}-{l}"
        rows += f"""
        <tr>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_MUTED}; width: 28px;">{idx + 1}</td>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 14px; color: {team_color}; font-weight: {weight};">{_escape(team)}</td>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_SECONDARY}; text-align: right;" align="right">{record}</td>
            <td style="padding: 8px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_SECONDARY}; text-align: right;" align="right">{pf:.1f}</td>
        </tr>
        """
    return f"""
    {_section_header("Standings")}
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


def _bracket_section(
    bracket_rounds: list[tuple[str, list[tuple[str, float, str, float]]]],
) -> str:
    blocks = ""
    for round_label, pairs in bracket_rounds:
        row_html = ""
        for a, sa, b, sb in pairs:
            if sa is not None and sb is not None:
                a_winner = sa > sb
                b_winner = sb > sa
                a_color = SUCCESS_GREEN if a_winner else (ACCENT_RED if b_winner else TEXT_SECONDARY)
                b_color = SUCCESS_GREEN if b_winner else (ACCENT_RED if a_winner else TEXT_SECONDARY)
                row_html += f"""
                <tr>
                    <td style="padding: 6px 12px; font-family: {FONT_BODY}; font-size: 13px; color: {a_color}; font-weight: 700;">{_escape(a)}</td>
                    <td style="padding: 6px 12px; font-family: {FONT_BODY}; font-size: 13px; color: {a_color}; text-align: right;" align="right">{sa:.1f}</td>
                    <td style="padding: 6px 6px; font-family: {FONT_BODY}; font-size: 11px; color: {TEXT_MUTED}; text-align: center;" align="center">vs</td>
                    <td style="padding: 6px 12px; font-family: {FONT_BODY}; font-size: 13px; color: {b_color}; text-align: right;" align="right">{sb:.1f}</td>
                    <td style="padding: 6px 12px; font-family: {FONT_BODY}; font-size: 13px; color: {b_color}; font-weight: 700;">{_escape(b)}</td>
                </tr>
                """
            else:
                row_html += f"""
                <tr>
                    <td style="padding: 6px 12px; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_SECONDARY};">{_escape(a)}</td>
                    <td colspan="3" style="padding: 6px 6px; font-family: {FONT_BODY}; font-size: 11px; color: {TEXT_MUTED}; text-align: center;" align="center">vs</td>
                    <td style="padding: 6px 12px; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_SECONDARY};">{_escape(b)}</td>
                </tr>
                """
        blocks += f"""
        <tr>
            <td style="padding: 6px 24px 4px; font-family: {FONT_DISPLAY}; font-size: 11px; color: {CHAMPION_GOLD}; letter-spacing: 1px; text-transform: uppercase;">{_escape(round_label)}</td>
        </tr>
        <tr>
            <td style="padding: 0 24px 12px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                       style="border: 1px solid {SURFACE_LIGHT}; border-radius: 8px;">
                    {row_html}
                </table>
            </td>
        </tr>
        """
    return f"""
    {_section_header("Playoff Bracket")}
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        {blocks}
    </table>
    """


def _worldcup_section(
    wc_personal: dict | None,
    wc_divisions: list[tuple[str, list[dict]]] | None,
) -> str:
    personal_block = ""
    if wc_personal:
        div = _escape(wc_personal.get("division") or "")
        pos = wc_personal.get("position") or "?"
        status = (wc_personal.get("status") or "alive").lower()
        status_color = {
            "clinched": SUCCESS_GREEN,
            "eliminated": ACCENT_RED,
            "alive": CHAMPION_GOLD,
        }.get(status, TEXT_SECONDARY)
        record_bits: list[str] = []
        if wc_personal.get("wins") is not None:
            w = wc_personal.get("wins", 0)
            l = wc_personal.get("losses", 0)
            t = wc_personal.get("ties", 0)
            record_bits.append(f"{w}-{l}-{t}" if t else f"{w}-{l}")
        if wc_personal.get("points_for") is not None:
            record_bits.append(f"{wc_personal['points_for']:.1f} PF")
        pb = wc_personal.get("points_back")
        pb_line = (
            f"<p style='margin: 4px 0 0; color: {TEXT_SECONDARY}; font-size: 13px;'>"
            f"{pb:.1f} pts back from a qualifying spot</p>"
            if pb is not None else ""
        )
        personal_block = f"""
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td style="padding: 0 24px 12px;">
                    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                           style="border: 1px solid {status_color}; border-radius: 8px;">
                        <tr>
                            <td style="padding: 12px 14px; font-family: {FONT_BODY}; color: {TEXT_PRIMARY};">
                                <p style="margin: 0; font-size: 15px;">
                                    You are <strong style="color: {status_color};">#{pos} in {div}</strong>
                                    — <span style="color: {status_color}; text-transform: uppercase; font-size: 12px; letter-spacing: 1px;">{_escape(status)}</span>
                                </p>
                                {('<p style="margin: 4px 0 0; color: ' + TEXT_SECONDARY + '; font-size: 13px;">' + ' · '.join(record_bits) + '</p>') if record_bits else ''}
                                {pb_line}
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
        """

    full_block = ""
    if wc_divisions:
        div_blocks = ""
        for div_name, teams in wc_divisions:
            rows = ""
            for idx, t in enumerate(teams):
                is_user = bool(t.get("is_user"))
                status = (t.get("status") or "alive").lower()
                marker = " ⭐" if status == "clinched" else (" ✕" if status == "eliminated" else "")
                bg = SURFACE_LIGHT if (idx % 2 == 1) else "transparent"
                name_color = CHAMPION_GOLD if is_user else (TEXT_PRIMARY if status == "clinched" else TEXT_SECONDARY)
                weight = "700" if is_user else "400"
                w = t.get("wins", 0)
                l = t.get("losses", 0)
                tie = t.get("ties", 0)
                record = f"{w}-{l}-{tie}" if tie else f"{w}-{l}"
                rows += f"""
                <tr>
                    <td style="padding: 6px 12px; background-color: {bg}; font-family: {FONT_BODY}; font-size: 13px; color: {name_color}; font-weight: {weight};">{_escape(t.get('team_name', ''))}{marker}</td>
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
        full_block = f"""
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
            {div_blocks}
        </table>
        """

    if not personal_block and not full_block:
        return ""

    return f"""
    {_section_header("World Cup")}
    {personal_block}
    {full_block}
    """
