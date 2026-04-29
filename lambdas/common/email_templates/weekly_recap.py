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
) -> str:
    """Generate HTML email for weekly recap.

    `all_scores` is a list of (team_name, points) sorted desc. Renders
    as the league standings table at the bottom of the recap.
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

    <!-- Standings table -->
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
) -> str:
    if is_tie:
        result = "Tied"
    else:
        result = "Won" if user_won else "Lost"
    table_lines = [f"{i + 1}. {team}: {pts:.2f}" for i, (team, pts) in enumerate(all_scores)]
    return (
        f"Hey {manager_name},\n\n"
        f"Week {week} {league_name} recap.\n\n"
        f"{result}: {user_team_name} {user_points:.2f} vs "
        f"{opponent_team_name} {opponent_points:.2f}\n"
        f"League high: {league_high_team} ({league_high_points:.2f})\n\n"
        f"Standings:\n" + "\n".join(table_lines) + "\n\n"
        f"Open Xomper: {XOMPER_URL}\n"
    )
