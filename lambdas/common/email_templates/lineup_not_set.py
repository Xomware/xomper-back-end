"""
Lineup Not Set - Sunday morning reminder
========================================
Sent Sunday morning to managers whose current-week starting lineup
contains players that are out / on bye / injured. Pairs with the
push notification from `notif_lineup_not_set`.
"""

from lambdas.common.email_templates.base import (
    wrap_email_html,
    generate_section_title,
    generate_league_badge,
    generate_button,
    _escape,
    CHAMPION_GOLD, TEXT_PRIMARY, TEXT_SECONDARY,
    DARK_NAVY, SURFACE_LIGHT, ACCENT_RED,
    FONT_BODY, XOMPER_URL,
)


def generate_lineup_not_set_email(
    manager_name: str,
    league_name: str,
    issue_count: int,
    week: int,
) -> str:
    """Generate HTML email for lineup-not-set reminder."""
    safe_manager = _escape(manager_name)
    plural = "s" if issue_count != 1 else ""

    content = f"""
    {generate_section_title("Set Your Lineup")}
    {generate_league_badge(league_name) if league_name else ""}

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px; font-family: {FONT_BODY}; font-size: 16px; color: {TEXT_PRIMARY}; line-height: 1.6;">
                <p style="margin: 0 0 12px;">Hey {safe_manager},</p>
                <p style="margin: 0 0 12px;">
                    Your <strong>Week {week}</strong> lineup has
                    <span style="color: {ACCENT_RED}; font-weight: 700;">
                        {issue_count} starter{plural}
                    </span>
                    that need attention — empty slot, bye week, or marked OUT.
                </p>
                <p style="margin: 0 0 16px; color: {TEXT_SECONDARY}; font-size: 14px;">
                    Fix it in Sleeper before kickoff so you're not playing short-handed.
                </p>
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

    preheader = f"{issue_count} starter{plural} need attention before kickoff."
    return wrap_email_html(content, preheader_text=preheader)


def generate_lineup_not_set_email_plain_text(
    manager_name: str,
    league_name: str,
    issue_count: int,
    week: int,
) -> str:
    plural = "s" if issue_count != 1 else ""
    return (
        f"Hey {manager_name},\n\n"
        f"Your Week {week} {league_name} lineup has {issue_count} starter{plural} "
        f"that need attention — empty, on bye, or marked OUT.\n\n"
        f"Open Xomper to fix it before kickoff: {XOMPER_URL}\n"
    )
