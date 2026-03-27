"""
Xomper Email Templates - Base
==============================
Shared HTML wrapper, header, footer, and reusable components.
All HTML is table-based with inline CSS for email client compatibility.
"""

from lambdas.common.constants import XOMPER_URL, LOGO_URL, BANNER_LOGO_URL

# Branding colors (from _variables.scss)
DEEP_NAVY = "#050a08"
DARK_NAVY = "#0c1612"
SURFACE_LIGHT = "#1a2e26"
CHAMPION_GOLD = "#00ffab"
ACCENT_RED = "#ff4757"
SUCCESS_GREEN = "#00e676"
ERROR_RED = "#ff5252"
TEXT_PRIMARY = "#f0f5f0"
TEXT_SECONDARY = "#8fadA0"
TEXT_MUTED = "#4a6b5c"

FONT_DISPLAY = "'Bebas Neue', Impact, 'Arial Black', sans-serif"
FONT_BODY = "'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif"
FONT_MONO = "'JetBrains Mono', 'Courier New', monospace"


def generate_header() -> str:
    """Xomper banner logo header."""
    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td align="center" style="padding: 24px 0 16px;">
                <a href="{XOMPER_URL}" target="_blank" style="text-decoration: none;">
                    <img src="{BANNER_LOGO_URL}" alt="Xomper" width="240" style="display: block; max-width: 240px; height: auto; border: 0;" />
                </a>
            </td>
        </tr>
    </table>
    """


def generate_footer() -> str:
    """Standard email footer."""
    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 24px 0 8px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                        <td style="border-top: 1px solid {SURFACE_LIGHT}; padding-top: 20px;" align="center">
                            <img src="{LOGO_URL}" alt="Xomper" width="36" style="display: block; width: 36px; height: 36px; border-radius: 8px; border: 0;" />
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
        <tr>
            <td align="center" style="padding: 8px 0; font-family: {FONT_BODY}; font-size: 12px; color: {TEXT_MUTED};">
                <a href="{XOMPER_URL}" style="color: {TEXT_MUTED}; text-decoration: none;">xomper.xomware.com</a>
                &nbsp;&middot;&nbsp; Fantasy Football
            </td>
        </tr>
        <tr>
            <td align="center" style="padding: 0 0 24px; font-family: {FONT_BODY}; font-size: 11px; color: {TEXT_MUTED};">
                You received this email because you are a member of a Xomper league.
            </td>
        </tr>
    </table>
    """


def generate_button(text: str, url: str, color: str = CHAMPION_GOLD, text_color: str = DEEP_NAVY) -> str:
    """Email-safe table-based CTA button."""
    return f"""
    <table role="presentation" cellpadding="0" cellspacing="0" border="0" align="center" style="margin: 0 auto;">
        <tr>
            <td style="border-radius: 8px; background-color: {color};" align="center">
                <a href="{url}" target="_blank"
                   style="display: inline-block; padding: 14px 32px; color: {text_color};
                          text-decoration: none; font-weight: 700; font-size: 15px;
                          font-family: {FONT_BODY}; letter-spacing: 0.02em;
                          border-radius: 8px; mso-padding-alt: 0;">
                    {text}
                </a>
            </td>
        </tr>
    </table>
    """


def generate_section_title(text: str, color: str = CHAMPION_GOLD) -> str:
    """Section title bar."""
    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 20px 24px 12px; font-family: {FONT_DISPLAY}; font-size: 28px;
                        letter-spacing: 0.08em; color: {color}; text-transform: uppercase;">
                {text}
            </td>
        </tr>
    </table>
    """


def generate_league_badge(league_name: str) -> str:
    """League name badge displayed below section titles."""
    safe_name = _escape(league_name)
    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td style="padding: 0 24px 16px;">
                <span style="display: inline-block; padding: 4px 12px; background-color: {SURFACE_LIGHT};
                             border-radius: 20px; font-family: {FONT_MONO}; font-size: 11px;
                             font-weight: 600; color: {TEXT_SECONDARY}; letter-spacing: 0.03em;">
                    {safe_name}
                </span>
            </td>
        </tr>
    </table>
    """


def generate_player_card(player_name: str, position: str, team: str,
                         player_image_url: str = "", team_logo_url: str = "") -> str:
    """Player info card component with optional player headshot and team logo."""
    position_colors = {
        "QB": "#5ba3ff",
        "RB": SUCCESS_GREEN,
        "WR": CHAMPION_GOLD,
        "TE": "#ff8a65",
        "K": TEXT_SECONDARY,
        "DEF": ACCENT_RED,
    }
    pos_color = position_colors.get(position.upper(), CHAMPION_GOLD)

    # Player avatar: use headshot image if provided, otherwise position circle
    if player_image_url:
        avatar = f"""
            <img src="{player_image_url}" alt="{_escape(player_name)}" width="52" height="52"
                 style="display: block; width: 52px; height: 52px; border-radius: 50%;
                        border: 2px solid {pos_color}; object-fit: cover;" />
        """
        avatar_width = "58"
    else:
        avatar = f"""
            <div style="width: 44px; height: 44px; border-radius: 50%; background-color: {pos_color};
                        text-align: center; line-height: 44px; font-family: {FONT_MONO};
                        font-weight: 700; font-size: 14px; color: {DEEP_NAVY};">
                {position.upper()}
            </div>
        """
        avatar_width = "48"

    # Team logo next to position/team text
    team_info = f'{position.upper()} &middot; {team}'
    if team_logo_url:
        team_info = (
            f'<img src="{team_logo_url}" alt="{_escape(team)}" width="16" height="16"'
            f' style="display: inline-block; width: 16px; height: 16px; vertical-align: middle;'
            f' margin-right: 4px; border: 0;" />'
            f'<span style="vertical-align: middle;">{position.upper()} &middot; {team}</span>'
        )

    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
           style="background-color: {DARK_NAVY}; border: 1px solid {SURFACE_LIGHT}; border-radius: 10px; overflow: hidden;">
        <tr>
            <td style="padding: 16px 20px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                        <td width="{avatar_width}" valign="middle">
                            {avatar}
                        </td>
                        <td style="padding-left: 14px;" valign="middle">
                            <div style="font-family: {FONT_BODY}; font-size: 18px; font-weight: 700; color: {TEXT_PRIMARY};">
                                {player_name}
                            </div>
                            <div style="font-family: {FONT_MONO}; font-size: 13px; color: {TEXT_SECONDARY}; margin-top: 2px;">
                                {team_info}
                            </div>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    """


def generate_vote_breakdown(approved_voters: list, rejected_voters: list) -> str:
    """Vote breakdown table with green/red indicators."""
    yes_count = len(approved_voters)
    no_count = len(rejected_voters)
    total = yes_count + no_count

    # Build voter rows
    yes_rows = ""
    for name in approved_voters:
        yes_rows += f"""
        <tr>
            <td style="padding: 4px 8px; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_PRIMARY};">
                <span style="color: {SUCCESS_GREEN}; font-weight: 700;">&#10003;</span>&nbsp; {_escape(name)}
            </td>
        </tr>
        """

    no_rows = ""
    for name in rejected_voters:
        no_rows += f"""
        <tr>
            <td style="padding: 4px 8px; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_PRIMARY};">
                <span style="color: {ERROR_RED}; font-weight: 700;">&#10007;</span>&nbsp; {_escape(name)}
            </td>
        </tr>
        """

    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
           style="background-color: {DARK_NAVY}; border: 1px solid {SURFACE_LIGHT}; border-radius: 10px; overflow: hidden;">
        <!-- Vote count summary -->
        <tr>
            <td colspan="2" style="padding: 14px 20px 8px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                        <td style="font-family: {FONT_MONO}; font-size: 14px; font-weight: 700;">
                            <span style="color: {SUCCESS_GREEN};">{yes_count} YES</span>
                            <span style="color: {TEXT_MUTED};">&nbsp;&middot;&nbsp;</span>
                            <span style="color: {ERROR_RED};">{no_count} NO</span>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
        <!-- Vote progress bar -->
        <tr>
            <td colspan="2" style="padding: 0 20px 12px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                       style="background-color: {SURFACE_LIGHT}; border-radius: 3px; overflow: hidden; height: 6px;">
                    <tr>
                        {"<td width='" + str(int(yes_count / total * 100)) + "%' style='background-color: " + SUCCESS_GREEN + "; height: 6px;'></td>" if yes_count > 0 and total > 0 else ""}
                        {"<td width='" + str(int(no_count / total * 100)) + "%' style='background-color: " + ERROR_RED + "; height: 6px;'></td>" if no_count > 0 and total > 0 else ""}
                        {"<td style='height: 6px;'></td>" if total == 0 else ""}
                    </tr>
                </table>
            </td>
        </tr>
        <!-- Voter lists -->
        <tr>
            <td width="50%" valign="top" style="padding: 4px 12px 16px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                    {yes_rows if yes_rows else f'<tr><td style="padding: 4px 8px; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_MUTED};">No yes votes</td></tr>'}
                </table>
            </td>
            <td width="50%" valign="top" style="padding: 4px 12px 16px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                    {no_rows if no_rows else f'<tr><td style="padding: 4px 8px; font-family: {FONT_BODY}; font-size: 13px; color: {TEXT_MUTED};">No dissenting votes</td></tr>'}
                </table>
            </td>
        </tr>
    </table>
    """


def generate_stamp(text: str, color: str) -> str:
    """Large stamp overlay text (APPROVED / DENIED)."""
    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td align="center" style="padding: 24px 0;">
                <div style="display: inline-block; padding: 10px 28px; border: 4px solid {color};
                            border-radius: 6px; transform: rotate(-6deg); -webkit-transform: rotate(-6deg);
                            font-family: {FONT_DISPLAY}; font-size: 42px; font-weight: 900;
                            letter-spacing: 0.12em; color: {color}; text-transform: uppercase;
                            opacity: 0.9;">
                    {text}
                </div>
            </td>
        </tr>
    </table>
    """


def generate_info_card(label: str, value: str) -> str:
    """Small info card for key-value display."""
    return f"""
    <td style="padding: 8px; background-color: {DARK_NAVY}; border: 1px solid {SURFACE_LIGHT};
               border-radius: 8px; text-align: center;">
        <div style="font-family: {FONT_BODY}; font-size: 11px; font-weight: 600;
                    text-transform: uppercase; letter-spacing: 0.05em; color: {TEXT_SECONDARY};">
            {label}
        </div>
        <div style="font-family: {FONT_MONO}; font-size: 16px; font-weight: 700; color: {TEXT_PRIMARY}; margin-top: 4px;">
            {value}
        </div>
    </td>
    """


def wrap_email_html(content: str, preheader_text: str = "") -> str:
    """Wrap email content in standard HTML document with header/footer."""
    header = generate_header()
    footer = generate_footer()

    preheader = ""
    if preheader_text:
        preheader = f"""
        <div style="display: none; max-height: 0px; overflow: hidden; mso-hide: all;">
            {_escape(preheader_text)}
        </div>
        <div style="display: none; max-height: 0px; overflow: hidden; mso-hide: all;">
            &nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;
        </div>
        """

    return f"""<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="x-apple-disable-message-reformatting">
    <meta name="color-scheme" content="dark">
    <meta name="supported-color-schemes" content="dark">
    <title>Xomper Fantasy Football</title>
    <!--[if mso]>
    <noscript>
        <xml>
            <o:OfficeDocumentSettings>
                <o:PixelsPerInch>96</o:PixelsPerInch>
            </o:OfficeDocumentSettings>
        </xml>
    </noscript>
    <![endif]-->
    <style>
        body {{ margin: 0; padding: 0; -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }}
        table {{ border-collapse: collapse; mso-table-lspace: 0pt; mso-table-rspace: 0pt; }}
        img {{ -ms-interpolation-mode: bicubic; border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; }}
        @media only screen and (max-width: 620px) {{
            .email-container {{ width: 100% !important; max-width: 100% !important; }}
            .stack-column {{ display: block !important; width: 100% !important; }}
        }}
    </style>
</head>
<body style="margin: 0; padding: 0; background-color: {DEEP_NAVY}; font-family: {FONT_BODY};">
    {preheader}
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: {DEEP_NAVY};">
        <tr>
            <td align="center" style="padding: 12px 8px;">
                <table role="presentation" class="email-container" width="600" cellpadding="0" cellspacing="0" border="0"
                       style="max-width: 600px; width: 100%; background-color: {DARK_NAVY}; border-radius: 12px;
                              border: 1px solid {SURFACE_LIGHT};">
                    <tr>
                        <td>
                            {header}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 24px;">
                            {content}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 24px;">
                            {footer}
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""


def _escape(text: str) -> str:
    """Escape HTML special characters in user-provided content."""
    if not text:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )
