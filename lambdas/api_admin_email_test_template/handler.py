"""
POST /admin/email-test-template
===============================
Admin-only endpoint that fires a SAMPLE rendering of any non-AI-Review
email template against a chosen recipient. Lets the admin preview the
visual layout + copy of every transactional + cron email before season
flips and the real fires start.

Why fixture data instead of pulling from Sleeper/Supabase:
- The 4 notif cron lambdas (weekly_recap, lineup_not_set, ...)
  gate on NFL `season_type == regular | post`. We're in the offseason,
  so the real data path returns empty.
- We're testing the email's TEMPLATE — what it looks like, the copy,
  the layout. Fixture data is sufficient + reproducible.

Body:
{
    "kind":                  "weekly_recap" | "lineup_not_set" |
                             "rule_proposed" | "rule_accepted" | "rule_denied" |
                             "taxi_steal_league" | "taxi_steal_owner",
    "recipient_sleeper_user_id": "...",   // required
    "email":                 "...",       // optional admin identity override
    "sleeper_user_id":       "...",       // optional admin identity override
}

Response:
{
    "Success": true,
    "kind": "weekly_recap",
    "recipient_email": "user@example.com",
    "message_id": "ses-mid-...",
    "sent_at": "2026-06-02T15:42:33Z"
}
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.audit_log import write_audit
from lambdas.common.email_templates import (
    generate_weekly_recap_email,
    generate_weekly_recap_email_plain_text,
    generate_lineup_not_set_email,
    generate_lineup_not_set_email_plain_text,
    generate_rule_proposed_email,
    generate_rule_proposed_email_plain_text,
    generate_rule_accepted_email,
    generate_rule_accepted_email_plain_text,
    generate_rule_denied_email,
    generate_rule_denied_email_plain_text,
    generate_taxi_steal_league_email,
    generate_taxi_steal_league_email_plain_text,
    generate_taxi_steal_owner_email,
    generate_taxi_steal_owner_email_plain_text,
)
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.ses_helper import send_email
from lambdas.common.supabase_helper import get_whitelisted_user_by_sleeper_id
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_email_test_template"

_LEAGUE_NAME = "CLT DYNASTY"

# Fixture data per template kind. Keeps the test send deterministic
# and offseason-safe.
_FIXTURES: dict[str, dict[str, Any]] = {
    "weekly_recap": {
        "subject": f"[TEST] Week 14 {_LEAGUE_NAME} recap",
        "params": {
            "manager_name": "Dom",
            "league_name": _LEAGUE_NAME,
            # Week 14 = first playoff week — exercises both standings
            # AND bracket sections in the sample.
            "week": 14,
            "user_team_name": "Nvr 4get Da CLT",
            "user_points": 124.5,
            "opponent_team_name": "ktatich",
            "opponent_points": 102.3,
            "user_won": True,
            "is_tie": False,
            "league_high_team": "Gangsters of Love",
            "league_high_points": 162.6,
            "all_scores": [
                ("Gangsters of Love", 162.6),
                ("Nvr 4get Da CLT", 124.5),
                ("The Goffather", 113.0),
                ("Wake Forest Factory", 111.5),
                ("Brock Party", 105.9),
                ("ktatich", 102.3),
                ("Shits and Gibbles", 95.4),
                ("Sinnott Committee", 90.2),
                ("Shane Beamer's Burner", 89.6),
                ("gniadek", 88.7),
                ("reesegriffin", 81.0),
                ("Nico Suave", 72.1),
            ],
            # Cumulative standings — (team, wins, losses, ties, points_for).
            "standings": [
                ("Gangsters of Love",      11, 3, 0, 1763.4),
                ("Nvr 4get Da CLT",        10, 4, 0, 1702.1),
                ("ktatich",                 9, 5, 0, 1690.5),
                ("The Goffather",           9, 5, 0, 1652.3),
                ("Wake Forest Factory",     8, 6, 0, 1641.0),
                ("Brock Party",             7, 7, 0, 1598.8),
                ("Shits and Gibbles",       6, 8, 0, 1554.2),
                ("Sinnott Committee",       6, 8, 0, 1521.7),
                ("Shane Beamer's Burner",   5, 9, 0, 1502.0),
                ("gniadek",                 4, 10, 0, 1480.4),
                ("reesegriffin",            3, 11, 0, 1421.6),
                ("Nico Suave",              2, 12, 0, 1380.9),
            ],
            # Playoff bracket — list of (round_label, [(team_a, score_a,
            # team_b, score_b)]). None scores mean upcoming.
            "bracket_rounds": [
                ("Quarterfinals", [
                    ("Gangsters of Love",   162.6, "Nico Suave",              72.1),
                    ("Nvr 4get Da CLT",     124.5, "ktatich",                 102.3),
                    ("The Goffather",       113.0, "Wake Forest Factory",     111.5),
                    ("Brock Party",         105.9, "Shane Beamer's Burner",   89.6),
                ]),
                ("Semifinals", [
                    ("Gangsters of Love",   None,  "Brock Party",             None),
                    ("Nvr 4get Da CLT",     None,  "The Goffather",           None),
                ]),
                ("Championship", [
                    ("TBD",                 None,  "TBD",                     None),
                ]),
            ],
            # Personal World Cup snapshot for the recipient.
            "wc_personal": {
                "division": "East Division",
                "position": 2,
                "status": "clinched",
                "wins": 7,
                "losses": 4,
                "ties": 0,
                "points_for": 1284.3,
                "points_back": None,
            },
            # Full WC standings by division, top-2 clinched.
            "wc_divisions": [
                ("East Division", [
                    {"team_name": "Gangsters of Love",   "wins": 9, "losses": 2, "ties": 0, "points_for": 1422.0, "status": "clinched", "is_user": False},
                    {"team_name": "Nvr 4get Da CLT",     "wins": 7, "losses": 4, "ties": 0, "points_for": 1284.3, "status": "clinched", "is_user": True},
                    {"team_name": "Shits and Gibbles",   "wins": 4, "losses": 7, "ties": 0, "points_for": 1188.5, "status": "eliminated", "is_user": False},
                ]),
                ("West Division", [
                    {"team_name": "ktatich",             "wins": 8, "losses": 3, "ties": 0, "points_for": 1351.2, "status": "clinched", "is_user": False},
                    {"team_name": "The Goffather",       "wins": 7, "losses": 4, "ties": 0, "points_for": 1297.8, "status": "alive",    "is_user": False},
                    {"team_name": "reesegriffin",        "wins": 2, "losses": 9, "ties": 0, "points_for": 1098.6, "status": "eliminated", "is_user": False},
                ]),
                ("North Division", [
                    {"team_name": "Wake Forest Factory", "wins": 7, "losses": 4, "ties": 0, "points_for": 1306.4, "status": "clinched", "is_user": False},
                    {"team_name": "Brock Party",         "wins": 6, "losses": 5, "ties": 0, "points_for": 1241.9, "status": "alive",    "is_user": False},
                    {"team_name": "gniadek",             "wins": 3, "losses": 8, "ties": 0, "points_for": 1142.7, "status": "alive",    "is_user": False},
                ]),
                ("South Division", [
                    {"team_name": "Sinnott Committee",   "wins": 6, "losses": 5, "ties": 0, "points_for": 1230.5, "status": "alive",    "is_user": False},
                    {"team_name": "Shane Beamer's Burner","wins": 5, "losses": 6, "ties": 0, "points_for": 1198.0, "status": "alive",    "is_user": False},
                    {"team_name": "Nico Suave",          "wins": 1, "losses": 10, "ties": 0, "points_for": 1054.1, "status": "eliminated", "is_user": False},
                ]),
            ],
        },
    },
    "lineup_not_set": {
        "subject": f"[TEST] Set your Week 1 lineup — {_LEAGUE_NAME}",
        "params": {
            "manager_name": "Dom",
            "league_name": _LEAGUE_NAME,
            "issue_count": 2,
            "week": 1,
        },
    },
    "rule_proposed": {
        "subject": f"[TEST] New rule proposal — {_LEAGUE_NAME}",
        "params": {
            "proposer_name": "Kyle",
            "rule_title": "Allow IR stashing for OUT designation",
            "rule_description": (
                "Players designated OUT for the week should be eligible "
                "for the IR slot, even if they're not on the official "
                "NFL IR list. Frees up bench space for high-injury weeks."
            ),
            "league_name": _LEAGUE_NAME,
        },
    },
    "rule_accepted": {
        "subject": f"[TEST] Rule passed — {_LEAGUE_NAME}",
        "params": {
            "proposer_name": "Kyle",
            "rule_title": "Allow IR stashing for OUT designation",
            "rule_description": (
                "Players designated OUT for the week should be eligible "
                "for the IR slot."
            ),
            "approved_voters": ["Dom", "Reese", "Connor", "Luke", "Mike", "Alex"],
            "rejected_voters": ["Jim", "Tony"],
            "league_name": _LEAGUE_NAME,
        },
    },
    "rule_denied": {
        "subject": f"[TEST] Rule rejected — {_LEAGUE_NAME}",
        "params": {
            "proposer_name": "Kyle",
            "rule_title": "Trade deadline week 12",
            "rule_description": (
                "Move the trade deadline up from Week 13 to Week 12 "
                "so playoff seeding isn't decided by last-minute trades."
            ),
            "approved_voters": ["Dom", "Kyle"],
            "rejected_voters": ["Jim", "Tony", "Reese", "Connor", "Luke", "Mike", "Alex", "Tibor"],
            "league_name": _LEAGUE_NAME,
        },
    },
    "taxi_steal_league": {
        "subject": f"[TEST] Taxi squad stolen — {_LEAGUE_NAME}",
        "params": {
            "stealer_name": "Kyle",
            "player_name": "Caleb Williams",
            "player_position": "QB",
            "player_team": "CHI",
            "target_owner_name": "Dom",
            "league_name": _LEAGUE_NAME,
            "pick_cost": "2027 3rd-round pick",
        },
    },
    "taxi_steal_owner": {
        "subject": f"[TEST] Your taxi player was stolen — {_LEAGUE_NAME}",
        "params": {
            "stealer_name": "Kyle",
            "player_name": "Caleb Williams",
            "player_position": "QB",
            "player_team": "CHI",
            "owner_name": "Dom",
            "compensation_table": [
                {"label": "Compensation pick", "value": "2027 3rd-round"},
            ],
            "league_name": _LEAGUE_NAME,
            "pick_cost": "2027 3rd-round pick",
        },
    },
}

# Wires each kind to (html builder, text builder). Listed inline so
# the dispatch table stays in one place.
_BUILDERS = {
    "weekly_recap":      (generate_weekly_recap_email,      generate_weekly_recap_email_plain_text),
    "lineup_not_set":    (generate_lineup_not_set_email,    generate_lineup_not_set_email_plain_text),
    "rule_proposed":     (generate_rule_proposed_email,     generate_rule_proposed_email_plain_text),
    "rule_accepted":     (generate_rule_accepted_email,     generate_rule_accepted_email_plain_text),
    "rule_denied":       (generate_rule_denied_email,       generate_rule_denied_email_plain_text),
    "taxi_steal_league": (generate_taxi_steal_league_email, generate_taxi_steal_league_email_plain_text),
    "taxi_steal_owner":  (generate_taxi_steal_owner_email,  generate_taxi_steal_owner_email_plain_text),
}


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin email-test-template request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    kind = (body.get("kind") or "").strip()
    if kind not in _BUILDERS:
        return success_response(
            {
                "Success": False,
                "Message": f"Unknown kind '{kind}'. Must be one of {sorted(_BUILDERS.keys())}.",
            },
            status_code=400,
        )

    recipient_sleeper_id = (body.get("recipient_sleeper_user_id") or "").strip()
    if not recipient_sleeper_id:
        return success_response(
            {"Success": False, "Message": "recipient_sleeper_user_id is required"},
            status_code=400,
        )

    recipient = get_whitelisted_user_by_sleeper_id(recipient_sleeper_id)
    if not recipient:
        return success_response(
            {"Success": False, "Message": "Recipient not found in whitelisted_users"},
            status_code=404,
        )

    recipient_email = recipient.get("email")
    if not recipient_email or "@" not in recipient_email:
        return success_response(
            {
                "Success": False,
                "Message": f"Recipient has no usable email ('{recipient_email}')",
            },
            status_code=400,
        )

    fixture = _FIXTURES[kind]
    html_builder, text_builder = _BUILDERS[kind]

    # Personalize the manager_name field on fixtures that have one so
    # the recipient sees their own name where the template would
    # interpolate it in production.
    params = dict(fixture["params"])
    recipient_display = (
        recipient.get("display_name")
        or recipient.get("sleeper_username")
        or "Manager"
    )
    for personal_key in ("manager_name", "owner_name", "target_owner_name"):
        if personal_key in params:
            params[personal_key] = recipient_display

    html = html_builder(**params)
    text = text_builder(**params)
    subject = fixture["subject"]

    ses_result = send_email(
        to_email=recipient_email,
        subject=subject,
        html_body=html,
        text_body=text,
        template=f"{kind}_test",
    )
    if not ses_result.get("success"):
        return success_response(
            {
                "Success": False,
                "Message": f"SES failure: {ses_result.get('error') or 'unknown'}",
            },
            status_code=500,
        )

    message_id = ses_result.get("message_id") or ""
    sent_at = datetime.now(timezone.utc).isoformat()

    write_audit(
        actor_user_id=admin_user.get("sleeper_user_id") or admin_user.get("email") or "",
        action="email.test_template",
        target_table="ses",
        target_id=recipient_sleeper_id,
        before=None,
        after={
            "kind": kind,
            "recipient_email": recipient_email,
            "subject": subject,
            "message_id": message_id,
        },
    )

    return success_response(
        {
            "Success": True,
            "kind": kind,
            "recipient_email": recipient_email,
            "recipient_user_id": recipient_sleeper_id,
            "message_id": message_id,
            "sent_at": sent_at,
            "subject": subject,
        }
    )
