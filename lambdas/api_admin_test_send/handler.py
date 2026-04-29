"""
POST /api/admin/test-send
=========================
Fires a sample push + email back to the calling admin so they can
preview formatting + verify that the channels are wired end-to-end
on a real device.

Body:
{
    "sleeper_user_id": "abc123",     // required — admin gate identity
    "email": "user@example.com",     // optional fallback identity
    "kind": "lineup_not_set",        // which template to fire
    "channels": ["push", "email"]    // optional, defaults to both
}

Supported `kind` values mirror the production templates:
- lineup_not_set
- weekly_recap
- close_game_alert
- worldcup_clinched / worldcup_eliminated / worldcup_line_moved
- rule_proposed / rule_accepted / rule_denied
- taxi_steal
"""
from typing import Any

from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response, parse_body
from lambdas.common.admin_gate import require_admin, NotAdmin
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.ses_helper import send_emails_concurrently
from lambdas.common.push_templates import (
    lineup_not_set_push,
    weekly_recap_push,
    close_game_push,
    worldcup_clinched_push,
    worldcup_eliminated_push,
    worldcup_line_moved_push,
    rule_proposed_push,
    rule_accepted_push,
    rule_denied_push,
    taxi_steal_push,
)
from lambdas.common.email_templates import (
    generate_lineup_not_set_email,
    generate_lineup_not_set_email_plain_text,
    generate_weekly_recap_email,
    generate_weekly_recap_email_plain_text,
    generate_rule_proposed_email,
    generate_rule_proposed_email_plain_text,
    generate_rule_accepted_email,
    generate_rule_accepted_email_plain_text,
    generate_rule_denied_email,
    generate_rule_denied_email_plain_text,
)

log = get_logger(__file__)
HANDLER = "api_admin_test_send"

# Sample data baked into each template — meant to LOOK like real
# content while being clearly identifiable as a test fire.
SAMPLE_LEAGUE = "CLT DYNASTY"
SAMPLE_RULE_TITLE = "[TEST] Lower taxi-squad cap"
SAMPLE_RULE_DESC = "Drop the taxi cap from 6 to 4. Test fire — no actual proposal."
SAMPLE_USER_TEAM = "Your Team"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin test-send request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    kind = (body.get("kind") or "").strip()
    if not kind:
        return success_response(
            {"Success": False, "Message": "Missing 'kind' in body"},
            status_code=400,
        )

    channels = body.get("channels") or ["push", "email"]
    if not isinstance(channels, list):
        channels = ["push", "email"]
    channels_set = {c.lower() for c in channels}

    sleeper_user_id = admin_user.get("sleeper_user_id") or ""
    admin_email = admin_user.get("email")
    manager_name = (
        admin_user.get("display_name")
        or admin_user.get("sleeper_username")
        or "Admin"
    )

    sent_push = 0
    sent_email = 0

    push_template, email_render = _resolve_template(kind, manager_name=manager_name)
    if push_template is None:
        return success_response(
            {"Success": False, "Message": f"Unknown kind '{kind}'"},
            status_code=400,
        )

    if "push" in channels_set and sleeper_user_id:
        title, push_body, category, data = push_template
        # Tag the title so test sends are obvious in notification center.
        prefixed_title = title if title.startswith("🧪") else f"🧪 {title}"
        successes, _failures = send_push_to_users(
            [sleeper_user_id],
            prefixed_title,
            push_body,
            category,
            data,
        )
        sent_push = successes

    if "email" in channels_set and admin_email and email_render:
        subject, html, text = email_render
        successes, _failures = send_emails_concurrently([(admin_email, subject, html, text)])
        sent_email = successes

    return success_response({
        "kind": kind,
        "push_sent": sent_push,
        "email_sent": sent_email,
        "to_user_id": sleeper_user_id,
        "to_email": admin_email,
    })


def _resolve_template(
    kind: str,
    manager_name: str,
) -> tuple[tuple[str, str, str, dict] | None, tuple[str, str, str] | None]:
    """Returns (push_tuple, email_tuple_or_none).

    Each push tuple is (title, body, category, data).
    Each email tuple is (subject, html, text). Templates that don't
    have an email leg in production also skip email here.
    """
    if kind == "lineup_not_set":
        push = lineup_not_set_push(league_name=SAMPLE_LEAGUE, issue_count=2)
        html = generate_lineup_not_set_email(
            manager_name=manager_name,
            league_name=SAMPLE_LEAGUE,
            issue_count=2,
            week=1,
        )
        text = generate_lineup_not_set_email_plain_text(
            manager_name=manager_name,
            league_name=SAMPLE_LEAGUE,
            issue_count=2,
            week=1,
        )
        return push, (f"[TEST] Set your {SAMPLE_LEAGUE} lineup", html, text)

    if kind == "weekly_recap":
        push = weekly_recap_push(
            week=1,
            user_team_name=SAMPLE_USER_TEAM,
            user_won=True,
            user_pts=128.5,
            opp_team_name="Sample Opponent",
            opp_pts=119.2,
            league_high_team="High Scorer",
            league_high_pts=158.4,
        )  # type: ignore[call-arg]
        sample_scores = [
            ("High Scorer", 158.4),
            (SAMPLE_USER_TEAM, 128.5),
            ("Sample Opponent", 119.2),
        ]
        html = generate_weekly_recap_email(
            manager_name=manager_name,
            league_name=SAMPLE_LEAGUE,
            week=1,
            user_team_name=SAMPLE_USER_TEAM,
            user_points=128.5,
            opponent_team_name="Sample Opponent",
            opponent_points=119.2,
            user_won=True,
            is_tie=False,
            league_high_team="High Scorer",
            league_high_points=158.4,
            all_scores=sample_scores,
        )
        text = generate_weekly_recap_email_plain_text(
            manager_name=manager_name,
            league_name=SAMPLE_LEAGUE,
            week=1,
            user_team_name=SAMPLE_USER_TEAM,
            user_points=128.5,
            opponent_team_name="Sample Opponent",
            opponent_points=119.2,
            user_won=True,
            is_tie=False,
            league_high_team="High Scorer",
            league_high_points=158.4,
            all_scores=sample_scores,
        )
        return push, (f"[TEST] Week 1 {SAMPLE_LEAGUE} recap", html, text)

    if kind == "close_game_alert":
        push = close_game_push(
            user_team_name=SAMPLE_USER_TEAM,
            user_pts=110.4,
            opp_team_name="Sample Opponent",
            opp_pts=109.2,
        )
        return push, None  # close-game is push-only by design

    if kind == "worldcup_clinched":
        push = worldcup_clinched_push("AFC East")
        return push, None

    if kind == "worldcup_eliminated":
        push = worldcup_eliminated_push("AFC East")
        return push, None

    if kind == "worldcup_line_moved":
        push = worldcup_line_moved_push("AFC East", crossed_into=True)
        return push, None

    if kind == "rule_proposed":
        push = rule_proposed_push("Sample Proposer", SAMPLE_RULE_TITLE)
        html = generate_rule_proposed_email(
            proposer_name="Sample Proposer",
            rule_title=SAMPLE_RULE_TITLE,
            rule_description=SAMPLE_RULE_DESC,
            league_name=SAMPLE_LEAGUE,
        )
        text = generate_rule_proposed_email_plain_text(
            proposer_name="Sample Proposer",
            rule_title=SAMPLE_RULE_TITLE,
            rule_description=SAMPLE_RULE_DESC,
            league_name=SAMPLE_LEAGUE,
        )
        return push, (f"[TEST] New Rule Proposal: {SAMPLE_RULE_TITLE}", html, text)

    if kind == "rule_accepted":
        push = rule_accepted_push(SAMPLE_RULE_TITLE)
        sample_voters = ["Voter One", "Voter Two"]
        html = generate_rule_accepted_email(
            proposer_name="Sample Proposer",
            rule_title=SAMPLE_RULE_TITLE,
            rule_description=SAMPLE_RULE_DESC,
            approved_voters=sample_voters,
            rejected_voters=["Voter Three"],
            league_name=SAMPLE_LEAGUE,
        )
        text = generate_rule_accepted_email_plain_text(
            proposer_name="Sample Proposer",
            rule_title=SAMPLE_RULE_TITLE,
            rule_description=SAMPLE_RULE_DESC,
            approved_voters=sample_voters,
            rejected_voters=["Voter Three"],
            league_name=SAMPLE_LEAGUE,
        )
        return push, (f"[TEST] Rule Accepted: {SAMPLE_RULE_TITLE}", html, text)

    if kind == "rule_denied":
        push = rule_denied_push(SAMPLE_RULE_TITLE)
        html = generate_rule_denied_email(
            proposer_name="Sample Proposer",
            rule_title=SAMPLE_RULE_TITLE,
            rule_description=SAMPLE_RULE_DESC,
            approved_voters=["Voter One"],
            rejected_voters=["Voter Two", "Voter Three"],
            league_name=SAMPLE_LEAGUE,
        )
        text = generate_rule_denied_email_plain_text(
            proposer_name="Sample Proposer",
            rule_title=SAMPLE_RULE_TITLE,
            rule_description=SAMPLE_RULE_DESC,
            approved_voters=["Voter One"],
            rejected_voters=["Voter Two", "Voter Three"],
            league_name=SAMPLE_LEAGUE,
        )
        return push, (f"[TEST] Rule Denied: {SAMPLE_RULE_TITLE}", html, text)

    if kind == "taxi_steal":
        push = taxi_steal_push("Sample Stealer", "Sample Player")
        # Taxi templates require additional structured args (player /
        # roster info). Skip the email leg in test-send for taxi —
        # the production handler builds these from request context.
        return push, None

    return None, None
