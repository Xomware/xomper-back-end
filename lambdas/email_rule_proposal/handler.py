"""
POST /email/rule-proposal - Send Rule Proposal Email
Notifies all league members about a new rule proposal.

Expected body:
{
    "proposal": {
        "title": "Allow IR stashing",
        "description": "Players on IR can be stashed...",
        "proposed_by_username": "Dom",
        "league_name": "The Dynasty League"
    },
    "recipients": ["email1@...", "email2@..."]
}
"""
from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response, parse_body, require_fields
from lambdas.common.ses_helper import send_emails_concurrently
from lambdas.common.constants import XOMPER_URL
from lambdas.common.email_templates import (
    generate_rule_proposed_email,
    generate_rule_proposed_email_plain_text,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.push_templates import rule_proposed_push

log = get_logger(__file__)

HANDLER = 'email_rule_proposal'


@handle_errors(HANDLER)
def handler(event, context):
    log.info("Starting Send Rule Proposal Email...")
    body = parse_body(event)
    require_fields(body, 'proposal', 'recipients')

    proposal = body['proposal']
    recipients = body['recipients']

    proposer_name = proposal.get('proposed_by_username', 'A league member')
    rule_title = proposal.get('title', 'Untitled Rule')
    rule_description = proposal.get('description', '')
    league_name = proposal.get('league_name', '')

    log.info(f"{proposer_name} proposing: {rule_title}. Notifying {len(recipients)} members.")

    subject = f"New Rule Proposal: {rule_title}"
    html_body = generate_rule_proposed_email(
        proposer_name=proposer_name,
        rule_title=rule_title,
        rule_description=rule_description,
        vote_url=XOMPER_URL,
        league_name=league_name,
    )
    text_body = generate_rule_proposed_email_plain_text(
        proposer_name=proposer_name,
        rule_title=rule_title,
        rule_description=rule_description,
        vote_url=XOMPER_URL,
        league_name=league_name,
    )

    tasks = [(email, subject, html_body, text_body) for email in recipients]
    successes, failures = send_emails_concurrently(tasks)
    log.info(f"Rule proposal emails complete: {successes} sent, {failures} failed")

    # Send push notifications (fire-and-forget)
    user_ids = body.get("user_ids", [])
    if user_ids:
        try:
            title, push_body, category, data = rule_proposed_push(proposer_name, rule_title)
            send_push_to_users(user_ids, title, push_body, category, data)
        except Exception as err:
            log.error(f"Push notification error (non-blocking): {err}")

    return success_response({
        "successfulEmails": successes,
        "failedEmails": failures
    }, is_api=False)
