"""
POST /email/taxi - Send Taxi Squad Steal Emails
Sends league-wide notification + targeted owner notification.

Expected body:
{
    "stealer": { "display_name": "Dom" },
    "player": { "first_name": "John", "last_name": "Johnson", "position": "RB", "team": "NYG" },
    "owner": { "display_name": "Steve", "email": "steve@example.com" },
    "recipients": ["email1@...", "email2@..."],
    "league_name": "The Dynasty League"
}
"""
from lambdas.common.logger import get_logger
from lambdas.common.errors import handle_errors
from lambdas.common.utility_helpers import success_response, parse_body, require_fields
from lambdas.common.ses_helper import send_emails_concurrently
from lambdas.common.constants import XOMPER_URL
from lambdas.common.email_templates import (
    generate_taxi_steal_league_email,
    generate_taxi_steal_league_email_plain_text,
    generate_taxi_steal_owner_email,
    generate_taxi_steal_owner_email_plain_text,
)
from lambdas.common.sns_helper import send_push_to_users
from lambdas.common.push_templates import taxi_steal_push

log = get_logger(__file__)

HANDLER = 'email_taxi'


@handle_errors(HANDLER)
def handler(event, context):
    log.info("Starting Send Taxi Squad Email...")
    body = parse_body(event)
    require_fields(body, 'stealer', 'player', 'owner', 'recipients', 'league_name')

    stealer = body['stealer']
    player = body['player']
    owner = body['owner']
    recipients = body['recipients']
    league_name = body.get('league_name', '')

    stealer_name = stealer.get('display_name', 'A league member')
    player_name = f"{player.get('first_name', '')} {player.get('last_name', '')}".strip() or 'Unknown Player'
    player_position = player.get('position', 'N/A')
    player_team = player.get('team', 'N/A')
    player_image_url = player.get('player_image_url', '')
    team_logo_url = player.get('team_logo_url', '')
    pick_cost = player.get('pick_cost', '')
    owner_name = owner.get('display_name', 'Unknown')
    owner_email = owner.get('email')

    log.info(f"{stealer_name} stealing {player_name} from {owner_name}. Notifying {len(recipients)} members.")

    # Generate league-wide template
    league_subject = f"Taxi Squad Alert: {stealer_name} is stealing {player_name}!"
    league_html = generate_taxi_steal_league_email(
        stealer_name=stealer_name,
        player_name=player_name,
        player_position=player_position,
        player_team=player_team,
        target_owner_name=owner_name,
        league_url=XOMPER_URL,
        league_name=league_name,
        player_image_url=player_image_url,
        team_logo_url=team_logo_url,
        pick_cost=pick_cost,
    )
    league_text = generate_taxi_steal_league_email_plain_text(
        stealer_name=stealer_name,
        player_name=player_name,
        player_position=player_position,
        player_team=player_team,
        target_owner_name=owner_name,
        league_url=XOMPER_URL,
        league_name=league_name,
        pick_cost=pick_cost,
    )

    # Build all email tasks
    tasks = [(email, league_subject, league_html, league_text) for email in recipients]

    # Add targeted owner notification
    if owner_email:
        owner_subject = f"URGENT: {stealer_name} is stealing {player_name} from your taxi squad!"
        owner_html = generate_taxi_steal_owner_email(
            stealer_name=stealer_name,
            player_name=player_name,
            player_position=player_position,
            player_team=player_team,
            owner_name=owner_name,
            league_url=XOMPER_URL,
            league_name=league_name,
            player_image_url=player_image_url,
            team_logo_url=team_logo_url,
            pick_cost=pick_cost,
        )
        owner_text = generate_taxi_steal_owner_email_plain_text(
            stealer_name=stealer_name,
            player_name=player_name,
            player_position=player_position,
            player_team=player_team,
            owner_name=owner_name,
            league_url=XOMPER_URL,
            league_name=league_name,
            pick_cost=pick_cost,
        )
        tasks.append((owner_email, owner_subject, owner_html, owner_text))

    # Send all emails concurrently
    successes, failures = send_emails_concurrently(tasks)
    log.info(f"Taxi emails complete: {successes} sent, {failures} failed")

    # Send push notifications (fire-and-forget)
    user_ids = body.get("user_ids", [])
    if user_ids:
        try:
            title, push_body, category, data = taxi_steal_push(stealer_name, player_name)
            send_push_to_users(user_ids, title, push_body, category, data)
        except Exception as err:
            log.error(f"Push notification error (non-blocking): {err}")

    return success_response({
        "successfulEmails": successes,
        "failedEmails": failures
    }, is_api=False)
