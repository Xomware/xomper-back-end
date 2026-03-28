"""
XOMPER SES Helper
=================
Email sending via AWS SES with validation and PII masking.
"""

import re
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from lambdas.common.constants import FROM_EMAIL, AWS_DEFAULT_REGION
from lambdas.common.errors import ValidationError, EmailError
from lambdas.common.logger import get_logger

log = get_logger(__file__)

ses_client = boto3.client('ses', region_name=AWS_DEFAULT_REGION)

# Basic email regex -- intentionally permissive; SES does final validation
_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


def mask_email(email: str) -> str:
    """Mask an email address for safe logging. e.g. d***@example.com"""
    if not email or '@' not in email:
        return '***'
    local, domain = email.rsplit('@', 1)
    masked_local = local[0] + '***' if local else '***'
    return f"{masked_local}@{domain}"


def validate_email(email: str) -> bool:
    """Return True if the email looks structurally valid."""
    if not email or not isinstance(email, str):
        return False
    return bool(_EMAIL_RE.match(email.strip()))


def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    text_body: str,
    tags: Optional[list[dict[str, str]]] = None,
) -> bool:
    """
    Send an email via AWS SES.

    Args:
        to_email: Recipient email address
        subject: Email subject
        html_body: HTML body content
        text_body: Plain text body content
        tags: Optional SES message tags

    Returns:
        True on success, False on failure

    Raises:
        ValidationError: If to_email is not a valid email address
    """
    if not validate_email(to_email):
        raise ValidationError(
            message=f"Invalid email address: {mask_email(to_email)}",
            handler="ses_helper",
            function="send_email",
            field="to_email",
        )

    masked = mask_email(to_email)
    try:
        response = ses_client.send_email(
            Source=FROM_EMAIL,
            Destination={'ToAddresses': [to_email]},
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {
                    'Text': {'Data': text_body, 'Charset': 'UTF-8'},
                    'Html': {'Data': html_body, 'Charset': 'UTF-8'},
                },
            },
            Tags=tags or [],
        )
        log.info(f"Email sent to {masked}, MessageId: {response.get('MessageId')}")
        return True
    except ClientError as err:
        error = err.response['Error']
        log.error(f"SES error sending to {masked}: {error['Code']} - {error['Message']}")
        return False
    except Exception as err:
        log.error(f"Error sending email to {masked}: {err}")
        return False


def send_emails_concurrently(email_tasks: list[tuple[str, str, str, str]]) -> tuple[int, int]:
    """
    Send multiple emails concurrently using a thread pool.

    Uses ThreadPoolExecutor instead of asyncio.run() to avoid event loop
    conflicts inside Lambda (which may already have a running loop).

    Args:
        email_tasks: List of (to_email, subject, html_body, text_body) tuples

    Returns:
        Tuple of (successes, failures)
    """
    if not email_tasks:
        return 0, 0

    with ThreadPoolExecutor(max_workers=min(len(email_tasks), 10)) as executor:
        futures = [executor.submit(send_email, *task) for task in email_tasks]
        results = [f.result() for f in futures]

    successes = sum(1 for r in results if r)
    return successes, len(results) - successes
