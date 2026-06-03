"""
XOMPER SES Helper
=================
Email sending via AWS SES with validation and PII masking.
"""

import re
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

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
    template: Optional[str] = None,
) -> dict[str, Any]:
    """
    Send an email via AWS SES.

    Args:
        to_email: Recipient email address
        subject: Email subject
        html_body: HTML body content
        text_body: Plain text body content
        tags: Optional SES message tags
        template: Optional semantic template name (e.g. "ai_review_test")
            persisted on the notification_log row so the admin activity
            feed + receipts surface can distinguish test sends from
            production broadcasts. Existing callers that omit this kwarg
            continue to write rows without a `template` attribute, so
            no back-compat is broken.

    Returns:
        Dict with `success: bool`, `message_id: Optional[str]`,
        `error: Optional[str]`. Truthy on success — callers that only
        care about success/failure can still treat the dict as truthy
        (an empty dict from older paths would be falsy, but we always
        return a populated dict here).

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

    # Lazy import — keeps cold-start light when notification_log
    # isn't reachable (e.g. in unit tests that monkey-patch SES).
    from lambdas.common.notification_log import log_email

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
        message_id = response.get('MessageId')
        log.info(f"Email sent to {masked}, MessageId: {message_id}")
        log_email(
            recipient=to_email,
            subject=subject,
            success=True,
            body_snippet=text_body,
            template=template,
        )
        # Persist full payload to Supabase email_archive so admin can
        # view + resend later. Best-effort — failures here MUST NOT
        # break the success path of the email send.
        _archive_email(
            to_email=to_email,
            subject=subject,
            html_body=html_body,
            text_body=text_body,
            message_id=message_id,
            template=template,
        )
        return {"success": True, "message_id": message_id, "error": None}
    except ClientError as err:
        error = err.response['Error']
        log.error(f"SES error sending to {masked}: {error['Code']} - {error['Message']}")
        log_email(
            recipient=to_email,
            subject=subject,
            success=False,
            error=f"{error['Code']}: {error['Message']}",
            template=template,
        )
        return {
            "success": False,
            "message_id": None,
            "error": f"{error['Code']}: {error['Message']}",
        }
    except Exception as err:
        log.error(f"Error sending email to {masked}: {err}")
        log_email(
            recipient=to_email,
            subject=subject,
            success=False,
            error=str(err),
            template=template,
        )
        return {"success": False, "message_id": None, "error": str(err)}


def send_emails_concurrently(
    email_tasks: list[tuple[str, str, str, str]],
    template: Optional[str] = None,
) -> tuple[int, int]:
    """
    Send multiple emails concurrently using a thread pool.

    Uses ThreadPoolExecutor instead of asyncio.run() to avoid event loop
    conflicts inside Lambda (which may already have a running loop).

    Args:
        email_tasks: List of (to_email, subject, html_body, text_body) tuples
        template: Optional semantic template name applied to every send
            in this batch (forwarded to `send_email` → `log_email`).

    Returns:
        Tuple of (successes, failures)
    """
    if not email_tasks:
        return 0, 0

    def _run(task: tuple[str, str, str, str]) -> dict[str, Any]:
        return send_email(*task, template=template)

    with ThreadPoolExecutor(max_workers=min(len(email_tasks), 10)) as executor:
        futures = [executor.submit(_run, task) for task in email_tasks]
        results = [f.result() for f in futures]

    successes = sum(1 for r in results if r.get("success"))
    return successes, len(results) - successes


def _archive_email(
    *,
    to_email: str,
    subject: str,
    html_body: str,
    text_body: str,
    message_id: Optional[str],
    template: Optional[str],
) -> None:
    """Write a row to Supabase `email_archive`. Failures are caught +
    logged but never re-raised — archiving is best-effort, the send
    path must not fail because Supabase is unreachable.

    Called from `send_email` after every successful SES delivery.
    Powers the admin Email Archive screen (view + resend).
    """
    try:
        # Lazy import + lazy URL fetch keeps cold-start light and lets
        # the Supabase helper layer remain optional for unit tests.
        from lambdas.common.supabase_helper import _post  # type: ignore[attr-defined]
        _post(
            "email_archive",
            {
                "subject": subject,
                "recipient_email": to_email,
                "html_body": html_body,
                "text_body": text_body,
                "message_id": message_id,
                "template": template,
            },
            "Archive email send",
        )
    except Exception as err:
        # Don't propagate — archiving is non-critical.
        log.warning(f"email_archive write failed (non-fatal): {err}")
