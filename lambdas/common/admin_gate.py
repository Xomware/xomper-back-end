"""
Admin gate
==========
Helper for admin-only API endpoints. Resolves the calling user from
the request and verifies their `is_admin` flag on
`whitelisted_users`. The base authorizer already enforces JWT
validity — this layer adds role enforcement on top.

Identification: requests must carry a `sleeper_user_id` (preferred)
or `email` either as a query string param, a request header
(`X-Sleeper-User-Id` / `X-User-Email`), or in the parsed body.
"""
from __future__ import annotations

from typing import Any, Optional

from lambdas.common.errors import ValidationError
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import (
    get_whitelisted_user_by_sleeper_id,
    get_whitelisted_user_by_email,
)

log = get_logger(__file__)


class NotAdmin(Exception):
    """Raised when an admin endpoint is hit by a non-admin user."""


def resolve_caller_identity(event: dict[str, Any], body: dict[str, Any] | None = None) -> tuple[Optional[str], Optional[str]]:
    """Extract the caller's `(sleeper_user_id, email)` from any of:
    query string, headers, or parsed body. Returns (None, None) if
    neither identifier was supplied."""
    qs = event.get("queryStringParameters") or {}
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    body = body or {}

    sleeper_id = (
        qs.get("sleeper_user_id")
        or headers.get("x-sleeper-user-id")
        or body.get("sleeper_user_id")
    )
    email = (
        qs.get("email")
        or headers.get("x-user-email")
        or body.get("email")
    )
    return sleeper_id, email


def require_admin(event: dict[str, Any], body: dict[str, Any] | None = None) -> dict[str, Any]:
    """Resolve the caller's whitelisted_users row and confirm
    `is_admin == true`. Returns the row on success.

    Raises:
    - ValidationError when no identifier was supplied.
    - NotAdmin when the user exists but isn't an admin.
    - NotAdmin when the user can't be found at all (treated as not
      authorized rather than 404 to avoid leaking which sleeper_ids
      are whitelisted).
    """
    sleeper_id, email = resolve_caller_identity(event, body)

    if not sleeper_id and not email:
        raise ValidationError(
            message="Admin endpoints require sleeper_user_id or email identifier.",
            handler="admin_gate",
            function="require_admin",
            field="sleeper_user_id",
        )

    user: dict[str, Any] | None = None
    if sleeper_id:
        user = get_whitelisted_user_by_sleeper_id(sleeper_id)
    if not user and email:
        user = get_whitelisted_user_by_email(email)

    if not user:
        log.warning(f"admin_gate: caller not found (sleeper_id={sleeper_id}, email={email})")
        raise NotAdmin("not authorized")

    if not user.get("is_admin"):
        log.warning(f"admin_gate: non-admin attempted access (user_id={user.get('id')})")
        raise NotAdmin("not authorized")

    return user


def is_admin(event: dict[str, Any], body: dict[str, Any] | None = None) -> bool:
    """Non-raising sibling of `require_admin`.

    Returns True when the resolved caller exists in `whitelisted_users`
    AND has `is_admin == true`. Returns False in every other case —
    missing identifier, unknown user, non-admin user, or any lookup
    error. Used by read-path endpoints
    (`api_ai_reports_latest`, `api_ai_reports_list`) to branch the
    redact filter without polluting the happy-path response with
    admin-gate exceptions: non-admin callers are still served, just
    with redacted rows filtered out.
    """
    try:
        sleeper_id, email = resolve_caller_identity(event, body)
    except Exception as err:  # noqa: BLE001 — read paths never fail on identity
        log.warning(f"admin_gate.is_admin: identity resolution failed: {err}")
        return False

    if not sleeper_id and not email:
        return False

    try:
        user: dict[str, Any] | None = None
        if sleeper_id:
            user = get_whitelisted_user_by_sleeper_id(sleeper_id)
        if not user and email:
            user = get_whitelisted_user_by_email(email)
    except Exception as err:  # noqa: BLE001 — read paths never fail on lookup
        log.warning(f"admin_gate.is_admin: lookup failed: {err}")
        return False

    if not user:
        return False
    return bool(user.get("is_admin"))
