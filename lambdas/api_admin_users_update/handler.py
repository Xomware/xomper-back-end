"""
POST /admin/users-update
========================
Admin-only endpoint that PATCHes a single row in `whitelisted_users`
with a strict per-field allowlist + per-field validation. Writes one
row into `admin_audit` per successful update.

Body:
{
    "sleeper_user_id": "abc123",       // required — admin gate identity
    "email":           "user@.. ",     // optional fallback identity
    "user_id":         "u7",           // required — sleeper_user_id of the user being edited
    "fields": {
        "email":        "new@example.com",   // optional, email-regex validated
        "display_name": "New Name",          // optional, non-empty trimmed string
        "is_admin":     true,                // optional, bool (accepts "true"/"false")
        "is_active":    false                // optional, bool (accepts "true"/"false")
    }
}

Behavior:
- 200 + Success=true on a successful update. Response carries the FULL
  updated `whitelisted_users` row + the `before` snapshot + the audit
  row id (when the audit write succeeded).
- 400 on bad input (missing `user_id`, missing/empty `fields`, unknown
  field key, invalid email, non-bool value).
- 403 when the caller is not an admin.
- 404 when the target user is not in `whitelisted_users`.
- 500 on Supabase / unexpected failure.
"""
from __future__ import annotations

import re
from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.audit_log import write_audit
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import get_row, update_row
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_users_update"

# Single source of truth: any whitelisted_users column we expose for
# editing must be added here AND mirrored in the iOS UserEditView form.
_ALLOWED_FIELDS: tuple[str, ...] = (
    "email",
    "display_name",
    "is_admin",
    "is_active",
)

# Simplified RFC 5322 — matches what the F1 TestEmailView regex accepts.
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin users-update request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    user_id = (body.get("user_id") or "").strip()
    if not user_id:
        return success_response(
            {"Success": False, "Message": "Missing 'user_id' in body"},
            status_code=400,
        )

    raw_fields = body.get("fields")
    if not isinstance(raw_fields, dict) or not raw_fields:
        return success_response(
            {
                "Success": False,
                "Message": "Missing or empty 'fields' object in body",
            },
            status_code=400,
        )

    # Validate + normalize each field. Unknown keys reject the whole
    # request so admins can't silently miss a typo.
    try:
        normalized = _validate_and_normalize(raw_fields)
    except ValueError as err:
        return success_response(
            {"Success": False, "Message": str(err)},
            status_code=400,
        )

    # Load the existing row so we can compute the `before` diff and
    # 404 cleanly when the user doesn't exist.
    existing = get_row("whitelisted_users", "sleeper_user_id", user_id)
    if not existing:
        return success_response(
            {
                "Success": False,
                "Message": f"User not found: {user_id}",
            },
            status_code=404,
        )

    # Build a `before` dict containing only the keys that are changing,
    # keyed off the existing row's values. This is what lands in the
    # audit row's `before` JSONB column.
    before: dict[str, Any] = {key: existing.get(key) for key in normalized.keys()}

    updated = update_row(
        "whitelisted_users",
        "sleeper_user_id",
        user_id,
        normalized,
    )

    # Best-effort audit write. Audit failures NEVER fail the parent
    # action — see audit_log.write_audit docstring.
    actor = admin_user.get("sleeper_user_id") or admin_user.get("id") or "unknown"
    audit_row = write_audit(
        actor_user_id=str(actor),
        action="users.update",
        target_table="whitelisted_users",
        target_id=user_id,
        before=before,
        after=normalized,
    )
    audit_id = (audit_row or {}).get("id") if audit_row else None

    return success_response(
        {
            "Success": True,
            "user_id": user_id,
            "user": updated or existing,
            "before": before,
            "after": normalized,
            "audit_id": audit_id,
        }
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_and_normalize(raw_fields: dict[str, Any]) -> dict[str, Any]:
    """Reject unknown keys + validate each known key's value. Returns
    the dict of normalized values ready to PATCH onto the row."""
    unknown = [k for k in raw_fields.keys() if k not in _ALLOWED_FIELDS]
    if unknown:
        raise ValueError(
            f"Unknown field '{unknown[0]}'. "
            f"Allowed fields: {list(_ALLOWED_FIELDS)}"
        )

    normalized: dict[str, Any] = {}
    for key, value in raw_fields.items():
        if key == "email":
            normalized[key] = _validate_email(value)
        elif key == "display_name":
            normalized[key] = _validate_non_empty_str(value, "display_name")
        elif key in ("is_admin", "is_active"):
            normalized[key] = _coerce_bool(value, key)
    return normalized


def _validate_email(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("Field 'email' must be a string")
    trimmed = value.strip()
    if not _EMAIL_RE.match(trimmed):
        raise ValueError(f"Invalid email: '{trimmed}'")
    return trimmed


def _validate_non_empty_str(value: Any, key: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Field '{key}' must be a string")
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"Field '{key}' must be non-empty")
    return trimmed


def _coerce_bool(value: Any, key: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    raise ValueError(
        f"Field '{key}' must be a boolean (true/false). Got: {value!r}"
    )
