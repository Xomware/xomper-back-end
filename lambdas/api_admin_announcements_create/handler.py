"""
POST /admin/announcements-create
================================
Admin-only endpoint that inserts one row into `league_announcements`
and writes a corresponding `admin_audit` row.

Body:
{
    "sleeper_user_id": "abc123",        // required — admin gate identity
    "email":           "user@..",       // optional fallback identity
    "title":           "2026 Rookie Draft",   // required, non-empty
    "body":            "July 6 — 6:30pm ET",  // required, non-empty
    "priority":        "critical",            // optional, default "info",
                                              //   must be 'critical' or 'info'
    "expires_at":      "2026-07-07T00:00:00Z",// optional ISO8601, default null
    "is_active":       true,                  // optional bool, default true
    "display_order":   0                      // optional int, default 0
}

Behavior:
- 200 + Success=true on a successful insert. Response carries the FULL
  inserted row (including server-assigned `id` + timestamps) + the
  audit row id (when the audit write succeeded).
- 400 on bad input (missing/empty title, missing/empty body, invalid
  priority, non-bool is_active, non-int display_order).
- 403 when the caller is not an admin.
- 500 on Supabase / unexpected failure.

Audit:
- action="announcements.create"
- target_table="league_announcements"
- target_id=<inserted row id>
- after=<insert payload>
- before=None (creation has no prior state)
"""
from __future__ import annotations

from typing import Any, Optional

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.announcements_store import create as create_announcement
from lambdas.common.audit_log import write_audit
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_announcements_create"

_ALLOWED_PRIORITIES: tuple[str, ...] = ("critical", "info")


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin announcements-create request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    try:
        payload = _validate_and_normalize(body)
    except ValueError as err:
        return success_response(
            {"Success": False, "Message": str(err)},
            status_code=400,
        )

    inserted = create_announcement(
        title=payload["title"],
        body=payload["body"],
        priority=payload["priority"],
        expires_at=payload["expires_at"],
        is_active=payload["is_active"],
        display_order=payload["display_order"],
    )

    inserted_id = (inserted or {}).get("id")

    # Best-effort audit write — failures NEVER fail the parent action
    # (see audit_log.write_audit docstring).
    actor = admin_user.get("sleeper_user_id") or admin_user.get("id") or "unknown"
    audit_row = write_audit(
        actor_user_id=str(actor),
        action="announcements.create",
        target_table="league_announcements",
        target_id=str(inserted_id) if inserted_id else None,
        before=None,
        after=payload,
    )
    audit_id = (audit_row or {}).get("id") if audit_row else None

    return success_response(
        {
            "Success": True,
            "row": inserted,
            "audit_id": audit_id,
        }
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_and_normalize(body: dict[str, Any]) -> dict[str, Any]:
    """Validate the create body and return a normalized payload dict
    matching the `create()` helper signature. Raises ValueError on any
    invalid input — caller turns that into a 400."""
    title = _require_non_empty_str(body.get("title"), "title")
    body_text = _require_non_empty_str(body.get("body"), "body")

    priority = body.get("priority")
    if priority is None or priority == "":
        priority = "info"
    if priority not in _ALLOWED_PRIORITIES:
        raise ValueError(
            f"Invalid priority '{priority}'. "
            f"Must be one of: {list(_ALLOWED_PRIORITIES)}"
        )

    expires_at = _normalize_optional_str(body.get("expires_at"), "expires_at")

    raw_is_active = body.get("is_active", True)
    is_active = _coerce_bool(raw_is_active, "is_active")

    raw_display_order = body.get("display_order", 0)
    display_order = _coerce_int(raw_display_order, "display_order")

    return {
        "title": title,
        "body": body_text,
        "priority": priority,
        "expires_at": expires_at,
        "is_active": is_active,
        "display_order": display_order,
    }


def _require_non_empty_str(value: Any, key: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Field '{key}' must be a string")
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"Field '{key}' must be non-empty")
    return trimmed


def _normalize_optional_str(value: Any, key: str) -> Optional[str]:
    """None / empty string both normalize to None. Non-string values
    raise. We don't try to parse + revalidate the ISO timestamp here —
    Supabase will reject obviously-wrong values, and accepting whatever
    the iOS DatePicker produces keeps us flexible."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Field '{key}' must be a string or null")
    trimmed = value.strip()
    return trimmed if trimmed else None


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


def _coerce_int(value: Any, key: str) -> int:
    if isinstance(value, bool):
        # bool is a subclass of int in Python — reject explicitly.
        raise ValueError(f"Field '{key}' must be an integer. Got: {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            raise ValueError(
                f"Field '{key}' must be an integer. Got: {value!r}"
            ) from None
    raise ValueError(f"Field '{key}' must be an integer. Got: {value!r}")
