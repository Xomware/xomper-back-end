"""
POST /admin/announcements-update
================================
Admin-only endpoint that PATCHes one row in `league_announcements`
with a strict per-field allowlist + per-field validation. Writes one
row into `admin_audit` per successful update with a before/after diff.

Body:
{
    "sleeper_user_id": "abc123",        // required — admin gate identity
    "email":           "user@..",       // optional fallback identity
    "id":              "<uuid>",        // required — row to update
    "fields": {
        "title":         "New title",         // optional, non-empty
        "body":          "New body",          // optional, non-empty
        "priority":      "critical",          // optional, 'critical' or 'info'
        "expires_at":    "2026-07-07T00:00:00Z",  // optional ISO8601 or null
        "is_active":     true,                // optional bool
        "display_order": 1                    // optional int
    }
}

Behavior:
- 200 + Success=true on a successful update. Response carries the FULL
  updated row + the `before` snapshot of changed keys + the audit row
  id (when the audit write succeeded).
- 400 on bad input (missing id, missing/empty fields, unknown field
  key, invalid priority, non-bool is_active, non-int display_order).
- 403 when the caller is not an admin.
- 404 when the row isn't in `league_announcements`.
- 500 on Supabase / unexpected failure.
"""
from __future__ import annotations

from typing import Any, Optional

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.announcements_store import update as update_announcement
from lambdas.common.audit_log import write_audit
from lambdas.common.errors import NotFoundError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import get_row
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_announcements_update"

# Mirrors `_UPDATABLE_FIELDS` in announcements_store. Duplicated here so
# the handler can reject unknown keys with a 400 BEFORE we hit the store
# (which would also raise but with a less polished error).
_ALLOWED_FIELDS: tuple[str, ...] = (
    "title",
    "body",
    "priority",
    "expires_at",
    "is_active",
    "display_order",
)

_ALLOWED_PRIORITIES: tuple[str, ...] = ("critical", "info")


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin announcements-update request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    announcement_id = (body.get("id") or "").strip()
    if not announcement_id:
        return success_response(
            {"Success": False, "Message": "Missing 'id' in body"},
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

    try:
        normalized = _validate_and_normalize(raw_fields)
    except ValueError as err:
        return success_response(
            {"Success": False, "Message": str(err)},
            status_code=400,
        )

    # Snapshot the row BEFORE the update so the audit row carries the
    # diff. We do this here (not inside the store) so the handler keeps
    # full control over the before/after shape.
    existing = get_row("league_announcements", "id", announcement_id)
    if not existing:
        return success_response(
            {
                "Success": False,
                "Message": f"Announcement not found: {announcement_id}",
            },
            status_code=404,
        )

    before: dict[str, Any] = {key: existing.get(key) for key in normalized.keys()}

    try:
        updated = update_announcement(announcement_id, normalized)
    except NotFoundError as err:
        # Race condition: row was deleted between the get_row check and
        # the update. Treat as 404.
        return success_response(
            {"Success": False, "Message": str(err.message)},
            status_code=404,
        )

    # Best-effort audit write — failures NEVER fail the parent action.
    actor = admin_user.get("sleeper_user_id") or admin_user.get("id") or "unknown"
    audit_row = write_audit(
        actor_user_id=str(actor),
        action="announcements.update",
        target_table="league_announcements",
        target_id=announcement_id,
        before=before,
        after=normalized,
    )
    audit_id = (audit_row or {}).get("id") if audit_row else None

    return success_response(
        {
            "Success": True,
            "id": announcement_id,
            "row": updated or existing,
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
        if key == "title":
            normalized[key] = _require_non_empty_str(value, "title")
        elif key == "body":
            normalized[key] = _require_non_empty_str(value, "body")
        elif key == "priority":
            if value not in _ALLOWED_PRIORITIES:
                raise ValueError(
                    f"Invalid priority '{value}'. "
                    f"Must be one of: {list(_ALLOWED_PRIORITIES)}"
                )
            normalized[key] = value
        elif key == "expires_at":
            normalized[key] = _normalize_optional_str(value, "expires_at")
        elif key == "is_active":
            normalized[key] = _coerce_bool(value, "is_active")
        elif key == "display_order":
            normalized[key] = _coerce_int(value, "display_order")
    return normalized


def _require_non_empty_str(value: Any, key: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Field '{key}' must be a string")
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"Field '{key}' must be non-empty")
    return trimmed


def _normalize_optional_str(value: Any, key: str) -> Optional[str]:
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
