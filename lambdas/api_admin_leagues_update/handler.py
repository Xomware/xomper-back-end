"""
POST /admin/leagues-update
==========================
Admin-only endpoint that PATCHes a single row in `whitelisted_leagues`
with a strict per-field allowlist + per-field validation. Writes one
row into `admin_audit` per successful update.

Body:
{
    "sleeper_user_id": "abc123",         // required — admin gate identity
    "email":           "user@..",        // optional fallback identity
    "league_id":       "LEAGUE_ID",      // required — sleeper_league_id of the league row
    "fields": {
        "league_name": "New Name",       // optional, non-empty trimmed string
        "is_active":   true,             // optional, bool
        "is_dynasty":  false,            // optional, bool
        "has_taxi":    false             // optional, bool
    }
}

Behavior:
- 200 + Success=true on a successful update. Response carries the FULL
  updated `whitelisted_leagues` row + the `before` snapshot + the
  audit row id (when the audit write succeeded).
- 400 on bad input (missing `league_id`, missing/empty `fields`,
  unknown field key, non-bool value).
- 403 when the caller is not an admin.
- 404 when the target league is not in `whitelisted_leagues`.
- 500 on Supabase / unexpected failure.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.audit_log import write_audit
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import get_row, update_row
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_leagues_update"

# Editable columns on `whitelisted_leagues`. `sleeper_league_id` is the
# immutable join key — surfaced in the iOS form as a read-only row.
_ALLOWED_FIELDS: tuple[str, ...] = (
    "league_name",
    "is_active",
    "is_dynasty",
    "has_taxi",
)

# Match column on the Supabase table. `sleeper_league_id` is the
# canonical league id used as the join key.
_MATCH_COLUMN = "sleeper_league_id"


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin leagues-update request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    league_id = (body.get("league_id") or "").strip()
    if not league_id:
        return success_response(
            {"Success": False, "Message": "Missing 'league_id' in body"},
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

    existing = get_row("whitelisted_leagues", _MATCH_COLUMN, league_id)
    if not existing:
        return success_response(
            {
                "Success": False,
                "Message": f"League not found: {league_id}",
            },
            status_code=404,
        )

    before: dict[str, Any] = {key: existing.get(key) for key in normalized.keys()}

    updated = update_row(
        "whitelisted_leagues",
        _MATCH_COLUMN,
        league_id,
        normalized,
    )

    actor = admin_user.get("sleeper_user_id") or admin_user.get("id") or "unknown"
    audit_row = write_audit(
        actor_user_id=str(actor),
        action="leagues.update",
        target_table="whitelisted_leagues",
        target_id=league_id,
        before=before,
        after=normalized,
    )
    audit_id = (audit_row or {}).get("id") if audit_row else None

    return success_response(
        {
            "Success": True,
            "league_id": league_id,
            "league": updated or existing,
            "before": before,
            "after": normalized,
            "audit_id": audit_id,
        }
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_and_normalize(raw_fields: dict[str, Any]) -> dict[str, Any]:
    unknown = [k for k in raw_fields.keys() if k not in _ALLOWED_FIELDS]
    if unknown:
        raise ValueError(
            f"Unknown field '{unknown[0]}'. "
            f"Allowed fields: {list(_ALLOWED_FIELDS)}"
        )

    normalized: dict[str, Any] = {}
    for key, value in raw_fields.items():
        if key == "league_name":
            normalized[key] = _validate_non_empty_str(value, "league_name")
        elif key in ("is_active", "is_dynasty", "has_taxi"):
            normalized[key] = _coerce_bool(value, key)
    return normalized


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
