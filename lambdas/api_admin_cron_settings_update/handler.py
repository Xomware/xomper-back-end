"""
POST /admin/cron-settings-update
================================
Admin-only endpoint that PATCHes one row in `admin_cron_settings`.
Validates the `cron_key` against an allowlist (the five seeded
lambdas) so an attacker can't insert a typo or invent a new key.

Writes one row into `admin_audit` per successful update with
`action="cron_settings.update"` capturing the before / after diff.

Body:
{
    "sleeper_user_id": "abc123",      // required — admin gate identity
    "email":           "user@.. ",    // optional fallback identity
    "cron_key":        "notif_weekly_recap",   // required, must be allowlisted
    "enabled":         true,                   // optional bool
    "test_mode":       false                   // optional bool
}

At least one of `enabled` / `test_mode` MUST be supplied.

Behavior:
- 200 + Success=true on a successful update. Response carries the
  resulting cron_key + enabled + test_mode + audit_id (when the audit
  write succeeded).
- 400 on bad input (missing cron_key, unknown cron_key, no toggleable
  field supplied, non-bool value).
- 403 when the caller is not an admin.
- 404 when the cron_key row is missing (should never happen post-seed
  but defensive — the helper raises NotFoundError).
- 500 on Supabase / unexpected failure.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.admin_gate import NotAdmin, require_admin
from lambdas.common.audit_log import write_audit
from lambdas.common.cron_settings import get_cron_setting, update_cron_setting
from lambdas.common.errors import NotFoundError, handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import parse_body, success_response

log = get_logger(__file__)
HANDLER = "api_admin_cron_settings_update"

# Allowlist mirrors the five rows seeded by the migration. Any new cron
# lambda needs an entry both here AND in the migration's seed insert.
_ALLOWED_CRON_KEYS: tuple[str, ...] = (
    "notif_weekly_recap",
    "notif_lineup_not_set",
    "notif_close_game_alert",
    "notif_worldcup_movement",
    "notif_ai_review_weekly",
    "notif_week_preview",
)


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    log.info("Admin cron-settings-update request")

    body = parse_body(event)

    try:
        admin_user = require_admin(event, body)
    except NotAdmin:
        return success_response(
            {"Success": False, "Message": "Not authorized"},
            status_code=403,
        )

    cron_key = (body.get("cron_key") or "").strip()
    if not cron_key:
        return success_response(
            {"Success": False, "Message": "Missing 'cron_key' in body"},
            status_code=400,
        )

    if cron_key not in _ALLOWED_CRON_KEYS:
        return success_response(
            {
                "Success": False,
                "Message": (
                    f"Unknown cron_key '{cron_key}'. "
                    f"Allowed keys: {list(_ALLOWED_CRON_KEYS)}"
                ),
            },
            status_code=400,
        )

    # Validate at least one of (enabled, test_mode) was supplied.
    raw_enabled = body.get("enabled", None)
    raw_test_mode = body.get("test_mode", None)
    if raw_enabled is None and raw_test_mode is None:
        return success_response(
            {
                "Success": False,
                "Message": (
                    "Body must include at least one of 'enabled' or "
                    "'test_mode'."
                ),
            },
            status_code=400,
        )

    try:
        enabled = _coerce_bool(raw_enabled, "enabled") if raw_enabled is not None else None
        test_mode = _coerce_bool(raw_test_mode, "test_mode") if raw_test_mode is not None else None
    except ValueError as err:
        return success_response(
            {"Success": False, "Message": str(err)},
            status_code=400,
        )

    # Snapshot the row BEFORE the update so the audit row carries the
    # diff. get_cron_setting is best-effort; on Supabase outage it
    # returns safe defaults — but in that case `update_cron_setting`
    # below will raise too, so we won't actually mis-audit silently.
    before_setting = get_cron_setting(cron_key)
    before: dict[str, Any] = {}
    if enabled is not None:
        before["enabled"] = before_setting.get("enabled")
    if test_mode is not None:
        before["test_mode"] = before_setting.get("test_mode")

    try:
        updated = update_cron_setting(
            cron_key=cron_key,
            enabled=enabled,
            test_mode=test_mode,
        )
    except NotFoundError as err:
        # Defensive — the allowlist should make this unreachable since
        # the migration seeds all five rows.
        return success_response(
            {"Success": False, "Message": str(err.message)},
            status_code=404,
        )

    after: dict[str, Any] = {}
    if enabled is not None:
        after["enabled"] = bool(enabled)
    if test_mode is not None:
        after["test_mode"] = bool(test_mode)

    # Best-effort audit write — failures NEVER fail the parent action
    # (see audit_log.write_audit docstring).
    actor = admin_user.get("sleeper_user_id") or admin_user.get("id") or "unknown"
    audit_row = write_audit(
        actor_user_id=str(actor),
        action="cron_settings.update",
        target_table="admin_cron_settings",
        target_id=cron_key,
        before=before,
        after=after,
    )
    audit_id = (audit_row or {}).get("id") if audit_row else None

    return success_response(
        {
            "Success": True,
            "cron_key": cron_key,
            "enabled": bool(updated.get("enabled", before_setting.get("enabled", True))),
            "test_mode": bool(updated.get("test_mode", before_setting.get("test_mode", False))),
            "before": before,
            "after": after,
            "audit_id": audit_id,
        }
    )


def _coerce_bool(value: Any, key: str) -> bool:
    """Same shape as api_admin_users_update — accepts real bools and
    "true"/"false" string variants. Anything else is rejected."""
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
