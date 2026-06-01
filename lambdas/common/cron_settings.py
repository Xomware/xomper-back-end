"""
cron_settings helper
====================
Read/write helpers for the Supabase `admin_cron_settings` table.

Each scheduled notif lambda owns one row keyed by `cron_key`. On cold
start the lambda calls `get_cron_setting(cron_key)` to fetch its
enabled + test_mode flags. The admin endpoints
(`api_admin_cron_settings_list` + `api_admin_cron_settings_update`)
use `list_cron_settings` + `update_cron_setting` to power the iOS
admin UI.

CRITICAL invariant — best-effort reads:
    Supabase outages MUST NEVER block a scheduled lambda from
    firing. The offseason guard is the actual safety belt; if our
    settings table is unreachable we'd rather send a notification
    than silently skip it. `get_cron_setting` therefore catches every
    exception, logs a warning, and returns the safe default
    `{enabled: True, test_mode: False, description: None,
      cron_key: <key>}`.

    `list_cron_settings` is similarly best-effort — returns `[]` on
    failure (including the `table_missing` case where the migration
    hasn't been applied yet). The api_admin_cron_settings_list
    handler turns this into a `table_missing: true` flag for the iOS
    empty state.

    `update_cron_setting` is the only call that DOES raise — admin
    edits need to surface failures so the iOS rollback path can fire.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.errors import NotFoundError
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import get_row, list_rows, update_row

log = get_logger(__file__)

_TABLE = "admin_cron_settings"

# Default returned by `get_cron_setting` when Supabase is unreachable or
# the row is missing. "Send" is safer than "skip" — the offseason guard
# remains the primary safety.
_SAFE_DEFAULT: dict[str, Any] = {
    "enabled": True,
    "test_mode": False,
    "description": None,
}


def get_cron_setting(cron_key: str) -> dict[str, Any]:
    """Fetch one cron setting by `cron_key`. Best-effort.

    Returns `{cron_key, enabled, test_mode, description}` where
    `description` may be None. On Supabase failure OR a missing row,
    returns the safe default (`enabled=True, test_mode=False,
    description=None`). Never raises.
    """
    try:
        row = get_row(_TABLE, "cron_key", cron_key)
    except Exception as err:  # noqa: BLE001 — best-effort: never block
        log.warning(
            f"cron_settings.get_cron_setting: Supabase fetch failed for "
            f"cron_key={cron_key} — defaulting to enabled=True, "
            f"test_mode=False. error={err}"
        )
        return {**_SAFE_DEFAULT, "cron_key": cron_key}

    if not row:
        log.warning(
            f"cron_settings.get_cron_setting: no row for cron_key={cron_key} "
            f"— defaulting to enabled=True, test_mode=False"
        )
        return {**_SAFE_DEFAULT, "cron_key": cron_key}

    return {
        "cron_key": row.get("cron_key", cron_key),
        "enabled": bool(row.get("enabled", True)),
        "test_mode": bool(row.get("test_mode", False)),
        "description": row.get("description"),
    }


def list_cron_settings() -> list[dict[str, Any]]:
    """Return all cron settings ordered by `cron_key` ascending.

    Best-effort: on any Supabase failure (including the table not
    existing because the migration hasn't been applied yet), returns
    `[]` and logs at ERROR so CloudWatch surfaces the misconfig. The
    api_admin_cron_settings_list lambda turns the empty result into a
    `table_missing: true` flag for iOS.
    """
    try:
        rows, _ = list_rows(
            _TABLE,
            filters=None,
            limit=200,
            cursor=None,
            order_by="cron_key.asc",
        )
    except Exception as err:  # noqa: BLE001 — graceful fallback
        log.error(
            f"cron_settings.list_cron_settings: fetch failed — returning "
            f"empty list (likely table_missing). error={err}"
        )
        return []
    return rows or []


def update_cron_setting(
    cron_key: str,
    enabled: bool | None,
    test_mode: bool | None,
) -> dict[str, Any]:
    """PATCH one cron setting row. Raises NotFoundError when the row
    doesn't exist; lets Supabase HTTP errors propagate so the calling
    endpoint can surface them.

    At least one of `enabled` / `test_mode` must be non-None — the
    caller is expected to validate this before calling.
    """
    fields: dict[str, Any] = {}
    if enabled is not None:
        fields["enabled"] = bool(enabled)
    if test_mode is not None:
        fields["test_mode"] = bool(test_mode)

    if not fields:
        # Defensive — handler should reject before getting here.
        raise ValueError(
            "cron_settings.update_cron_setting: nothing to update "
            "(both 'enabled' and 'test_mode' were None)"
        )

    # Stamp updated_at on every patch so the UI's "last toggled" hint
    # reflects the actual write moment (not whatever default Supabase
    # would compute from triggers).
    from lambdas.common.utility_helpers import get_iso_timestamp

    fields["updated_at"] = get_iso_timestamp()

    existing = get_row(_TABLE, "cron_key", cron_key)
    if not existing:
        raise NotFoundError(
            message=f"cron_key not found: {cron_key}",
            handler="cron_settings",
            function="update_cron_setting",
            resource=cron_key,
        )

    updated = update_row(_TABLE, "cron_key", cron_key, fields)
    return updated or existing
