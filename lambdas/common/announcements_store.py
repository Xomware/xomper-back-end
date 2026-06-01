"""
announcements_store helper
==========================
Read/write helpers for the Supabase `league_announcements` table.

Backs three layers:

1. Public read endpoint (`api_announcements`) — `list_active()` filters
   `is_active=true AND (expires_at IS NULL OR expires_at > now())`,
   ordered critical-first then by `display_order`. Best-effort: on
   Supabase failure returns `[]` so the iOS Landing page can transparently
   fall back to its hardcoded `LeagueAnnouncements.current` array.

2. Admin list endpoint (`api_admin_announcements_list`) — `list_all()`
   returns every row including inactive + expired so the admin UI can
   manage them. Also best-effort `[]` on failure (the lambda turns the
   empty result into `table_missing: true` for iOS).

3. Admin write endpoints (`api_admin_announcements_{create,update,delete}`)
   — `create`, `update`, and `delete` (soft) propagate Supabase HTTP
   errors so the admin client can surface failures + roll back optimistic
   UI state.

CRITICAL invariants:
- Read paths NEVER raise — Landing must never blank because Supabase is
  unreachable. Failures are logged at ERROR and degraded to `[]`.
- Write paths DO raise — admin needs to know when an edit failed.
- `delete` is soft (`is_active = false`). The hard-delete column is
  intentionally absent from the schema; if a row ever needs to vanish
  for real that's a Supabase dashboard chore.
- Field allowlist is enforced in `update` so callers can't accidentally
  patch `created_at` or insert a column that doesn't exist.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from lambdas.common.errors import NotFoundError
from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import (
    get_row,
    insert_row,
    list_rows,
    update_row,
)
from lambdas.common.utility_helpers import get_iso_timestamp

log = get_logger(__file__)

_TABLE = "league_announcements"

# Fields the admin update endpoint is allowed to patch. `id`,
# `created_at`, and `updated_at` are server-managed; anything else is a
# typo and should reject early.
_UPDATABLE_FIELDS: tuple[str, ...] = (
    "title",
    "body",
    "priority",
    "expires_at",
    "is_active",
    "display_order",
)

# Default returned for `priority` when a row has an unexpected value.
# Defensive — the CHECK constraint should make this unreachable.
_DEFAULT_PRIORITY = "info"


# ---------------------------------------------------------------------------
# Read helpers (best-effort)
# ---------------------------------------------------------------------------


def list_active() -> list[dict[str, Any]]:
    """Return rows for the public-read endpoint: `is_active=true` AND
    (`expires_at IS NULL` OR `expires_at > now()`), ordered:
        1. critical priority before info  (DESC on priority — 'critical'
           sorts after 'info' alphabetically, so we reverse it via
           Python sort below)
        2. display_order ascending
        3. created_at descending (newest first within same order)

    Best-effort: on any Supabase failure (including the
    `relation does not exist` case where the migration hasn't been
    applied yet), returns `[]` and logs at ERROR so CloudWatch surfaces
    the misconfiguration. Never raises.
    """
    # Fetch all active rows; PostgREST's eq filter is enough for
    # `is_active=true`. We post-filter on `expires_at > now()` in Python
    # because PostgREST's OR semantics across NULL + comparison are
    # awkward via the basic `list_rows` helper, and the volume here is
    # trivial (single league, < ~50 active rows ever).
    try:
        rows, _ = list_rows(
            _TABLE,
            filters={"is_active": "true"},
            limit=200,
            cursor=None,
            order_by="display_order.asc",
        )
    except Exception as err:  # noqa: BLE001 — best-effort: never block Landing
        log.error(
            f"announcements_store.list_active: fetch failed — returning "
            f"empty list (likely table_missing). error={err}"
        )
        return []

    rows = rows or []
    now = datetime.now(timezone.utc)
    filtered = [row for row in rows if _not_expired(row, now)]
    return sorted(filtered, key=_active_sort_key)


def list_all() -> list[dict[str, Any]]:
    """Return every row for the admin list view (including inactive +
    expired). Ordered display_order ASC, created_at DESC. Best-effort:
    `[]` on Supabase failure.
    """
    try:
        rows, _ = list_rows(
            _TABLE,
            filters=None,
            limit=200,
            cursor=None,
            order_by="display_order.asc",
        )
    except Exception as err:  # noqa: BLE001 — graceful fallback
        log.error(
            f"announcements_store.list_all: fetch failed — returning "
            f"empty list (likely table_missing). error={err}"
        )
        return []

    rows = rows or []
    # Secondary sort on created_at desc within same display_order.
    return sorted(rows, key=_admin_sort_key)


# ---------------------------------------------------------------------------
# Write helpers (raise on failure)
# ---------------------------------------------------------------------------


def create(
    title: str,
    body: str,
    priority: str = "info",
    expires_at: Optional[str] = None,
    is_active: bool = True,
    display_order: int = 0,
) -> dict[str, Any]:
    """Insert one row and return it (including the server-assigned `id`
    and timestamps). Callers must validate inputs before calling — this
    helper does no validation beyond what Supabase itself enforces."""
    row: dict[str, Any] = {
        "title": title,
        "body": body,
        "priority": priority,
        "expires_at": expires_at,
        "is_active": bool(is_active),
        "display_order": int(display_order),
    }
    return insert_row(_TABLE, row)


def update(announcement_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    """PATCH one row by id. Raises `NotFoundError` if no row matches.

    `fields` is allowlisted — keys outside `_UPDATABLE_FIELDS` raise
    `ValueError` so the caller can return 400. The handler should
    additionally validate the values themselves (priority enum, types,
    non-empty strings) before calling.

    Stamps `updated_at` on every patch so the UI's "last edited" hint
    reflects the actual write moment.
    """
    if not fields:
        raise ValueError(
            "announcements_store.update: empty 'fields' — nothing to update"
        )

    unknown = [key for key in fields.keys() if key not in _UPDATABLE_FIELDS]
    if unknown:
        raise ValueError(
            f"announcements_store.update: unknown field '{unknown[0]}'. "
            f"Allowed fields: {list(_UPDATABLE_FIELDS)}"
        )

    existing = get_row(_TABLE, "id", announcement_id)
    if not existing:
        raise NotFoundError(
            message=f"announcement not found: {announcement_id}",
            handler="announcements_store",
            function="update",
            resource=announcement_id,
        )

    patch = dict(fields)
    patch["updated_at"] = get_iso_timestamp()

    updated = update_row(_TABLE, "id", announcement_id, patch)
    return updated or existing


def delete(announcement_id: str) -> dict[str, Any]:
    """Soft delete: flip `is_active=false`. Returns the updated row.
    Raises `NotFoundError` if no row matches.
    """
    return update(announcement_id, {"is_active": False})


# ---------------------------------------------------------------------------
# Sort + filter internals
# ---------------------------------------------------------------------------


def _not_expired(row: dict[str, Any], now: datetime) -> bool:
    """True when `expires_at` is null OR strictly in the future."""
    raw = row.get("expires_at")
    if not raw:
        return True
    parsed = _parse_iso(raw)
    if parsed is None:
        # Unparseable timestamp — defensive: treat as not-expired so we
        # don't silently hide a row over a parsing bug.
        log.warning(
            f"announcements_store: unparseable expires_at on row "
            f"id={row.get('id')} value={raw!r} — treating as not expired"
        )
        return True
    return parsed > now


def _parse_iso(raw: Any) -> Optional[datetime]:
    """Parse an ISO8601 timestamp string into an aware datetime in UTC.
    Returns None on parse failure. Accepts both `Z` and `+00:00` suffix."""
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    # `datetime.fromisoformat` in Python 3.11+ accepts `Z`; the lambda
    # runtime is 3.10 where we must swap it for `+00:00`.
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _active_sort_key(row: dict[str, Any]) -> tuple[int, int, float]:
    """Sort key for the public-read endpoint:
        priority_rank   0 for critical, 1 for info  (critical first)
        display_order   ascending
        created_at_neg  negative epoch seconds  (newer first because
                        a larger positive timestamp negates to a
                        smaller value, which sorts earlier ASC)
    """
    priority_rank = 0 if (row.get("priority") or _DEFAULT_PRIORITY) == "critical" else 1
    display_order = int(row.get("display_order") or 0)
    return (priority_rank, display_order, _negated_epoch(row.get("created_at")))


def _admin_sort_key(row: dict[str, Any]) -> tuple[int, float]:
    """Sort key for admin list: display_order ASC, created_at DESC."""
    display_order = int(row.get("display_order") or 0)
    return (display_order, _negated_epoch(row.get("created_at")))


def _negated_epoch(raw: Any) -> float:
    """Return a sort token such that LARGER `raw` timestamps sort
    EARLIER in ascending order (i.e. "newest first" within a stable
    Python `sorted`). On parse failure, returns +inf so the row sorts
    last (oldest)."""
    parsed = _parse_iso(raw) if raw else None
    if parsed is None:
        return float("inf")
    return -parsed.timestamp()
