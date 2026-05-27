"""
XOMPER Supabase Helper
======================
Read-only REST wrappers for the Supabase tables backing the app
(`whitelisted_leagues`, `whitelisted_users`). Used by scheduled
notification lambdas that fan out by league + manager and don't have
the iOS client to pre-resolve recipients.

Auth uses the Supabase service-role key. URL + key are pulled from
SSM at first use via `ssm_helpers.get_parameter`, matching the
existing lazy-load pattern. SSM paths:
  /<app>/api/SUPABASE_URL
  /<app>/api/SUPABASE_SERVICE_KEY
"""
from __future__ import annotations

from typing import Any
import requests

from lambdas.common.constants import PRODUCT
from lambdas.common.errors import SleeperAPIError  # reused — generic external-API error
from lambdas.common.logger import get_logger
from lambdas.common.ssm_helpers import get_parameter

log = get_logger(__file__)

_SSM_URL_PATH = f"/{PRODUCT}/api/SUPABASE_URL"
_SSM_SERVICE_KEY_PATH = f"/{PRODUCT}/api/SUPABASE_SERVICE_KEY"


def _supabase_url() -> str:
    return get_parameter(_SSM_URL_PATH).rstrip("/")


def _supabase_headers() -> dict[str, str]:
    key = get_parameter(_SSM_SERVICE_KEY_PATH)
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def _get(path: str, params: dict[str, str], description: str) -> Any:
    url = f"{_supabase_url()}/rest/v1/{path}"
    try:
        response = requests.get(
            url,
            headers=_supabase_headers(),
            params=params,
            timeout=15,
        )
        if response.status_code == 200:
            return response.json()
        raise SleeperAPIError(
            message=f"{description}: HTTP {response.status_code}",
            function="supabase_helper._get",
            endpoint=url,
        )
    except SleeperAPIError:
        raise
    except Exception as err:
        raise SleeperAPIError(
            message=f"{description}: {err}",
            function="supabase_helper._get",
            endpoint=url,
        ) from err


def _post(path: str, row: dict[str, Any], description: str) -> Any:
    """POST a single row to the Supabase REST endpoint.

    Uses the `Prefer: return=representation` header so the response body
    is the inserted row (useful for surfacing server-side defaults like
    `id` + `created_at`).
    """
    url = f"{_supabase_url()}/rest/v1/{path}"
    headers = {**_supabase_headers(), "Prefer": "return=representation"}
    try:
        response = requests.post(
            url,
            headers=headers,
            json=row,
            timeout=15,
        )
        if response.status_code in (200, 201):
            parsed = response.json()
            # PostgREST returns a list for single-row inserts when
            # Prefer=return=representation is set.
            if isinstance(parsed, list):
                return parsed[0] if parsed else {}
            return parsed
        raise SleeperAPIError(
            message=f"{description}: HTTP {response.status_code} body={response.text[:200]}",
            function="supabase_helper._post",
            endpoint=url,
        )
    except SleeperAPIError:
        raise
    except Exception as err:
        raise SleeperAPIError(
            message=f"{description}: {err}",
            function="supabase_helper._post",
            endpoint=url,
        ) from err


def _patch(
    path: str,
    params: dict[str, str],
    fields: dict[str, Any],
    description: str,
) -> Any:
    """PATCH a single row identified by the given filter params."""
    url = f"{_supabase_url()}/rest/v1/{path}"
    headers = {**_supabase_headers(), "Prefer": "return=representation"}
    try:
        response = requests.patch(
            url,
            headers=headers,
            params=params,
            json=fields,
            timeout=15,
        )
        if response.status_code in (200, 204):
            if response.status_code == 204 or not response.text:
                return {}
            parsed = response.json()
            if isinstance(parsed, list):
                return parsed[0] if parsed else {}
            return parsed
        raise SleeperAPIError(
            message=f"{description}: HTTP {response.status_code} body={response.text[:200]}",
            function="supabase_helper._patch",
            endpoint=url,
        )
    except SleeperAPIError:
        raise
    except Exception as err:
        raise SleeperAPIError(
            message=f"{description}: {err}",
            function="supabase_helper._patch",
            endpoint=url,
        ) from err


# ---------------------------------------------------------------------------
# Generic write + list helpers (used by admin-portal F4 — audit log, table
# editors, audit feed). Kept generic so future admin tables share plumbing.
# ---------------------------------------------------------------------------


def insert_row(table: str, row: dict[str, Any]) -> dict[str, Any]:
    """Insert a single row into the given Supabase table and return the
    inserted row (including server-side defaults). Raises
    `SleeperAPIError` on HTTP failure."""
    return _post(
        table,
        row,
        f"Error inserting row into {table}",
    )


def update_row(
    table: str,
    match_column: str,
    match_value: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    """PATCH a single row identified by `match_column == match_value`
    with the provided field dict. Returns the updated row. Raises
    `SleeperAPIError` on HTTP failure."""
    return _patch(
        table,
        {match_column: f"eq.{match_value}"},
        fields,
        f"Error updating row in {table}",
    )


def get_row(
    table: str,
    match_column: str,
    match_value: str,
) -> dict[str, Any] | None:
    """Fetch a single row by `match_column == match_value`. Returns
    None when no row matches. Used by the admin update endpoints to
    capture the `before` snapshot prior to mutating."""
    rows = _get(
        table,
        {f"{match_column}": f"eq.{match_value}", "limit": "1"},
        f"Error fetching row from {table}",
    )
    return rows[0] if rows else None


def list_rows(
    table: str,
    filters: dict[str, str] | None = None,
    limit: int = 50,
    cursor: str | None = None,
    order_by: str = "created_at.desc",
) -> tuple[list[dict[str, Any]], str | None]:
    """List rows from a Supabase table with optional filters + cursor
    pagination.

    - `filters` are eq-comparisons against the column name (e.g.
      `{"action": "users.update"}` → `action=eq.users.update`).
    - `cursor` is the `created_at` timestamp of the last row of the
      previous page; results return rows strictly older than `cursor`
      (assuming `order_by` is `created_at.desc`).
    - `order_by` is a PostgREST-style `column.dir` string. Defaults to
      newest-first.

    Returns `(rows, next_cursor)` where `next_cursor` is the
    `created_at` of the last row of the current page when more rows
    likely exist, else None.
    """
    params: dict[str, str] = {
        "limit": str(limit),
        "order": order_by,
    }
    if filters:
        for column, value in filters.items():
            params[column] = f"eq.{value}"
    if cursor:
        # Assume created_at desc ordering — cursor is the last created_at
        # we saw; the next page starts strictly older than that.
        params["created_at"] = f"lt.{cursor}"

    rows = _get(table, params, f"Error listing rows from {table}") or []

    # Heuristic: if we got `limit` rows back, assume more exist and emit
    # the last row's created_at as the next cursor. If we got fewer than
    # `limit`, we've reached the tail.
    next_cursor: str | None = None
    if rows and len(rows) >= limit:
        last = rows[-1]
        ts = last.get("created_at")
        if ts:
            next_cursor = str(ts)

    return rows, next_cursor


def get_active_whitelisted_league() -> dict[str, Any] | None:
    """Returns the single active row from `whitelisted_leagues`, or
    None if no active league is configured."""
    rows = _get(
        "whitelisted_leagues",
        {"is_active": "eq.true", "limit": "1"},
        "Error fetching active whitelisted league",
    )
    return rows[0] if rows else None


def get_active_whitelisted_users() -> list[dict[str, Any]]:
    """Returns all rows from `whitelisted_users` where `is_active=true`.
    Each row carries email, display_name, sleeper_username, and
    (when linked) sleeper_user_id — used to resolve push recipients."""
    return _get(
        "whitelisted_users",
        {"is_active": "eq.true"},
        "Error fetching active whitelisted users",
    )


def get_whitelisted_user_by_sleeper_id(sleeper_user_id: str) -> dict[str, Any] | None:
    """Lookup a whitelisted user by their Sleeper user_id. Used by
    the admin endpoints to gate access via the `is_admin` column.
    Returns nil for unknown / inactive accounts."""
    rows = _get(
        "whitelisted_users",
        {"sleeper_user_id": f"eq.{sleeper_user_id}", "is_active": "eq.true"},
        "Error fetching whitelisted user by sleeper id",
    )
    return rows[0] if rows else None


def get_whitelisted_user_by_email(email: str) -> dict[str, Any] | None:
    """Lookup a whitelisted user by email. Fallback when the
    Sleeper id isn't available (e.g. token-based auth that only
    has the user's email)."""
    rows = _get(
        "whitelisted_users",
        {"email": f"eq.{email}", "is_active": "eq.true"},
        "Error fetching whitelisted user by email",
    )
    return rows[0] if rows else None
