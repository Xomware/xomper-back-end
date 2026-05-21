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
