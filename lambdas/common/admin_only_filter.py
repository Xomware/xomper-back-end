"""
admin_only_filter
=================
Shared helper for test-mode recipient filtering across all scheduled
notif lambdas. When an admin flips a lambda's `test_mode=true` via the
cron-settings admin UI, the lambda restricts delivery to admin
(Dominick) only so we can preview the output before fanning out to
the whole league.

Kept here (rather than inline in each lambda) so the five notif
lambdas share a single source of truth — preventing recipient-filter
drift across handlers.
"""
from __future__ import annotations

from typing import Any

from lambdas.common.constants import ADMIN_DOMINICK_USER_ID
from lambdas.common.logger import get_logger

log = get_logger(__file__)


def filter_to_admin_only(
    users: list[dict[str, Any]],
    admin_user_id: str = ADMIN_DOMINICK_USER_ID,
) -> list[dict[str, Any]]:
    """Restrict a `whitelisted_users` row list to admin only.

    Returns the matching row(s) — typically exactly one. When the
    admin isn't present in the input list (misconfigured
    whitelisted_users, or admin is inactive), returns `[]` and logs
    at WARNING so CloudWatch surfaces the dead-letter test send.

    Accepts the standard `whitelisted_users` shape — each row must
    carry `sleeper_user_id`. Rows missing the field are ignored.
    """
    if not users:
        log.warning(
            "filter_to_admin_only: input user list was empty — "
            "test-mode send will have no recipients"
        )
        return []

    matched = [
        u for u in users
        if u.get("sleeper_user_id") == admin_user_id
    ]

    if not matched:
        log.warning(
            f"filter_to_admin_only: admin user_id={admin_user_id} not "
            f"present in input list of {len(users)} user(s) — test-mode "
            f"send will have no recipients"
        )
        return []

    return matched
