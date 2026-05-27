"""
admin_audit write helper
========================
Single canonical place for admin-action audit writes (admin-portal F4).

Every mutating admin lambda calls `write_audit(...)` inline at the end
of the happy path. JSONB `before` + `after` blobs hold only the keys
that actually changed. Read paths (api_admin_audit_list) read the same
rows via `supabase_helper.list_rows`.

CRITICAL invariant — best-effort writes:
    Audit failures MUST NEVER fail the parent action. The primary
    behaviour (test email sent, flag toggled, user updated) is the
    user-visible outcome; an audit failure logs to CloudWatch and is
    silently swallowed so the lambda still returns 200.

    Concretely: every error raised by Supabase (table missing, REST
    failure, timeouts, bad-key-rejection) is caught and logged. The
    helper never re-raises.
"""
from __future__ import annotations

from typing import Any, Optional

from lambdas.common.logger import get_logger
from lambdas.common.supabase_helper import insert_row

log = get_logger(__file__)

_AUDIT_TABLE = "admin_audit"


def write_audit(
    *,
    actor_user_id: str,
    action: str,
    target_table: Optional[str] = None,
    target_id: Optional[str] = None,
    before: Optional[dict[str, Any]] = None,
    after: Optional[dict[str, Any]] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    """Write one row to the `admin_audit` Supabase table.

    Best-effort: any exception (including the table not existing yet
    in environments where the Supabase migration hasn't been applied)
    is caught + logged. Never raises.

    Returns the inserted row on success, or `None` when the write
    failed. Callers should not depend on the return value — the audit
    side-effect is purely advisory.
    """
    row: dict[str, Any] = {
        "actor_user_id": actor_user_id,
        "action": action,
        "target_table": target_table,
        "target_id": target_id,
        "before": before,
        "after": after,
        "metadata": metadata or {},
    }
    try:
        result = insert_row(_AUDIT_TABLE, row)
        log.info(
            f"audit_log: wrote row action={action} "
            f"actor={actor_user_id} target_table={target_table} "
            f"target_id={target_id}"
        )
        return result
    except Exception as err:  # noqa: BLE001 — best-effort: swallow all
        # Audit failures must never fail the parent action. Log at
        # ERROR with context so CloudWatch picks it up but return
        # cleanly so the calling lambda continues.
        log.error(
            f"audit_log: write FAILED action={action} "
            f"actor={actor_user_id} target_table={target_table} "
            f"target_id={target_id} error={err}"
        )
        return None
