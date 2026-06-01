"""
Tests for `lambdas.common.cron_settings` (admin-cron-settings).

Covers:
  - get_cron_setting happy path: returns row fields.
  - get_cron_setting best-effort defaults on Supabase failure:
    returns enabled=True, test_mode=False, never raises.
  - get_cron_setting safe default when row is missing (None).
  - list_cron_settings happy path: returns rows via list_rows.
  - list_cron_settings graceful empty list on Supabase failure
    (table_missing scenario).
  - update_cron_setting happy path: PATCHes only the supplied fields,
    stamps updated_at, returns the merged row.
  - update_cron_setting raises NotFoundError when row is missing.
  - update_cron_setting raises ValueError when nothing to update.
"""
from __future__ import annotations

from typing import Any

import pytest


KEY = "notif_weekly_recap"


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    """Stub get_row / list_rows / update_row so the helper can be
    exercised without touching Supabase."""
    from lambdas.common import cron_settings as cs

    state: dict[str, Any] = {
        "row": {
            "cron_key": KEY,
            "enabled": True,
            "test_mode": False,
            "description": "Weekly matchup recap — Tue 9am ET",
            "updated_at": "2026-06-01T10:00:00Z",
        },
        "get_calls": [],
        "list_calls": [],
        "update_calls": [],
        "get_raises": None,
        "list_raises": None,
        "list_response": None,  # if set, overrides default
    }

    def _get_row(table: str, column: str, value: str):
        state["get_calls"].append({"table": table, "column": column, "value": value})
        if state["get_raises"] is not None:
            raise state["get_raises"]
        row = state["row"]
        if row and row.get(column) == value:
            return row
        return None

    def _list_rows(table: str, **kwargs: Any):
        state["list_calls"].append({"table": table, **kwargs})
        if state["list_raises"] is not None:
            raise state["list_raises"]
        if state["list_response"] is not None:
            return state["list_response"]
        return ([state["row"]] if state["row"] else [], None)

    def _update_row(table: str, column: str, value: str, fields: dict[str, Any]):
        state["update_calls"].append(
            {"table": table, "column": column, "value": value, "fields": fields}
        )
        merged = dict(state["row"] or {})
        merged.update(fields)
        state["row"] = merged
        return merged

    monkeypatch.setattr(cs, "get_row", _get_row)
    monkeypatch.setattr(cs, "list_rows", _list_rows)
    monkeypatch.setattr(cs, "update_row", _update_row)

    return state


# ---------------------------------------------------------------------------
# get_cron_setting
# ---------------------------------------------------------------------------


class TestGetCronSetting:
    def test_happy_path_returns_row_fields(self, patched) -> None:
        from lambdas.common.cron_settings import get_cron_setting

        result = get_cron_setting(KEY)
        assert result == {
            "cron_key": KEY,
            "enabled": True,
            "test_mode": False,
            "description": "Weekly matchup recap — Tue 9am ET",
        }
        # Lookup uses cron_key column.
        call = patched["get_calls"][0]
        assert call["table"] == "admin_cron_settings"
        assert call["column"] == "cron_key"
        assert call["value"] == KEY

    def test_supabase_failure_returns_safe_default(self, patched) -> None:
        """Best-effort: any exception from Supabase falls back to
        enabled=True, test_mode=False so the cron still fires."""
        from lambdas.common.cron_settings import get_cron_setting

        patched["get_raises"] = Exception("network timeout")

        result = get_cron_setting(KEY)
        assert result["enabled"] is True
        assert result["test_mode"] is False
        assert result["description"] is None
        assert result["cron_key"] == KEY

    def test_missing_row_returns_safe_default(self, patched) -> None:
        """If the row simply doesn't exist (migration partial, or a
        new cron not yet seeded), same safe default applies."""
        from lambdas.common.cron_settings import get_cron_setting

        patched["row"] = None

        result = get_cron_setting(KEY)
        assert result["enabled"] is True
        assert result["test_mode"] is False
        assert result["cron_key"] == KEY

    def test_disabled_row_propagates(self, patched) -> None:
        from lambdas.common.cron_settings import get_cron_setting

        patched["row"] = {
            "cron_key": KEY,
            "enabled": False,
            "test_mode": False,
            "description": "off",
        }
        result = get_cron_setting(KEY)
        assert result["enabled"] is False

    def test_test_mode_row_propagates(self, patched) -> None:
        from lambdas.common.cron_settings import get_cron_setting

        patched["row"] = {
            "cron_key": KEY,
            "enabled": True,
            "test_mode": True,
            "description": "preview",
        }
        result = get_cron_setting(KEY)
        assert result["test_mode"] is True

    def test_never_raises_even_on_unexpected_supabase_shape(self, patched) -> None:
        """Defensive: arbitrary Exception subclasses are swallowed."""
        from lambdas.common.cron_settings import get_cron_setting

        class _Boom(RuntimeError):
            pass

        patched["get_raises"] = _Boom("kaboom")
        # Should NOT propagate.
        assert get_cron_setting(KEY)["enabled"] is True


# ---------------------------------------------------------------------------
# list_cron_settings
# ---------------------------------------------------------------------------


class TestListCronSettings:
    def test_happy_path_returns_rows(self, patched) -> None:
        from lambdas.common.cron_settings import list_cron_settings

        result = list_cron_settings()
        assert len(result) == 1
        assert result[0]["cron_key"] == KEY
        # Ordering query plumbs through.
        call = patched["list_calls"][0]
        assert call["order_by"] == "cron_key.asc"

    def test_supabase_failure_returns_empty_list(self, patched) -> None:
        """table_missing scenario — Supabase returns 404 because the
        migration hasn't been applied. Helper degrades to []."""
        from lambdas.common.cron_settings import list_cron_settings
        from lambdas.common.errors import SleeperAPIError

        patched["list_raises"] = SleeperAPIError(
            message="HTTP 404 — relation public.admin_cron_settings does not exist",
            function="supabase_helper._get",
        )

        result = list_cron_settings()
        assert result == []

    def test_arbitrary_exception_returns_empty_list(self, patched) -> None:
        from lambdas.common.cron_settings import list_cron_settings

        patched["list_raises"] = RuntimeError("network down")
        assert list_cron_settings() == []


# ---------------------------------------------------------------------------
# update_cron_setting
# ---------------------------------------------------------------------------


class TestUpdateCronSetting:
    def test_patch_enabled_only(self, patched) -> None:
        from lambdas.common.cron_settings import update_cron_setting

        result = update_cron_setting(KEY, enabled=False, test_mode=None)
        assert result["enabled"] is False
        # Only `enabled` + `updated_at` should appear in fields.
        write = patched["update_calls"][0]
        assert "enabled" in write["fields"]
        assert write["fields"]["enabled"] is False
        assert "test_mode" not in write["fields"]
        assert "updated_at" in write["fields"]

    def test_patch_test_mode_only(self, patched) -> None:
        from lambdas.common.cron_settings import update_cron_setting

        update_cron_setting(KEY, enabled=None, test_mode=True)
        write = patched["update_calls"][0]
        assert "test_mode" in write["fields"]
        assert write["fields"]["test_mode"] is True
        assert "enabled" not in write["fields"]

    def test_patch_both(self, patched) -> None:
        from lambdas.common.cron_settings import update_cron_setting

        update_cron_setting(KEY, enabled=False, test_mode=True)
        write = patched["update_calls"][0]
        assert write["fields"]["enabled"] is False
        assert write["fields"]["test_mode"] is True

    def test_missing_row_raises_not_found(self, patched) -> None:
        from lambdas.common.cron_settings import update_cron_setting
        from lambdas.common.errors import NotFoundError

        patched["row"] = None

        with pytest.raises(NotFoundError):
            update_cron_setting(KEY, enabled=True, test_mode=None)
        # No update call should have fired.
        assert patched["update_calls"] == []

    def test_nothing_to_update_raises_value_error(self, patched) -> None:
        from lambdas.common.cron_settings import update_cron_setting

        with pytest.raises(ValueError):
            update_cron_setting(KEY, enabled=None, test_mode=None)
        assert patched["update_calls"] == []
