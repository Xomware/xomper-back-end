"""
Tests for `lambdas.common.announcements_store` (announcements #100).

Covers:
  - list_active: filter (is_active=true AND expires_at > now() OR null).
  - list_active: sort order — critical-first, then display_order asc,
    then created_at desc.
  - list_active: best-effort `[]` on Supabase failure (table_missing).
  - list_active: unparseable expires_at falls through (treats as active).
  - list_all: returns every row including inactive + expired.
  - list_all: best-effort `[]` on Supabase failure.
  - create: insert_row called with the expected payload + returns row.
  - update: get_row + update_row called with the right args, stamps
    updated_at, returns merged row.
  - update: raises NotFoundError when row missing.
  - update: raises ValueError on empty `fields`.
  - update: raises ValueError on unknown allowlist key.
  - delete: soft-deletes via update is_active=false; raises NotFoundError
    when row missing.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest


ANNOUNCEMENT_ID = "00000000-0000-0000-0000-000000000001"


def _row(
    id_: str = ANNOUNCEMENT_ID,
    *,
    title: str = "Test announcement",
    body: str = "Test body",
    priority: str = "info",
    expires_at: str | None = None,
    is_active: bool = True,
    display_order: int = 0,
    created_at: str = "2026-05-01T12:00:00+00:00",
) -> dict[str, Any]:
    return {
        "id": id_,
        "title": title,
        "body": body,
        "priority": priority,
        "expires_at": expires_at,
        "is_active": is_active,
        "display_order": display_order,
        "created_at": created_at,
        "updated_at": created_at,
    }


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    """Stub out the supabase_helper seams used by announcements_store."""
    from lambdas.common import announcements_store as store

    state: dict[str, Any] = {
        "rows": [],
        "get_calls": [],
        "list_calls": [],
        "insert_calls": [],
        "update_calls": [],
        "list_raises": None,
        "get_raises": None,
        "insert_response": None,
    }

    def _list_rows(table: str, **kwargs: Any):
        state["list_calls"].append({"table": table, **kwargs})
        if state["list_raises"] is not None:
            raise state["list_raises"]
        filters = kwargs.get("filters") or {}
        rows = list(state["rows"])
        if filters.get("is_active") == "true":
            rows = [r for r in rows if r.get("is_active")]
        return (rows, None)

    def _get_row(table: str, column: str, value: str):
        state["get_calls"].append({"table": table, "column": column, "value": value})
        if state["get_raises"] is not None:
            raise state["get_raises"]
        for row in state["rows"]:
            if row.get(column) == value:
                return row
        return None

    def _insert_row(table: str, row: dict[str, Any]):
        state["insert_calls"].append({"table": table, "row": row})
        if state["insert_response"] is not None:
            inserted = state["insert_response"]
        else:
            inserted = {
                "id": ANNOUNCEMENT_ID,
                **row,
                "created_at": "2026-06-01T12:00:00+00:00",
                "updated_at": "2026-06-01T12:00:00+00:00",
            }
        state["rows"].append(inserted)
        return inserted

    def _update_row(table: str, column: str, value: str, fields: dict[str, Any]):
        state["update_calls"].append(
            {"table": table, "column": column, "value": value, "fields": fields}
        )
        for idx, row in enumerate(state["rows"]):
            if row.get(column) == value:
                merged = dict(row)
                merged.update(fields)
                state["rows"][idx] = merged
                return merged
        return {}

    monkeypatch.setattr(store, "list_rows", _list_rows)
    monkeypatch.setattr(store, "get_row", _get_row)
    monkeypatch.setattr(store, "insert_row", _insert_row)
    monkeypatch.setattr(store, "update_row", _update_row)
    return state


# ---------------------------------------------------------------------------
# list_active
# ---------------------------------------------------------------------------


class TestListActive:
    def test_filters_inactive_rows(self, patched) -> None:
        from lambdas.common.announcements_store import list_active

        patched["rows"] = [
            _row("a", is_active=True),
            _row("b", is_active=False),
            _row("c", is_active=True),
        ]
        result = list_active()
        ids = [r["id"] for r in result]
        assert "b" not in ids
        assert set(ids) == {"a", "c"}

    def test_filters_expired_rows(self, patched) -> None:
        from lambdas.common.announcements_store import list_active

        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        patched["rows"] = [
            _row("future", expires_at=future, is_active=True),
            _row("expired", expires_at=past, is_active=True),
            _row("no_expiry", expires_at=None, is_active=True),
        ]
        result = list_active()
        ids = [r["id"] for r in result]
        assert "expired" not in ids
        assert set(ids) == {"future", "no_expiry"}

    def test_critical_sorts_before_info(self, patched) -> None:
        from lambdas.common.announcements_store import list_active

        patched["rows"] = [
            _row("info1", priority="info", display_order=0),
            _row("crit1", priority="critical", display_order=5),
            _row("info2", priority="info", display_order=2),
        ]
        result = list_active()
        # critical must come first regardless of display_order.
        assert result[0]["id"] == "crit1"
        # Among the two info rows, lower display_order wins.
        assert result[1]["id"] == "info1"
        assert result[2]["id"] == "info2"

    def test_display_order_ascending_within_priority(self, patched) -> None:
        from lambdas.common.announcements_store import list_active

        patched["rows"] = [
            _row("a", priority="info", display_order=2),
            _row("b", priority="info", display_order=0),
            _row("c", priority="info", display_order=1),
        ]
        result = list_active()
        assert [r["id"] for r in result] == ["b", "c", "a"]

    def test_created_at_desc_within_same_order(self, patched) -> None:
        """When priority + display_order tie, newer rows win."""
        from lambdas.common.announcements_store import list_active

        patched["rows"] = [
            _row(
                "older",
                priority="info",
                display_order=0,
                created_at="2026-01-01T00:00:00+00:00",
            ),
            _row(
                "newer",
                priority="info",
                display_order=0,
                created_at="2026-05-01T00:00:00+00:00",
            ),
        ]
        result = list_active()
        assert [r["id"] for r in result] == ["newer", "older"]

    def test_supabase_failure_returns_empty(self, patched) -> None:
        """Best-effort: any exception from Supabase falls back to []
        so the iOS Landing page can use its hardcoded fallback."""
        from lambdas.common.announcements_store import list_active

        patched["list_raises"] = RuntimeError("network down")
        assert list_active() == []

    def test_unparseable_expires_at_treats_as_active(self, patched) -> None:
        """Defensive: if an expires_at string can't be parsed, the row
        should still appear (better to over-show than to silently hide
        on a data bug)."""
        from lambdas.common.announcements_store import list_active

        patched["rows"] = [_row("a", expires_at="not-a-date", is_active=True)]
        assert len(list_active()) == 1

    def test_z_suffix_iso_parses(self, patched) -> None:
        """Sanity: `Z`-suffixed ISO timestamps (PostgREST default) parse
        on the lambda's Python 3.10 runtime."""
        from lambdas.common.announcements_store import list_active

        future = (
            datetime.now(timezone.utc) + timedelta(days=30)
        ).isoformat().replace("+00:00", "Z")
        past = (
            datetime.now(timezone.utc) - timedelta(days=1)
        ).isoformat().replace("+00:00", "Z")
        patched["rows"] = [
            _row("future", expires_at=future, is_active=True),
            _row("expired", expires_at=past, is_active=True),
        ]
        result = list_active()
        assert [r["id"] for r in result] == ["future"]


# ---------------------------------------------------------------------------
# list_all
# ---------------------------------------------------------------------------


class TestListAll:
    def test_returns_inactive_and_expired_rows(self, patched) -> None:
        from lambdas.common.announcements_store import list_all

        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        patched["rows"] = [
            _row("active", is_active=True),
            _row("inactive", is_active=False),
            _row("expired", is_active=True, expires_at=past),
        ]
        result = list_all()
        ids = {r["id"] for r in result}
        assert ids == {"active", "inactive", "expired"}

    def test_supabase_failure_returns_empty(self, patched) -> None:
        from lambdas.common.announcements_store import list_all

        patched["list_raises"] = RuntimeError("HTTP 404 — relation does not exist")
        assert list_all() == []

    def test_order_by_display_order_asc(self, patched) -> None:
        from lambdas.common.announcements_store import list_all

        patched["rows"] = [
            _row("a", display_order=2),
            _row("b", display_order=0),
            _row("c", display_order=1),
        ]
        result = list_all()
        assert [r["id"] for r in result] == ["b", "c", "a"]


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


class TestCreate:
    def test_inserts_row_with_provided_fields(self, patched) -> None:
        from lambdas.common.announcements_store import create

        result = create(
            title="New",
            body="Body",
            priority="critical",
            expires_at="2026-07-07T00:00:00Z",
            is_active=True,
            display_order=3,
        )
        assert result["title"] == "New"
        assert result["priority"] == "critical"
        assert result["display_order"] == 3
        assert result["id"] == ANNOUNCEMENT_ID

        # Verify the call shape.
        assert len(patched["insert_calls"]) == 1
        payload = patched["insert_calls"][0]["row"]
        assert payload["title"] == "New"
        assert payload["body"] == "Body"
        assert payload["priority"] == "critical"
        assert payload["expires_at"] == "2026-07-07T00:00:00Z"
        assert payload["is_active"] is True
        assert payload["display_order"] == 3

    def test_defaults_priority_info_and_is_active_true(self, patched) -> None:
        from lambdas.common.announcements_store import create

        create(title="t", body="b")
        payload = patched["insert_calls"][0]["row"]
        assert payload["priority"] == "info"
        assert payload["is_active"] is True
        assert payload["display_order"] == 0
        assert payload["expires_at"] is None


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


class TestUpdate:
    def test_happy_path_merges_fields_and_stamps_updated_at(self, patched) -> None:
        from lambdas.common.announcements_store import update

        patched["rows"] = [_row(ANNOUNCEMENT_ID, title="Old", body="b")]
        result = update(ANNOUNCEMENT_ID, {"title": "New"})
        assert result["title"] == "New"

        write = patched["update_calls"][0]
        assert write["column"] == "id"
        assert write["value"] == ANNOUNCEMENT_ID
        assert write["fields"]["title"] == "New"
        # Stamp written.
        assert "updated_at" in write["fields"]

    def test_missing_row_raises_not_found(self, patched) -> None:
        from lambdas.common.announcements_store import update
        from lambdas.common.errors import NotFoundError

        patched["rows"] = []
        with pytest.raises(NotFoundError):
            update(ANNOUNCEMENT_ID, {"title": "X"})
        assert patched["update_calls"] == []

    def test_empty_fields_raises_value_error(self, patched) -> None:
        from lambdas.common.announcements_store import update

        with pytest.raises(ValueError):
            update(ANNOUNCEMENT_ID, {})
        assert patched["update_calls"] == []

    def test_unknown_field_raises_value_error(self, patched) -> None:
        from lambdas.common.announcements_store import update

        patched["rows"] = [_row(ANNOUNCEMENT_ID)]
        with pytest.raises(ValueError) as exc_info:
            update(ANNOUNCEMENT_ID, {"unknown_col": 1})
        assert "unknown_col" in str(exc_info.value)
        assert patched["update_calls"] == []

    def test_multi_field_update(self, patched) -> None:
        from lambdas.common.announcements_store import update

        patched["rows"] = [_row(ANNOUNCEMENT_ID)]
        result = update(
            ANNOUNCEMENT_ID,
            {"title": "New", "is_active": False, "display_order": 7},
        )
        assert result["title"] == "New"
        assert result["is_active"] is False
        assert result["display_order"] == 7


# ---------------------------------------------------------------------------
# delete (soft)
# ---------------------------------------------------------------------------


class TestDelete:
    def test_soft_delete_sets_is_active_false(self, patched) -> None:
        from lambdas.common.announcements_store import delete

        patched["rows"] = [_row(ANNOUNCEMENT_ID, is_active=True)]
        result = delete(ANNOUNCEMENT_ID)
        assert result["is_active"] is False

        # Verify update_row payload.
        write = patched["update_calls"][0]
        assert write["fields"]["is_active"] is False

    def test_missing_row_raises_not_found(self, patched) -> None:
        from lambdas.common.announcements_store import delete
        from lambdas.common.errors import NotFoundError

        patched["rows"] = []
        with pytest.raises(NotFoundError):
            delete(ANNOUNCEMENT_ID)
        assert patched["update_calls"] == []
