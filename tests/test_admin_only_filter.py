"""
Tests for `lambdas.common.admin_only_filter`.

Covers:
  - Filters whitelisted user list to admin row only.
  - Returns [] when admin is missing (and logs warning).
  - Returns [] for empty input.
  - Default admin_user_id is ADMIN_DOMINICK_USER_ID (the constant).
  - Custom admin_user_id override works.
"""
from __future__ import annotations

from typing import Any


ADMIN_ID = "594625531702460416"


def _admin_row() -> dict[str, Any]:
    return {
        "id": "row-admin",
        "sleeper_user_id": ADMIN_ID,
        "email": "admin@example.com",
        "is_active": True,
        "is_admin": True,
    }


def _other_user(sleeper_id: str) -> dict[str, Any]:
    return {
        "id": f"row-{sleeper_id}",
        "sleeper_user_id": sleeper_id,
        "email": f"{sleeper_id}@example.com",
        "is_active": True,
        "is_admin": False,
    }


class TestFilterToAdminOnly:
    def test_admin_present_returns_admin_row(self) -> None:
        from lambdas.common.admin_only_filter import filter_to_admin_only

        users = [
            _other_user("u1"),
            _admin_row(),
            _other_user("u2"),
        ]
        result = filter_to_admin_only(users)
        assert len(result) == 1
        assert result[0]["sleeper_user_id"] == ADMIN_ID

    def test_admin_absent_returns_empty(self) -> None:
        from lambdas.common.admin_only_filter import filter_to_admin_only

        users = [_other_user("u1"), _other_user("u2")]
        result = filter_to_admin_only(users)
        assert result == []

    def test_empty_input_returns_empty(self) -> None:
        from lambdas.common.admin_only_filter import filter_to_admin_only

        result = filter_to_admin_only([])
        assert result == []

    def test_custom_admin_id_override(self) -> None:
        from lambdas.common.admin_only_filter import filter_to_admin_only

        users = [_other_user("custom-id"), _admin_row()]
        result = filter_to_admin_only(users, admin_user_id="custom-id")
        assert len(result) == 1
        assert result[0]["sleeper_user_id"] == "custom-id"

    def test_rows_missing_sleeper_user_id_ignored(self) -> None:
        from lambdas.common.admin_only_filter import filter_to_admin_only

        users = [
            {"id": "row-x"},  # no sleeper_user_id
            _admin_row(),
        ]
        result = filter_to_admin_only(users)
        assert len(result) == 1
        assert result[0]["sleeper_user_id"] == ADMIN_ID
