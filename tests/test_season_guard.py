"""
Tests for `lambdas.common.season_guard`.

Locks the offseason suppression that keeps the four game-dependent
scheduled notifs (weekly recap, close-game, lineup-not-set, world-cup
movement) from firing year-round. Regression target: a "Week 1 recap"
that went out in June because no guard existed.
"""
from __future__ import annotations

import pytest

from lambdas.common.season_guard import is_offseason, offseason_skip


class TestIsOffseason:
    @pytest.mark.parametrize("season_type", ["regular", "post", "REGULAR", "Post"])
    def test_in_season_is_false(self, season_type):
        assert is_offseason({"season_type": season_type}) is False

    @pytest.mark.parametrize("season_type", ["off", "pre", "OFF", "Pre"])
    def test_out_of_season_is_true(self, season_type):
        assert is_offseason({"season_type": season_type}) is True

    def test_blank_or_unknown_is_false(self):
        # Best-effort: never suppress on a transient/blank read.
        assert is_offseason({}) is False
        assert is_offseason({"season_type": ""}) is False
        assert is_offseason({"season_type": None}) is False


class TestOffseasonSkip:
    def test_in_season_returns_none(self):
        assert offseason_skip({"season_type": "regular"}, "notif_weekly_recap") is None

    def test_offseason_returns_skip_response(self):
        resp = offseason_skip({"season_type": "off"}, "notif_weekly_recap")
        assert resp is not None
        # is_api=False → body is the raw dict, not a JSON string.
        body = resp["body"]
        assert body["skipped"] is True
        assert body["reason"] == "offseason"
        assert body["season_type"] == "off"

    def test_force_bypasses_guard(self):
        # Manual/backfill invoke with an explicit week → never suppressed.
        assert offseason_skip({"season_type": "off"}, "notif_weekly_recap", force=True) is None

    def test_blank_season_type_does_not_skip(self):
        assert offseason_skip({}, "notif_weekly_recap") is None
