"""
Tests for `lambdas.common.espn_projections`.

ESPN scores its own projections under the league's settings, so the work here
is picking the right stat entry out of several and reporting anything that
cannot be turned into a row. Silence is the failure mode that matters: a
dropped player is a hole in the draft board with nothing pointing at it.
"""
from __future__ import annotations

from lambdas.common.espn_projections import applied_total, scored_rows

CROSSWALK = {
    "900": {"sleeperId": "s900", "source": "sleeper_espn_id"},
    "901": {"sleeperId": "s901", "source": "name_position"},
    "902": {"sleeperId": "s900", "source": "name_position"},
}


def player(id_, pos_id, stats, name="Someone"):
    return {"id": id_, "fullName": name, "defaultPositionId": pos_id, "stats": stats}


PROJECTION = {"statSourceId": 1, "statSplitTypeId": 0, "appliedTotal": 295.25}
ACTUAL = {"statSourceId": 0, "statSplitTypeId": 0, "appliedTotal": 379.35}
WEEKLY = {"statSourceId": 1, "statSplitTypeId": 2, "appliedTotal": 20.15}


class TestAppliedTotal:
    def test_takes_the_season_projection(self):
        assert applied_total(player(900, 2, [ACTUAL, WEEKLY, PROJECTION])) == 295.25

    def test_ignores_actuals(self):
        # statSourceId 0 is what the player already did, not what he will do.
        assert applied_total(player(900, 2, [ACTUAL])) is None

    def test_ignores_weekly_splits(self):
        assert applied_total(player(900, 2, [WEEKLY])) is None

    def test_missing_stats_is_none(self):
        assert applied_total(player(900, 2, [])) is None
        assert applied_total({"id": 900}) is None


class TestScoredRows:
    def test_builds_rows_through_the_crosswalk(self):
        out = scored_rows([player(900, 2, [PROJECTION])], CROSSWALK)
        assert out["rows"] == [("s900", "RB", 295.25)]
        assert out["unresolved"] == []

    def test_unwraps_the_player_envelope(self):
        # The league endpoint nests each player under a "player" key.
        out = scored_rows([{"player": player(900, 2, [PROJECTION])}], CROSSWALK)
        assert out["rows"] == [("s900", "RB", 295.25)]

    def test_reports_a_player_the_crosswalk_cannot_resolve(self):
        out = scored_rows([player(999, 3, [PROJECTION], "Nobody")], CROSSWALK)
        assert out["rows"] == []
        assert out["unresolved"] == [
            {"espnId": "999", "name": "Nobody", "reason": "no_crosswalk"}
        ]

    def test_reports_a_player_with_no_projection(self):
        out = scored_rows([player(900, 2, [ACTUAL])], CROSSWALK)
        assert out["rows"] == []
        assert out["unresolved"][0]["reason"] == "no_projection"

    def test_skips_positions_we_do_not_value(self):
        out = scored_rows([player(900, 7, [PROJECTION])], CROSSWALK)
        assert out["rows"] == [] and out["unresolved"] == []

    def test_keeps_one_row_per_sleeper_player(self):
        # 900 and 902 both crosswalk to s900; counting both would rank him twice.
        out = scored_rows(
            [player(900, 2, [PROJECTION]), player(902, 2, [PROJECTION])], CROSSWALK
        )
        assert out["rows"] == [("s900", "RB", 295.25)]

    def test_keeps_a_zero_projection(self):
        # 0.0 is a real projection. Treating it as missing would drop every
        # player ESPN expects nothing from, which is information.
        zero = {"statSourceId": 1, "statSplitTypeId": 0, "appliedTotal": 0.0}
        out = scored_rows([player(901, 4, [zero])], CROSSWALK)
        assert out["rows"] == [("s901", "TE", 0.0)]
