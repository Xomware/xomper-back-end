"""
Tests for `lambdas.common.espn_crosswalk`.

Sleeper's own `espn_id` resolves only 42% of ESPN's player list, so the
crosswalk layers three more sources on top. What matters is that each layer
fires only when the one above it misses, and that the source is reported
honestly — name matching carries the largest share, and a consumer should be
able to tell a published id from a guess.
"""
from __future__ import annotations

from lambdas.common.espn_crosswalk import build_crosswalk, normalize_name

SLEEPER = {
    "100": {"full_name": "Direct Match", "position": "WR", "espn_id": 900},
    "200": {"full_name": "Calc Match", "position": "RB"},
    "300": {"full_name": "Name Match", "position": "TE"},
    "400": {"full_name": "A.J. O'Neill-Smith Jr.", "position": "WR"},
    "HOU": {"full_name": "Houston Texans", "last_name": "Texans", "position": "DEF"},
    "500": {"full_name": "Same Name", "position": "QB"},
    "501": {"full_name": "Same Name", "position": "RB"},
}

FANTASYCALC = [{"player": {"espnId": 901, "sleeperId": "200"}}]


def espn(id_, name, pos_id):
    return {"id": id_, "fullName": name, "defaultPositionId": pos_id}


class TestNormalizeName:
    def test_strips_punctuation_and_suffixes(self):
        assert normalize_name("A.J. O'Neill-Smith Jr.") == "aj oneill smith"

    def test_handles_none(self):
        assert normalize_name(None) == ""


class TestBuildCrosswalk:
    def test_prefers_sleeper_espn_id(self):
        out = build_crosswalk(SLEEPER, [espn(900, "Direct Match", 3)], FANTASYCALC)
        assert out["mapping"]["900"] == {"sleeperId": "100", "source": "sleeper_espn_id"}

    def test_falls_back_to_fantasycalc(self):
        out = build_crosswalk(SLEEPER, [espn(901, "Calc Match", 2)], FANTASYCALC)
        assert out["mapping"]["901"] == {"sleeperId": "200", "source": "fantasycalc"}

    def test_falls_back_to_name_and_position(self):
        out = build_crosswalk(SLEEPER, [espn(902, "Name Match", 4)], FANTASYCALC)
        assert out["mapping"]["902"] == {"sleeperId": "300", "source": "name_position"}

    def test_matches_defenses_on_nickname(self):
        # ESPN gives defenses a negative id that appears in no id source.
        out = build_crosswalk(SLEEPER, [espn(-16034, "Texans D/ST", 16)], FANTASYCALC)
        assert out["mapping"]["-16034"] == {"sleeperId": "HOU", "source": "def_nickname"}

    def test_normalizes_names_before_matching(self):
        out = build_crosswalk(SLEEPER, [espn(903, "AJ ONeill Smith", 3)], FANTASYCALC)
        assert out["mapping"]["903"]["sleeperId"] == "400"

    def test_disambiguates_a_shared_name_by_position(self):
        out = build_crosswalk(SLEEPER, [espn(904, "Same Name", 2)], FANTASYCALC)
        assert out["mapping"]["904"]["sleeperId"] == "501"

    def test_records_misses_rather_than_guessing(self):
        out = build_crosswalk(SLEEPER, [espn(905, "Nobody At All", 3)], FANTASYCALC)
        assert out["mapping"] == {}
        assert out["misses"] == [
            {"espnId": "905", "name": "Nobody At All", "position": "WR"}
        ]
        assert out["coverage"] == 0.0

    def test_ignores_positions_we_do_not_value(self):
        # defaultPositionId 7 is a punter; it should not count against coverage.
        out = build_crosswalk(SLEEPER, [espn(906, "Some Punter", 7)], FANTASYCALC)
        assert out["mapping"] == {}
        assert out["misses"] == []
        assert out["coverage"] == 0.0

    def test_counts_sources_and_coverage(self):
        out = build_crosswalk(
            SLEEPER,
            [
                espn(900, "Direct Match", 3),
                espn(901, "Calc Match", 2),
                espn(902, "Name Match", 4),
                espn(905, "Nobody At All", 3),
            ],
            FANTASYCALC,
        )
        assert out["sources"] == {
            "sleeper_espn_id": 1,
            "fantasycalc": 1,
            "name_position": 1,
        }
        assert out["coverage"] == 0.75

    def test_survives_malformed_fantasycalc_rows(self):
        rows = [{"player": {}}, {}, {"player": {"espnId": 901}}]
        out = build_crosswalk(SLEEPER, [espn(901, "Calc Match", 2)], rows)
        # No sleeperId on the row, so it falls through to the name layer.
        assert out["mapping"]["901"]["source"] == "name_position"
