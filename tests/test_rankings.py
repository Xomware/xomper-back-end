"""
Tests for multi-source consensus rankings.

The consensus number is the least interesting output. `spread` is the reason to
pull several lists at all — a player the sources rank 12, 14 and 41 is a
decision, and an average alone hides that.
"""
from lambdas.common.rankings import (
    adp_ranks,
    consensus,
    espn_ranks,
    fantasycalc_ranks,
    norm_name,
)

BY_NAME = {
    ("jamarr chase", "WR"): "p1",
    ("bijan robinson", "RB"): "p2",
    ("marvin harrison", "WR"): "p3",
    ("steelers", "DEF"): "pdef",
}
BY_ESPN = {"4362628": "p1"}


class TestNormName:
    def test_strips_punctuation(self):
        assert norm_name("Ja'Marr Chase") == "jamarr chase"

    def test_strips_generational_suffix(self):
        # Sources disagree on Jr. and the difference is not real.
        assert norm_name("Marvin Harrison Jr.") == "marvin harrison"

    def test_handles_missing(self):
        assert norm_name(None) == ""


class TestFantasyCalc:
    def test_maps_overall_rank(self):
        rows = [{"player": {"name": "Bijan Robinson", "position": "RB"}, "overallRank": 4}]
        assert fantasycalc_ranks(rows, BY_NAME) == {"p2": 4}

    def test_skips_players_it_cannot_resolve(self):
        rows = [{"player": {"name": "Nobody At All", "position": "WR"}, "overallRank": 9}]
        assert fantasycalc_ranks(rows, BY_NAME) == {}

    def test_skips_rows_with_no_rank(self):
        rows = [{"player": {"name": "Bijan Robinson", "position": "RB"}}]
        assert fantasycalc_ranks(rows, BY_NAME) == {}


class TestEspn:
    def test_prefers_espn_id(self):
        rows = [{"id": 4362628, "player": {"fullName": "Wrong Name",
                 "draftRanksByRankType": {"PPR": {"rank": 1}}}}]
        assert espn_ranks(rows, BY_ESPN, BY_NAME) == {"p1": 1}

    def test_falls_back_to_name(self):
        # espn_id resolves only about a third of the list on its own.
        rows = [{"id": 999999, "player": {"fullName": "Bijan Robinson",
                 "eligibleSlots": [2, 23], "draftRanksByRankType": {"PPR": {"rank": 3}}}}]
        assert espn_ranks(rows, BY_ESPN, BY_NAME) == {"p2": 3}

    def test_resolves_a_defense(self):
        # ESPN writes "Steelers D/ST"; Sleeper does not.
        rows = [{"id": 1, "player": {"fullName": "Steelers D/ST", "eligibleSlots": [16],
                 "draftRanksByRankType": {"PPR": {"rank": 140}}}}]
        assert espn_ranks(rows, BY_ESPN, BY_NAME) == {"pdef": 140}

    def test_honours_rank_type(self):
        rows = [{"id": 4362628, "player": {"fullName": "x", "draftRanksByRankType": {
            "PPR": {"rank": 1}, "STANDARD": {"rank": 6}}}}]
        assert espn_ranks(rows, BY_ESPN, BY_NAME, "STANDARD") == {"p1": 6}


class TestAdpRanks:
    def test_converts_adp_to_ordinal(self):
        # ADP is a pick number; the others are ordinals. Averaging raw ADP would
        # let a 12-team draft outvote a 300-long list on scale alone.
        players = [
            {"name": "Bijan Robinson", "position": "RB", "adp": 2.4},
            {"name": "Ja'Marr Chase", "position": "WR", "adp": 1.2},
        ]
        assert adp_ranks(players, BY_NAME) == {"p1": 1, "p2": 2}

    def test_ignores_players_with_no_adp(self):
        players = [{"name": "Bijan Robinson", "position": "RB", "adp": None}]
        assert adp_ranks(players, BY_NAME) == {}


class TestConsensus:
    def test_averages_and_measures_disagreement(self):
        out = consensus({"ffc": {"p1": 12}, "espn": {"p1": 14}, "fantasycalc": {"p1": 41}})
        assert out["p1"]["consensus"] == 22.3
        # The whole reason for pulling several lists.
        assert out["p1"]["spread"] > 13
        assert out["p1"]["sourceCount"] == 3

    def test_agreement_shows_as_no_spread(self):
        out = consensus({"ffc": {"p1": 5}, "espn": {"p1": 5}})
        assert out["p1"]["spread"] == 0.0
        assert out["p1"]["sourceCount"] == 2

    def test_one_source_is_not_agreement(self):
        # sourceCount lets a caller tell "everyone agrees" from "one opinion".
        out = consensus({"ffc": {"p1": 5}})
        assert out["p1"]["spread"] == 0.0
        assert out["p1"]["sourceCount"] == 1

    def test_keeps_every_source_rank(self):
        out = consensus({"ffc": {"p1": 12}, "espn": {"p1": 14}})
        assert out["p1"]["ranks"] == {"ffc": 12, "espn": 14}

    def test_unions_players_across_sources(self):
        out = consensus({"ffc": {"p1": 1}, "espn": {"p2": 2}})
        assert set(out) == {"p1", "p2"}
