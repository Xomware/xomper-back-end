"""
Tests for `lambdas.api_values_compute.handler`.

This endpoint was one function that fetched a Sleeper league and valued it in
the same breath. It is now a resolver plus a computation so a non-Sleeper
caller can supply the same settings shape (#110). These tests pin the response
contract so that follow-up cannot quietly change it — the refactor's whole
claim is that the body is identical.
"""
from __future__ import annotations

import importlib
import json
import sys
import types
from typing import Any
from unittest.mock import MagicMock

from urllib.error import HTTPError

import pytest

# duckdb reaches the Lambda through a layer, not requirements.txt, so it is not
# importable here. Same shim approach as tests/test_claude_helper.py takes for
# the anthropic SDK. Nothing in these tests exercises real DuckDB — _connect
# and the three warehouse_values functions are all patched out.
if "duckdb" not in sys.modules:
    _duckdb = types.ModuleType("duckdb")
    _duckdb.connect = MagicMock(name="duckdb.connect")
    _duckdb.DuckDBPyConnection = object
    sys.modules["duckdb"] = _duckdb

LEAGUE = {
    "league_id": "123",
    "season": "2025",
    "total_rosters": 12,
    "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
    "roster_positions": ["QB", "RB", "RB", "WR", "WR", "TE", "FLEX", "BN"],
}

SCORED = [("p1", "RB", 250.0), ("p2", "WR", 240.0)]
STARTERS = {"RB": 24, "WR": 24, "QB": 12, "TE": 12}
VALUES = [{"playerId": "p1", "value": 40.0}, {"playerId": "p2", "value": 30.0}]


@pytest.fixture
def mod(monkeypatch):
    m = importlib.import_module("lambdas.api_values_compute.handler")
    monkeypatch.setattr(m, "get_sleeper_league", lambda lid: LEAGUE if lid == "123" else None)
    monkeypatch.setattr(m, "get_nfl_state", lambda: {"season": "2026"})
    monkeypatch.setattr(m, "_connect", lambda: object())
    monkeypatch.setattr(m, "score_players", lambda con, uri, scoring, ppr: SCORED)
    monkeypatch.setattr(m, "starters_by_position", lambda rp, teams, scored: STARTERS)
    monkeypatch.setattr(m, "values_for", lambda con, starters: VALUES)
    return m


def _call(mod: Any, body: dict | None) -> dict:
    event = {"body": json.dumps(body)} if body is not None else {"body": None}
    return mod.handler(event, None)


class TestResolveSleeperSettings:
    def test_maps_league_onto_engine_inputs(self, mod):
        s = mod.resolve_sleeper_settings("123")
        assert s == {
            "scoring": {"rec": 1.0, "pass_td": 4.0},
            "rosterPositions": LEAGUE["roster_positions"],
            "numTeams": 12,
            "ppr": 1.0,
            "season": "2025",
        }

    def test_unknown_league_is_none(self, mod):
        assert mod.resolve_sleeper_settings("nope") is None

    def test_league_season_wins_over_nfl_state(self, mod):
        assert mod.resolve_sleeper_settings("123")["season"] == "2025"

    def test_falls_back_to_nfl_state_without_a_league_season(self, mod, monkeypatch):
        monkeypatch.setattr(mod, "get_sleeper_league", lambda lid: {**LEAGUE, "season": None})
        assert mod.resolve_sleeper_settings("123")["season"] == "2026"

    def test_defaults_when_the_league_omits_fields(self, mod, monkeypatch):
        monkeypatch.setattr(mod, "get_sleeper_league", lambda lid: {"season": "2025"})
        s = mod.resolve_sleeper_settings("123")
        assert s["numTeams"] == 12
        assert s["ppr"] == 0
        assert s["scoring"] == {}
        assert s["rosterPositions"] == []


class TestComputeValues:
    def test_returns_starters_count_and_values(self, mod):
        out = mod.compute_values(mod.resolve_sleeper_settings("123"))
        assert out == {"starters": STARTERS, "count": 2, "values": VALUES}

    def test_takes_a_settings_dict_with_no_league_id(self, mod):
        # The point of the split: nothing Sleeper-shaped reaches this function.
        out = mod.compute_values({
            "scoring": {"rec": 0.5},
            "rosterPositions": ["QB", "RB"],
            "numTeams": 10,
            "ppr": 0.5,
            "season": "2025",
        })
        assert out["count"] == 2


class TestExplicitSettings:
    BODY = {
        "scoring": {"rec": 0.5, "pass_td": 4.0},
        "rosterPositions": ["QB", "RB", "WR", "TE"],
        "numTeams": 10,
        "season": "2025",
    }

    def test_maps_a_body_onto_engine_inputs(self, mod):
        settings, error = mod.explicit_settings(self.BODY)
        assert error is None
        assert settings == {
            "scoring": {"rec": 0.5, "pass_td": 4.0},
            "rosterPositions": ["QB", "RB", "WR", "TE"],
            "numTeams": 10,
            "ppr": 0.5,
            "season": "2025",
        }

    def test_derives_ppr_the_same_way_the_sleeper_path_does(self, mod):
        explicit, _ = mod.explicit_settings({**self.BODY, "scoring": {"rec": 1.0}})
        sleeper = mod.resolve_sleeper_settings("123")
        assert explicit["ppr"] == sleeper["ppr"] == 1.0

    def test_ppr_is_zero_for_standard_scoring(self, mod):
        settings, _ = mod.explicit_settings({**self.BODY, "scoring": {"pass_td": 4.0}})
        assert settings["ppr"] == 0

    def test_names_every_missing_field(self, mod):
        settings, error = mod.explicit_settings({"scoring": {}})
        assert settings is None
        for field in ("rosterPositions", "numTeams", "season"):
            assert field in error

    def test_rejects_wrong_types(self, mod):
        for bad, expected in [
            ({"scoring": []}, "scoring must be an object"),
            ({"rosterPositions": "QB,RB"}, "rosterPositions must be an array"),
            ({"numTeams": "many"}, "numTeams must be a number"),
            ({"numTeams": 0}, "numTeams must be at least 1"),
        ]:
            settings, error = mod.explicit_settings({**self.BODY, **bad})
            assert settings is None
            assert error == expected

    def test_accepts_a_numeric_string_for_num_teams(self, mod):
        settings, error = mod.explicit_settings({**self.BODY, "numTeams": "12"})
        assert error is None
        assert settings["numTeams"] == 12


class TestHandlerContract:
    def test_response_body_is_unchanged(self, mod):
        res = _call(mod, {"leagueId": "123"})
        assert res["statusCode"] == 200
        assert json.loads(res["body"]) == {
            "leagueId": "123",
            "season": "2025",
            "numTeams": 12,
            # Added deliberately with the ESPN path. ESPN leagues are valued
            # off ESPN's projections and Sleeper leagues off the warehouse, so
            # a caller has to be able to tell which currency it received.
            "projectionSource": "warehouse",
            "starters": STARTERS,
            "count": 2,
            "values": VALUES,
        }

    def test_empty_body_is_400_naming_both_options(self, mod):
        res = _call(mod, {})
        assert res["statusCode"] == 400
        error = json.loads(res["body"])["error"]
        assert "leagueId" in error and "settings" in error

    def test_explicit_settings_value_without_a_league_id(self, mod):
        res = _call(mod, TestExplicitSettings.BODY)
        assert res["statusCode"] == 200
        body = json.loads(res["body"])
        assert body["leagueId"] is None
        assert body["season"] == "2025"
        assert body["numTeams"] == 10
        assert body["values"] == VALUES

    def test_league_id_still_wins_when_both_are_sent(self, mod):
        res = _call(mod, {"leagueId": "123", **TestExplicitSettings.BODY})
        body = json.loads(res["body"])
        # Sleeper's own settings, not the ones pasted alongside them.
        assert body["leagueId"] == "123"
        assert body["numTeams"] == 12
        assert body["season"] == "2025"

    def test_partial_explicit_settings_is_400_not_500(self, mod):
        res = _call(mod, {"scoring": {"rec": 1.0}})
        assert res["statusCode"] == 400
        assert "missing required field" in json.loads(res["body"])["error"]

    def test_unknown_league_is_404(self, mod):
        res = _call(mod, {"leagueId": "nope"})
        assert res["statusCode"] == 404

    def test_unresolvable_season_is_500(self, mod, monkeypatch):
        monkeypatch.setattr(mod, "get_sleeper_league", lambda lid: {**LEAGUE, "season": None})
        monkeypatch.setattr(mod, "get_nfl_state", lambda: {})
        res = _call(mod, {"leagueId": "123"})
        assert res["statusCode"] == 500


ESPN_SETTINGS = {
    "settings": {
        "size": 10,
        "rosterSettings": {
            "lineupSlotCounts": {"0": 1, "2": 2, "4": 2, "6": 1, "23": 1, "16": 1, "17": 1, "20": 6}
        },
    }
}


class TestEspnPath:
    """The ESPN league path, which skips scoring entirely."""

    @pytest.fixture
    def espn(self, mod, monkeypatch):
        monkeypatch.setattr(mod, "fetch_league_settings", lambda lid, season, ck=None: ESPN_SETTINGS)
        monkeypatch.setattr(
            mod, "fetch_league_players", lambda lid, season, cookies=None: [{"id": 1}]
        )
        monkeypatch.setattr(mod, "get_espn", lambda uid: None)
        monkeypatch.setattr(
            mod, "_stored_crosswalk", lambda: {"1": {"sleeperId": "p1", "source": "stored"}}
        )
        monkeypatch.setattr(
            mod, "scored_rows", lambda players, xw: {
                "rows": [("p1", "RB", 250.0)],
                "unresolved": [{"espnId": "9", "name": "Nobody", "reason": "no_crosswalk"}],
            }
        )
        monkeypatch.setattr(mod, "load_scored", lambda con, rows: rows)
        return mod

    def test_reads_roster_shape_from_espn(self, espn):
        settings = espn.resolve_espn_settings("899513", "2025")
        assert settings["numTeams"] == 10
        assert settings["rosterPositions"].count("RB") == 2
        assert settings["rosterPositions"].count("WR") == 2
        assert "FLEX" in settings["rosterPositions"]
        # Slot 20 is bench. It is not a starter and must not inflate replacement.
        assert len(settings["rosterPositions"]) == 9

    def test_espn_settings_carry_no_scoring(self, espn):
        # An ESPN league never supplies Sleeper-shaped scoring; #112 established
        # that translating its statIds cannot be done safely.
        assert "scoring" not in espn.resolve_espn_settings("899513", "2025")

    def test_values_an_espn_league(self, espn):
        res = _call(espn, {"espnLeagueId": "899513", "season": "2025"})
        assert res["statusCode"] == 200
        body = json.loads(res["body"])
        assert body["espnLeagueId"] == "899513"
        assert body["numTeams"] == 10
        assert body["values"] == VALUES

    def test_says_which_projection_source_it_used(self, espn):
        body = json.loads(_call(espn, {"espnLeagueId": "899513", "season": "2025"})["body"])
        assert body["projectionSource"] == "espn"

    def test_reports_unresolved_players(self, espn):
        body = json.loads(_call(espn, {"espnLeagueId": "899513", "season": "2025"})["body"])
        assert body["unresolved"][0]["reason"] == "no_crosswalk"

    def test_season_is_required(self, espn):
        res = _call(espn, {"espnLeagueId": "899513"})
        assert res["statusCode"] == 400
        assert "season" in json.loads(res["body"])["error"]

    def test_unknown_espn_league_is_404(self, espn, monkeypatch):
        monkeypatch.setattr(espn, "fetch_league_settings", lambda lid, season, ck=None: None)
        res = _call(espn, {"espnLeagueId": "nope", "season": "2025"})
        assert res["statusCode"] == 404

    def test_unreadable_roster_shape_is_502(self, espn, monkeypatch):
        monkeypatch.setattr(
            espn, "fetch_league_settings", lambda lid, season, ck=None: {"settings": {}}
        )
        res = _call(espn, {"espnLeagueId": "899513", "season": "2025"})
        assert res["statusCode"] == 502

    def test_espn_wins_over_a_sleeper_league_id(self, espn):
        body = json.loads(
            _call(espn, {"espnLeagueId": "899513", "season": "2025", "leagueId": "123"})["body"]
        )
        assert body["projectionSource"] == "espn"

    def test_empty_body_names_all_three_options(self, mod):
        res = _call(mod, {})
        error = json.loads(res["body"])["error"]
        assert "leagueId" in error and "espnLeagueId" in error and "settings" in error


class TestEspnPrivateLeagues:
    """Private leagues need the caller's cookies; public ones must not."""

    @pytest.fixture
    def espn(self, mod, monkeypatch):
        monkeypatch.setattr(mod, "fetch_league_settings", lambda lid, season, ck=None: ESPN_SETTINGS)
        monkeypatch.setattr(mod, "fetch_league_players", lambda lid, season, cookies=None: [{"id": 1}])
        monkeypatch.setattr(mod, "_stored_crosswalk", lambda: {})
        monkeypatch.setattr(mod, "get_espn", lambda uid: None)
        monkeypatch.setattr(
            mod, "scored_rows", lambda p, xw: {"rows": [("p1", "RB", 250.0)], "unresolved": []}
        )
        monkeypatch.setattr(mod, "load_scored", lambda con, rows: rows)
        return mod

    def _event(self, body, user_id=None):
        ev = {"body": json.dumps(body)}
        if user_id:
            ev["requestContext"] = {"authorizer": {"sub": user_id}}
        return ev

    def test_unauthenticated_callers_get_no_cookies(self, espn):
        assert espn._caller_espn_cookies({}) is None

    def test_a_caller_who_never_connected_espn_gets_none(self, espn, monkeypatch):
        monkeypatch.setattr(espn, "get_espn", lambda uid: None)
        assert espn._caller_espn_cookies(self._event({}, "user-1")) is None

    def test_cookies_are_read_for_an_authenticated_caller(self, espn, monkeypatch):
        monkeypatch.setattr(espn, "get_espn", lambda uid: {"espn_s2": "a", "SWID": "{b}"})
        assert espn._caller_espn_cookies(self._event({}, "user-1")) == {"espn_s2": "a", "SWID": "{b}"}

    def test_cookies_reach_the_upstream_calls(self, espn, monkeypatch):
        seen = {}
        monkeypatch.setattr(espn, "get_espn", lambda uid: {"espn_s2": "a", "SWID": "{b}"})
        monkeypatch.setattr(
            espn, "fetch_league_settings",
            lambda lid, season, ck=None: seen.update(settings=ck) or ESPN_SETTINGS,
        )
        monkeypatch.setattr(
            espn, "fetch_league_players",
            lambda lid, season, cookies=None: seen.update(players=cookies) or [{"id": 1}],
        )
        espn.handler(self._event({"espnLeagueId": "1", "season": "2025"}, "user-1"), None)
        assert seen["settings"] == {"espn_s2": "a", "SWID": "{b}"}
        assert seen["players"] == {"espn_s2": "a", "SWID": "{b}"}

    def test_public_league_still_works_with_no_caller(self, espn):
        res = espn.handler(self._event({"espnLeagueId": "1", "season": "2025"}), None)
        assert res["statusCode"] == 200

    def test_private_league_without_cookies_says_connect(self, espn, monkeypatch):
        def denied(lid, season, ck=None):
            raise HTTPError("u", 401, "Unauthorized", {}, None)

        monkeypatch.setattr(espn, "fetch_league_settings", denied)
        res = espn.handler(self._event({"espnLeagueId": "1", "season": "2025"}), None)
        assert res["statusCode"] == 403
        body = json.loads(res["body"])
        assert body["error"] == "espn_auth_required"
        # No stored cookies -> the user has never connected ESPN.
        assert body["hasStoredCredentials"] is False

    def test_private_league_with_stale_cookies_says_reconnect(self, espn, monkeypatch):
        def denied(lid, season, ck=None):
            raise HTTPError("u", 403, "Forbidden", {}, None)

        monkeypatch.setattr(espn, "get_espn", lambda uid: {"espn_s2": "old", "SWID": "{b}"})
        monkeypatch.setattr(espn, "fetch_league_settings", denied)
        res = espn.handler(self._event({"espnLeagueId": "1", "season": "2025"}, "user-1"), None)
        assert res["statusCode"] == 403
        # Cookies existed and were rejected -> expired, reconnect rather than connect.
        assert json.loads(res["body"])["hasStoredCredentials"] is True

    def test_other_http_errors_are_not_swallowed(self, espn, monkeypatch):
        def boom(lid, season, ck=None):
            raise HTTPError("u", 500, "Server Error", {}, None)

        monkeypatch.setattr(espn, "fetch_league_settings", boom)
        res = espn.handler(self._event({"espnLeagueId": "1", "season": "2025"}), None)
        assert res["statusCode"] != 403

    def test_cookies_never_appear_in_the_response(self, espn, monkeypatch):
        monkeypatch.setattr(espn, "get_espn", lambda uid: {"espn_s2": "SECRET", "SWID": "{b}"})
        res = espn.handler(self._event({"espnLeagueId": "1", "season": "2025"}, "user-1"), None)
        assert "SECRET" not in res["body"]
