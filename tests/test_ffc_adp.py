"""
Tests for `lambdas.common.ffc_adp` and the /adp/current endpoint.

ADP is served as context, not prediction — the calibrated survival model was
cut for having no held-out skill. So what matters here is that the sample
window travels with the numbers, that an unsupported format is refused by name
rather than quietly served PPR, and that one dead format does not cost the
whole snapshot.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from lambdas.common import ffc_adp

RAW = {
    "status": "Success",
    "meta": {
        "type": "PPR",
        "teams": 12,
        "rounds": 15,
        "total_drafts": 8470,
        "start_date": "2025-08-25",
        "end_date": "2025-09-01",
    },
    "players": [
        {
            "player_id": 5177,
            "name": "Ja'Marr Chase",
            "position": "WR",
            "team": "CIN",
            "adp": 1.5,
            "stdev": 0.8,
            "high": 1,
            "low": 5,
            "times_drafted": 1800,
            "bye": 6,
            "adp_formatted": "1.02",
        }
    ],
}


class TestFormats:
    def test_only_formats_ffc_actually_serves(self):
        assert set(ffc_adp.FORMATS) == {
            "standard", "ppr", "half_ppr", "superflex", "dynasty", "rookie",
        }

    def test_superflex_maps_to_ffc_2qb(self):
        # FFC has no `superflex` endpoint; it 400s. 2qb is the same thing.
        assert ffc_adp.FORMATS["superflex"] == "2qb"

    def test_no_te_premium(self):
        # There is no TE-premium ADP upstream. Absent beats silently wrong.
        assert "te_premium" not in ffc_adp.FORMATS

    def test_url_carries_no_team_count(self):
        # The teams parameter is a no-op upstream: teams=8 and teams=14 return
        # identical data. Sending it would imply a precision that is not there.
        url = ffc_adp.BASE_URL.format(fmt="ppr", season="2025")
        assert "teams" not in url


class TestNormalize:
    def test_keeps_the_sample_window_with_the_numbers(self):
        out = ffc_adp.normalize(RAW)
        assert out["sampleStart"] == "2025-08-25"
        assert out["sampleEnd"] == "2025-09-01"
        assert out["totalDrafts"] == 8470

    def test_keeps_spread_fields(self):
        player = ffc_adp.normalize(RAW)["players"][0]
        assert player["adp"] == 1.5
        assert player["stdev"] == 0.8
        assert player["high"] == 1 and player["low"] == 5

    def test_drops_fields_we_do_not_serve(self):
        assert "adp_formatted" not in ffc_adp.normalize(RAW)["players"][0]

    def test_survives_an_empty_payload(self):
        out = ffc_adp.normalize({})
        assert out["players"] == [] and out["type"] is None


class TestFetchAll:
    def test_one_dead_format_does_not_lose_the_rest(self):
        def fake(fmt, season):
            if fmt == "dynasty":
                raise TimeoutError("upstream down")
            return RAW

        with patch.object(ffc_adp, "fetch_format", fake):
            out = ffc_adp.fetch_all("2025")

        assert "dynasty" not in out["formats"]
        assert "dynasty" in out["failed"]
        assert "TimeoutError" in out["failed"]["dynasty"]
        assert len(out["formats"]) == len(ffc_adp.FORMATS) - 1


class TestEndpoint:
    SNAPSHOT = {
        "season": "2025",
        "capturedAt": "2026-08-29T08:00:00+00:00",
        "formats": {"ppr": {"type": "PPR", "players": [], "sampleEnd": "2025-09-01"}},
        "failed": {},
    }

    @pytest.fixture
    def mod(self, monkeypatch):
        m = __import__(
            "lambdas.api_adp_current.handler", fromlist=["handler"]
        )
        monkeypatch.setattr(m, "_load", lambda: self.SNAPSHOT)
        return m

    def test_returns_the_whole_snapshot_by_default(self, mod):
        res = mod.handler({}, None)
        assert res["statusCode"] == 200
        assert json.loads(res["body"])["season"] == "2025"

    def test_returns_one_format(self, mod):
        res = mod.handler({"queryStringParameters": {"format": "ppr"}}, None)
        body = json.loads(res["body"])
        assert body["format"] == "ppr"
        assert body["capturedAt"] == self.SNAPSHOT["capturedAt"]

    def test_unsupported_format_is_refused_by_name(self, mod):
        res = mod.handler({"queryStringParameters": {"format": "te_premium"}}, None)
        assert res["statusCode"] == 400
        message = json.loads(res["body"])["error"]["message"]
        assert "te_premium" in message and "ppr" in message

    def test_missing_snapshot_for_a_supported_format_is_404(self, mod):
        res = mod.handler({"queryStringParameters": {"format": "rookie"}}, None)
        assert res["statusCode"] == 404
