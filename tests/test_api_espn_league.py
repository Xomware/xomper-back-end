"""
Tests for `lambdas.api_espn_league.handler` and the credential store.

This endpoint forwards requests carrying a user's full ESPN session, so the
tests that matter are the ones proving a caller cannot steer it: the host is
fixed, ids must be numeric, and `view` is allowlisted. The rest is routing.
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import pytest

from lambdas.api_espn_league import handler as mod
from lambdas.common.errors import ValidationError

CALLER = {"requestContext": {"authorizer": {"sub": "user-1"}}}


def event(path: str, method: str, params: dict | None = None, body: dict | None = None) -> dict:
    return {
        **CALLER,
        "path": f"/espn/{path}",
        "httpMethod": method,
        "queryStringParameters": params,
        "body": json.dumps(body) if body is not None else None,
    }


class TestInputGuards:
    @pytest.mark.parametrize("bad", ["", None, "abc", "12; DROP", "../../etc"])
    def test_league_id_must_be_numeric(self, bad):
        with pytest.raises(ValidationError):
            mod._require_digits(bad, "leagueId")

    def test_numeric_ids_pass(self):
        assert mod._require_digits("899513", "leagueId") == "899513"

    def test_view_must_be_allowlisted(self):
        with pytest.raises(ValidationError) as err:
            mod._views("mDraftDetail,mSecretInternalThing")
        assert "mSecretInternalThing" in str(err.value)

    def test_view_is_required(self):
        with pytest.raises(ValidationError):
            mod._views(None)

    def test_multiple_allowed_views(self):
        assert mod._views("mDraftDetail,mSettings") == ["mDraftDetail", "mSettings"]

    def test_accepts_a_list_of_views(self):
        assert mod._views(["mSettings"]) == ["mSettings"]


class TestUrlConstruction:
    def test_targets_only_the_espn_reads_host(self):
        seen: dict[str, Any] = {}

        def fake_urlopen(request, timeout=None):
            seen["url"] = request.full_url
            seen["cookie"] = request.headers.get("Cookie")
            raise RuntimeError("stop after url")

        with patch("urllib.request.urlopen", fake_urlopen):
            with pytest.raises(RuntimeError):
                mod._fetch("899513", "2025", ["mDraftDetail"], None)

        assert seen["url"].startswith(
            "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/2025"
            "/segments/0/leagues/899513?"
        )
        assert seen["cookie"] is None

    def test_sends_cookies_when_the_user_has_them(self):
        seen: dict[str, Any] = {}

        def fake_urlopen(request, timeout=None):
            seen["cookie"] = request.headers.get("Cookie")
            raise RuntimeError("stop")

        with patch("urllib.request.urlopen", fake_urlopen):
            with pytest.raises(RuntimeError):
                mod._fetch("1", "2025", ["mSettings"], {"espn_s2": "abc", "SWID": "{x}"})

        assert seen["cookie"] == "espn_s2=abc; SWID={x}"


class TestRouting:
    def test_methods_do_not_share_a_path_part(self):
        # api-gateway-service keys one resource per path_part, so PUT and
        # DELETE on a shared part cannot both be wired.
        parts = [part for part, _ in mod._ROUTES]
        assert len(parts) == len(set(parts))

    def test_unknown_route_is_rejected(self):
        # handle_errors turns a ValidationError into a 400 rather than raising.
        res = mod.handler(event("league", "POST"), None)
        assert res["statusCode"] == 400
        assert "unsupported route" in json.loads(res["body"])["error"]["message"]

    def test_credentials_require_both_values(self):
        with patch.object(mod, "store_espn") as store:
            with pytest.raises(ValidationError):
                mod._put_credentials(event("connect", "PUT", body={"espn_s2": "a"}))
            store.assert_not_called()

    def test_storing_credentials_reports_connected(self):
        with patch.object(mod, "store_espn") as store:
            res = mod._put_credentials(
                event("connect", "PUT", body={"espn_s2": "a", "swid": "{b}"})
            )
        store.assert_called_once_with("user-1", "a", "{b}")
        assert json.loads(res["body"])["connected"] is True

    def test_deleting_credentials_reports_disconnected(self):
        with patch.object(mod, "clear_espn") as clear:
            res = mod._delete_credentials(event("disconnect", "DELETE"))
        clear.assert_called_once_with("user-1")
        assert json.loads(res["body"])["connected"] is False


class TestRedaction:
    def test_espn_cookies_never_survive_a_log_line(self):
        from lambdas.common.log_redact import redact

        line = (
            "Cookie: espn_s2=AEB%2Fabcdef123456789longvalue; "
            "SWID={1A2B3C4D-5E6F-7081-92A3-B4C5D6E7F809}"
        )
        out = redact(line)
        assert "AEB%2Fabcdef" not in out
        assert "1A2B3C4D" not in out
        assert "espn_s2=[cookie]" in out and "[swid]" in out
