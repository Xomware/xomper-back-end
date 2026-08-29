"""
Fantasy Football Calculator ADP.

ADP ships as displayed context, not as a prediction. The calibrated
"will he last until my next pick" model was cut after replaying three real
drafts showed no held-out skill — see
docs/features/fantasy-draft-helper/SPIKE-adp-calibration.md. What survives is
"ADP 47, you pick again at 52", with the reader doing the inference.

Two measured facts about the upstream API shape this module:

- Only these formats exist. `superflex`, `te-premium`, `tep`, `dynasty-ppr`
  and `best-ball` all return 400. Superflex leagues use `2qb`; TE-premium has
  no ADP at all and must be told so rather than served PPR silently.
- The `teams` parameter is a no-op. `teams=8` and `teams=14` return byte
  identical ADP for all 249 players and `teams=16` 400s. There is one dataset
  per scoring format, not per league size, so nothing here accepts a team
  count and no caller should imply one.
"""
import json
import urllib.request
from typing import Any

BASE_URL = "https://fantasyfootballcalculator.com/api/v1/adp/{fmt}?year={season}"
USER_AGENT = "xomper-warehouse-ingest/1.0"

# Verified 2026-08-28. Keys are ours, values are FFC's path segment.
FORMATS = {
    "standard": "standard",
    "ppr": "ppr",
    "half_ppr": "half-ppr",
    "superflex": "2qb",
    "dynasty": "dynasty",
    "rookie": "rookie",
}

# Fields worth keeping. `stdev`, `high` and `low` describe the spread rather
# than a point estimate, which is the honest way to show ADP now that the
# probability model is gone.
FIELDS = ("name", "position", "team", "adp", "stdev", "high", "low", "times_drafted", "bye")


def fetch_format(fmt: str, season: str) -> dict[str, Any]:
    """One scoring format's ADP. `fmt` is a key of FORMATS."""
    url = BASE_URL.format(fmt=FORMATS[fmt], season=season)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def normalize(payload: dict[str, Any]) -> dict[str, Any]:
    """Trim one FFC response to the fields we serve, keeping its sample window."""
    meta = payload.get("meta") or {}
    players = [
        {field: player.get(field) for field in FIELDS}
        for player in payload.get("players") or []
    ]
    return {
        "type": meta.get("type"),
        "sampleStart": meta.get("start_date"),
        "sampleEnd": meta.get("end_date"),
        "totalDrafts": meta.get("total_drafts"),
        "rounds": meta.get("rounds"),
        "players": players,
    }


def fetch_all(season: str) -> dict[str, Any]:
    """Every supported format.

    A format that fails is recorded rather than raised on: one dead endpoint
    should not cost the whole nightly snapshot, and the caller can see which
    are stale.
    """
    formats: dict[str, Any] = {}
    failed: dict[str, str] = {}

    for name in FORMATS:
        try:
            formats[name] = normalize(fetch_format(name, season))
        except Exception as err:  # noqa: BLE001 - recorded, not swallowed
            failed[name] = f"{type(err).__name__}: {err}"

    return {"season": season, "formats": formats, "failed": failed}
