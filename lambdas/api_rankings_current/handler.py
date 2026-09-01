"""
API — Consensus rankings
========================
GET /rankings/current

Every player several public sources have an opinion on, with each source's
rank, the mean, and the spread between them.

`spread` is the reason this exists. A player FantasyCalc ranks 107th and ESPN
ranks 416th is a decision the drafter should be shown; a consensus number alone
buries exactly the disagreement that makes him interesting.
"""
import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

from lambdas.common.constants import WAREHOUSE_BUCKET_NAME
from lambdas.common.errors import handle_errors
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import success_response

HANDLER = "api_rankings_current"
log = get_logger(HANDLER)

RANKINGS_KEY = "rankings/current/rankings.json"


def _load() -> dict[str, Any] | None:
    """The snapshot, or None if the ingest has not written one yet.

    None is an expected state rather than a failure: between deploying this and
    the next 08:00 UTC run there genuinely is no snapshot, and a caller needs to
    tell that apart from a broken warehouse.
    """
    try:
        obj = boto3.client("s3").get_object(Bucket=WAREHOUSE_BUCKET_NAME, Key=RANKINGS_KEY)
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(obj["Body"].read())


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    snapshot = _load()
    if snapshot is None:
        return success_response(
            {
                "error": "no_rankings_snapshot",
                "detail": "the nightly warehouse ingest has not written one yet",
            },
            status_code=404,
        )

    return success_response(
        {
            "capturedAt": snapshot.get("capturedAt", ""),
            "season": snapshot.get("season", ""),
            "sources": snapshot.get("sources", []),
            # Named so a caller can say "ESPN is missing today" rather than
            # silently presenting two lists as three.
            "failed": snapshot.get("failed", {}),
            "count": len(snapshot.get("players", {})),
            "players": snapshot.get("players", {}),
        }
    )
