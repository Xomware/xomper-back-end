"""
API — Current ADP
=================
GET /adp/current[?format=ppr]

Serves the nightly Fantasy Football Calculator snapshot the warehouse ingest
writes. Plain JSON on S3 rather than Parquet: the whole payload is a few
thousand small rows read on every draft-board load, and standing up DuckDB and
its layer to return a static list would cost far more than it saves.

The response carries the sample window with the numbers. ADP sampled a week
before a draft is a different claim from ADP sampled the same morning, and the
board is expected to show which it is.
"""
from typing import Any

import boto3

from lambdas.common.constants import WAREHOUSE_BUCKET_NAME
from lambdas.common.errors import ValidationError, handle_errors
from lambdas.common.ffc_adp import FORMATS
from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import success_response

HANDLER = "api_adp_current"
log = get_logger(HANDLER)

ADP_KEY = "adp/current/adp.json"


def _load() -> dict[str, Any]:
    import json

    obj = boto3.client("s3").get_object(Bucket=WAREHOUSE_BUCKET_NAME, Key=ADP_KEY)
    return json.loads(obj["Body"].read())


@handle_errors(HANDLER)
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    params = event.get("queryStringParameters") or {}
    requested = params.get("format")

    if requested is not None and requested not in FORMATS:
        # Naming the supported set matters: a TE-premium league has no ADP
        # upstream at all, and the board must say so rather than quietly
        # showing PPR numbers under a TE-premium heading.
        raise ValidationError(
            f"unsupported format '{requested}'; supported: {', '.join(sorted(FORMATS))}"
        )

    snapshot = _load()

    if requested:
        payload = snapshot.get("formats", {}).get(requested)
        if not payload:
            return success_response(
                {"error": f"no snapshot for format '{requested}'"}, status_code=404
            )
        return success_response({
            "season": snapshot.get("season"),
            "capturedAt": snapshot.get("capturedAt"),
            "format": requested,
            **payload,
        })

    return success_response(snapshot)
