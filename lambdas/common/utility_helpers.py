"""
XOMPER Utility Helpers
======================
Common utilities for Lambda handlers.
"""

import json
import decimal
import base64
from datetime import datetime, timezone
from typing import Any, Optional, Set

from lambdas.common.logger import get_logger

log = get_logger(__file__)


# ============================================
# JSON Encoding
# ============================================

class XomperJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that handles:
    - Decimal (from DynamoDB)
    - datetime objects
    - sets
    """
    def default(self, obj: Any) -> Any:
        if isinstance(obj, decimal.Decimal):
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)


def json_dumps(obj: Any) -> str:
    """Serialize object to JSON string with custom encoder."""
    return json.dumps(obj, cls=XomperJSONEncoder)


# ============================================
# Request Parsing
# ============================================

def is_api_request(event: dict) -> bool:
    """Check if the event is from API Gateway."""
    return isinstance(event.get('body'), str)


def is_cron_event(event: dict) -> bool:
    """Check if the event is from CloudWatch Events (cron)."""
    return event.get('source') == 'aws.events'


def parse_body(event: dict) -> dict:
    """
    Parse the request body from an event.
    Handles both API Gateway (string) and direct invocation (dict).
    """
    body = event.get('body')

    if body is None:
        return {}

    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            log.warning("Failed to parse body as JSON")
            return {}

    return body if isinstance(body, dict) else {}


def get_query_params(event: dict) -> dict:
    """Get query string parameters from event."""
    return event.get('queryStringParameters') or {}


def get_path_params(event: dict) -> dict:
    """Get path parameters from event."""
    return event.get('pathParameters') or {}


# ============================================
# Response Building
# ============================================

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "https://xomper.xomware.com",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    "Content-Type": "application/json"
}


def success_response(body: Any, status_code: int = 200, is_api: bool = True) -> dict:
    """
    Build a successful Lambda response.

    Args:
        body: Response data (will be JSON encoded if is_api=True)
        status_code: HTTP status code (default 200)
        is_api: If True, JSON encode the body

    Returns:
        Lambda response dict
    """
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json_dumps(body) if is_api else body,
        "isBase64Encoded": False
    }


def error_response(
    message: str,
    status_code: int = 500,
    is_api: bool = True,
    details: Optional[dict] = None
) -> dict:
    """
    Build an error Lambda response.

    Args:
        message: Error message
        status_code: HTTP status code (default 500)
        is_api: If True, JSON encode the body
        details: Optional additional error details

    Returns:
        Lambda response dict
    """
    body = {
        "error": {
            "message": message,
            "status": status_code,
            **(details or {})
        }
    }

    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json_dumps(body) if is_api else body,
        "isBase64Encoded": False
    }


# ============================================
# Input Validation
# ============================================

def validate_input(
    data: Optional[dict],
    required_fields: Set[str] = None,
    optional_fields: Set[str] = None
) -> tuple[bool, Optional[str]]:
    """
    Validate input data has required fields and no extra fields.

    Args:
        data: Input dictionary to validate
        required_fields: Set of required field names
        optional_fields: Set of optional field names

    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = required_fields or set()
    optional_fields = optional_fields or set()

    if data is None:
        if required_fields:
            return False, f"Missing required fields: {required_fields}"
        return True, None

    if not isinstance(data, dict):
        return False, "Input must be a dictionary"

    data_keys = set(data.keys())
    allowed_keys = required_fields | optional_fields

    missing = required_fields - data_keys
    if missing:
        return False, f"Missing required fields: {missing}"

    if optional_fields:
        extra = data_keys - allowed_keys
        if extra:
            return False, f"Unexpected fields: {extra}"

    return True, None


def require_fields(data: dict, *fields: str) -> None:
    """
    Raise ValidationError if any required fields are missing.

    Usage:
        require_fields(body, 'email', 'userId')
    """
    from lambdas.common.errors import ValidationError

    missing = [f for f in fields if f not in data or data[f] is None]
    if missing:
        raise ValidationError(
            message=f"Missing required fields: {', '.join(missing)}",
            field=missing[0]
        )


# ============================================
# Date/Time Utilities
# ============================================

def get_timestamp() -> str:
    """Get current UTC timestamp in standard format."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def get_iso_timestamp() -> str:
    """Get current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def format_date(raw_date: str) -> datetime:
    """Parse MM/DD/YYYY date string to datetime."""
    parts = raw_date.split('/')
    return datetime(int(parts[2]), int(parts[0]), int(parts[1]), tzinfo=timezone.utc)


# ============================================
# Encoding Utilities
# ============================================

def encode_credentials(key: str, secret: str) -> str:
    """Base64 encode credentials for Basic Auth."""
    data = f"{key}:{secret}"
    return base64.b64encode(data.encode('utf-8')).decode('utf-8')
