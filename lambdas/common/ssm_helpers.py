"""
XOMPER SSM Helpers
==================
Lazy-loaded SSM parameter fetching.
Lambda IAM role provides AWS credentials -- no need to fetch them from SSM.
"""

import boto3
from typing import Optional

from lambdas.common.constants import PRODUCT
from lambdas.common.logger import get_logger

log = get_logger(__file__)

_ssm_client: Optional[boto3.client] = None
_cache: dict[str, str] = {}

__API_ROOT = f'/{PRODUCT}/api/'


def _get_ssm_client() -> boto3.client:
    """Return a cached SSM client (created on first call)."""
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    return _ssm_client


def get_parameter(name: str, *, decrypt: bool = True) -> str:
    """
    Fetch an SSM parameter by name with local caching.

    Args:
        name: Full SSM parameter path
        decrypt: Whether to decrypt SecureString parameters

    Returns:
        Parameter value string
    """
    if name in _cache:
        return _cache[name]

    client = _get_ssm_client()
    value = client.get_parameter(Name=name, WithDecryption=decrypt)['Parameter']['Value']
    _cache[name] = value
    log.info(f"Fetched SSM parameter: {name}")
    return value


# ---------------------------------------------------------------------------
# Lazy module-level attribute access (same pattern as xomify)
# Accessing `ssm_helpers.API_SECRET_KEY` triggers a fetch on first read.
# ---------------------------------------------------------------------------

_LAZY_PARAMS: dict[str, str] = {
    "API_SECRET_KEY": f"{__API_ROOT}API_SECRET_KEY",
}


def __getattr__(name: str) -> str:
    if name in _LAZY_PARAMS:
        value = get_parameter(_LAZY_PARAMS[name])
        # Cache on the module so subsequent access is direct
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
