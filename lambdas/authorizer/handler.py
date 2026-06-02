"""
Lambda Authorizer
=================
API Gateway authorizer for Xomper. Verifies Supabase access tokens
signed with ES256 via the project's published JWKS.

Supabase JWTs use ES256 (ECDSA P-256) signed with a project-scoped
key pair. Public keys are published at
`{SUPABASE_URL}/auth/v1/.well-known/jwks.json`. PyJWKClient fetches +
caches them at module load, so verification on each invocation is a
local signature check.
"""

import os

import jwt
from jwt import PyJWKClient

from lambdas.common.constants import PRODUCT
from lambdas.common.logger import get_logger

log = get_logger(__file__)

HANDLER = 'authorizer'

# Module-level so PyJWKClient's internal key cache survives across
# Lambda warm invocations.
_SUPABASE_URL = (os.environ.get('SUPABASE_URL') or '').rstrip('/')
_JWKS_URL = f"{_SUPABASE_URL}/auth/v1/.well-known/jwks.json" if _SUPABASE_URL else ''
_jwks_client: PyJWKClient | None = (
    PyJWKClient(_JWKS_URL, cache_keys=True) if _JWKS_URL else None
)


def generate_policy(effect: str, resource: str) -> dict:
    """Return a valid AWS IAM policy response for API Gateway."""
    return {
        'principalId': PRODUCT,
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Action': 'execute-api:*',
                    'Effect': effect,
                    'Resource': resource
                }
            ]
        }
    }


def decode_auth_token(auth_token: str) -> dict | None:
    """Decode + verify a Supabase JWT. Returns claims dict or None on
    any failure (expired, bad signature, missing key, etc.)."""
    if _jwks_client is None:
        log.error("Authorizer: SUPABASE_URL env var missing — cannot verify tokens")
        return None
    try:
        token = auth_token.replace('Bearer ', '').strip()
        signing_key = _jwks_client.get_signing_key_from_jwt(token)
        return jwt.decode(
            token,
            signing_key.key,
            algorithms=['ES256'],
            audience='authenticated',
        )
    except jwt.ExpiredSignatureError:
        log.warning("Authorizer: token expired")
        return None
    except jwt.InvalidTokenError as err:
        log.warning(f"Authorizer: invalid token - {err}")
        return None
    except Exception as err:
        # Network failures fetching JWKS, malformed token header, etc.
        log.warning(f"Authorizer: decode error - {err}")
        return None


def handler(event: dict, context: object) -> dict:
    """Lambda authorizer entry point."""
    method_arn = event.get('methodArn', '')

    try:
        auth_token = event.get('authorizationToken', '')

        if not auth_token:
            log.warning("Authorizer: no authorization token provided")
            return generate_policy('Deny', method_arn)

        if not method_arn:
            log.error("Authorizer: no methodArn in event")
            return generate_policy('Deny', method_arn)

        user_details = decode_auth_token(auth_token)
        if user_details:
            arn_parts = method_arn.split(':')
            api_gateway_arn_tmp = arn_parts[5].split('/')
            resource_arn = (
                f"{arn_parts[0]}:{arn_parts[1]}:{arn_parts[2]}:"
                f"{arn_parts[3]}:{arn_parts[4]}:"
                f"{api_gateway_arn_tmp[0]}/{api_gateway_arn_tmp[1]}/*"
            )
            return generate_policy('Allow', resource_arn)

        log.warning("Authorizer: Deny - token decode failed")
        return generate_policy('Deny', method_arn)

    except Exception as err:
        log.error(f"Authorizer: unexpected error - {err}", exc_info=True)
        return generate_policy('Deny', method_arn)
