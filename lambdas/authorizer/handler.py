"""
Lambda Authorizer
=================
API Gateway authorizer for Xomper. Accepts either identity provider while the
platform migrates from Supabase to Cognito.

Supabase issues ES256 tokens signed with a project-scoped key pair, published
at `{SUPABASE_URL}/auth/v1/.well-known/jwks.json`.

Cognito issues RS256 tokens for the shared `xomware-users` pool, published at
`https://cognito-idp.{region}.amazonaws.com/{poolId}/.well-known/jwks.json`.

Accepting both is deliberate. A single-provider authorizer forces a flag day:
the frontend and the API have to switch in the same deploy, and any user
holding a session from the old provider is signed out mid-migration. Verifying
both means the frontend can move whenever it is ready, and this narrows back
to Cognito alone once nothing issues Supabase tokens.

Both clients cache their keys at module load, so verification on each
invocation is a local signature check.

Claims are returned to the caller as `principalId` and in the authorizer
context, so downstream handlers can identify the user without re-decoding.
"""

from __future__ import annotations

import os

import jwt
from jwt import PyJWKClient

from lambdas.common.constants import PRODUCT
from lambdas.common.logger import get_logger

log = get_logger(__file__)

HANDLER = 'authorizer'

# Module-level so PyJWKClient's internal key cache survives warm invocations.
_SUPABASE_URL = (os.environ.get('SUPABASE_URL') or '').rstrip('/')
_SUPABASE_JWKS = (
    f"{_SUPABASE_URL}/auth/v1/.well-known/jwks.json" if _SUPABASE_URL else ''
)
_supabase_jwks: PyJWKClient | None = (
    PyJWKClient(_SUPABASE_JWKS, cache_keys=True) if _SUPABASE_JWKS else None
)

_COGNITO_POOL_ID = os.environ.get('COGNITO_USER_POOL_ID') or ''
_COGNITO_CLIENT_ID = os.environ.get('COGNITO_CLIENT_ID') or ''
_AWS_REGION = os.environ.get('AWS_REGION') or 'us-east-1'
_COGNITO_JWKS = (
    f"https://cognito-idp.{_AWS_REGION}.amazonaws.com/"
    f"{_COGNITO_POOL_ID}/.well-known/jwks.json"
    if _COGNITO_POOL_ID
    else ''
)
_cognito_jwks: PyJWKClient | None = (
    PyJWKClient(_COGNITO_JWKS, cache_keys=True) if _COGNITO_JWKS else None
)


def generate_policy(effect: str, resource: str, claims: dict | None = None) -> dict:
    """Return a valid AWS IAM policy response for API Gateway."""
    policy = {
        'principalId': (claims or {}).get('sub') or PRODUCT,
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

    if claims:
        # Context values must be strings. Handlers read these instead of
        # decoding the token a second time.
        groups = claims.get('cognito:groups') or []
        policy['context'] = {
            'sub': str(claims.get('sub') or ''),
            'email': str(claims.get('email') or ''),
            'provider': str(claims.get('_provider') or ''),
            'groups': ','.join(groups) if isinstance(groups, list) else str(groups),
        }

    return policy


def _try_supabase(token: str) -> dict | None:
    if _supabase_jwks is None:
        return None
    try:
        signing_key = _supabase_jwks.get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=['ES256'],
            audience='authenticated',
        )
        claims['_provider'] = 'supabase'
        return claims
    except jwt.ExpiredSignatureError:
        log.warning("Authorizer: supabase token expired")
        return None
    except Exception:
        # Wrong issuer is the common case now that two providers are live, and
        # it is not worth a warning on every Cognito request.
        return None


def _try_cognito(token: str) -> dict | None:
    if _cognito_jwks is None:
        return None
    try:
        signing_key = _cognito_jwks.get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=['RS256'],
            # Cognito ACCESS tokens carry no `aud`, only `client_id`; ID tokens
            # carry `aud`. Verifying audience here would reject access tokens
            # outright, so the client is checked below instead.
            options={'verify_aud': False},
            issuer=(
                f"https://cognito-idp.{_AWS_REGION}.amazonaws.com/"
                f"{_COGNITO_POOL_ID}"
            ),
        )
    except jwt.ExpiredSignatureError:
        log.warning("Authorizer: cognito token expired")
        return None
    except Exception:
        return None

    # A token from another app client on the shared pool is a valid Cognito
    # token but not one for this app. The pool is estate-wide, so this check is
    # what keeps xomforms or xomtracks sessions out of Xomper's API.
    if _COGNITO_CLIENT_ID:
        presented = claims.get('client_id') or claims.get('aud')
        if presented != _COGNITO_CLIENT_ID:
            log.warning("Authorizer: cognito token for a different app client")
            return None

    claims['_provider'] = 'cognito'
    return claims


def decode_auth_token(auth_token: str) -> dict | None:
    """Verify against either provider. Returns claims or None."""
    token = auth_token.replace('Bearer ', '').strip()
    if not token:
        return None

    claims = _try_cognito(token) or _try_supabase(token)
    if claims is None:
        log.warning("Authorizer: token matched neither provider")
    return claims


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

        claims = decode_auth_token(auth_token)
        if claims:
            arn_parts = method_arn.split(':')
            api_gateway_arn_tmp = arn_parts[5].split('/')
            resource_arn = (
                f"{arn_parts[0]}:{arn_parts[1]}:{arn_parts[2]}:"
                f"{arn_parts[3]}:{arn_parts[4]}:"
                f"{api_gateway_arn_tmp[0]}/{api_gateway_arn_tmp[1]}/*"
            )
            log.info(f"Authorizer: Allow via {claims.get('_provider')}")
            return generate_policy('Allow', resource_arn, claims)

        log.warning("Authorizer: Deny - token decode failed")
        return generate_policy('Deny', method_arn)

    except Exception as err:
        log.error(f"Authorizer: unexpected error - {err}", exc_info=True)
        return generate_policy('Deny', method_arn)
