"""
Caller identity
===============
Reads who is calling out of the API Gateway request context.

The Lambda authorizer already verified the token and put the claims it cares
about into `requestContext.authorizer`. Handlers read them from there rather
than decoding the Authorization header a second time — the decode is the
authorizer's job, and a handler that repeats it can disagree with the decision
that let the request through.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lambdas.common.errors import AuthorizationError


@dataclass
class Caller:
    """The authenticated user behind a request."""

    user_id: str
    email: str = ""
    provider: str = ""
    groups: list[str] = field(default_factory=list)

    @property
    def is_admin(self) -> bool:
        return "admin" in self.groups


def get_caller(event: dict[str, Any]) -> Caller:
    """Extract the caller, raising 401 if the context is missing.

    A missing context means the request reached the handler without passing
    the authorizer, which should be impossible on a protected route. Treat it
    as unauthenticated rather than defaulting to an anonymous user, so a route
    accidentally wired without an authorizer fails closed.
    """
    context = (event.get("requestContext") or {}).get("authorizer") or {}
    user_id = context.get("sub") or ""

    if not user_id:
        raise AuthorizationError("No authenticated caller on this request")

    raw_groups = context.get("groups") or ""
    groups = [g for g in str(raw_groups).split(",") if g]

    return Caller(
        user_id=user_id,
        email=context.get("email") or "",
        provider=context.get("provider") or "",
        groups=groups,
    )
