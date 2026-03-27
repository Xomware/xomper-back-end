"""
XOMPER Pydantic Models
======================
Request/response validation models for Lambda handlers.
"""

from typing import Optional
from pydantic import BaseModel, field_validator


class Proposal(BaseModel):
    """Rule proposal details."""
    title: str = "Untitled Rule"
    description: str = ""
    proposed_by_username: str = "A league member"
    league_name: str = ""


class RuleProposalRequest(BaseModel):
    """POST /email/rule-proposal request body."""
    proposal: Proposal
    recipients: list[str]

    @field_validator("recipients")
    @classmethod
    def recipients_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("recipients must not be empty")
        return v


class RuleVoteRequest(BaseModel):
    """POST /email/rule-accept and /email/rule-deny request body."""
    proposal: Proposal
    approved_by: list[str]
    rejected_by: list[str]
    recipients: list[str]

    @field_validator("recipients")
    @classmethod
    def recipients_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("recipients must not be empty")
        return v


class Stealer(BaseModel):
    """Player who is stealing from a taxi squad."""
    display_name: str = "A league member"


class Player(BaseModel):
    """Fantasy player being stolen."""
    first_name: str = ""
    last_name: str = ""
    position: str = "N/A"
    team: str = "N/A"
    player_image_url: str = ""
    team_logo_url: str = ""
    pick_cost: str = ""


class Owner(BaseModel):
    """Taxi squad owner being stolen from."""
    display_name: str = "Unknown"
    email: Optional[str] = None


class TaxiStealRequest(BaseModel):
    """POST /email/taxi request body."""
    stealer: Stealer
    player: Player
    owner: Owner
    recipients: list[str]
    league_name: str = ""

    @field_validator("recipients")
    @classmethod
    def recipients_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("recipients must not be empty")
        return v


class EmailResponse(BaseModel):
    """Standard email handler response."""
    successfulEmails: int
    failedEmails: int
