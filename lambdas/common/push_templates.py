"""
XOMPER Push Notification Templates
===================================
Returns (title, body, category, data) tuples for each notification type.
"""


def rule_proposed_push(proposer_name: str, rule_title: str) -> tuple[str, str, str, dict]:
    """Push template for a new rule proposal."""
    return (
        "New Rule Proposal",
        f"{proposer_name} proposed: {rule_title}",
        "RULE_PROPOSAL",
        {},
    )


def rule_accepted_push(rule_title: str) -> tuple[str, str, str, dict]:
    """Push template for an approved rule."""
    return (
        "Rule Approved",
        f"{rule_title} has been approved!",
        "RULE_ACCEPTED",
        {},
    )


def rule_denied_push(rule_title: str) -> tuple[str, str, str, dict]:
    """Push template for a denied rule."""
    return (
        "Rule Denied",
        f"{rule_title} has been denied",
        "RULE_DENIED",
        {},
    )


def taxi_steal_push(stealer_name: str, player_name: str) -> tuple[str, str, str, dict]:
    """Push template for a taxi squad steal alert."""
    return (
        "Taxi Steal!",
        f"{stealer_name} is stealing {player_name}!",
        "TAXI_STEAL",
        {},
    )
