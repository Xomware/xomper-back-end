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


# MARK: - Scheduled / scoring-week notifications


def weekly_recap_push(
    week: int,
    user_team_name: str,
    user_won: bool,
    user_pts: float,
    opp_team_name: str,
    opp_pts: float,
    league_high_team: str,
    league_high_pts: float,
) -> tuple[str, str, str, dict]:
    """Personalized weekly recap push, one per manager."""
    outcome = "won" if user_won else "lost"
    body = (
        f"You {outcome} {user_pts:.1f}–{opp_pts:.1f} vs {opp_team_name}. "
        f"League high: {league_high_team} {league_high_pts:.1f}."
    )
    return (
        f"Week {week} recap",
        body,
        "WEEKLY_RECAP",
        {"week": str(week)},
    )


def lineup_not_set_push(
    league_name: str,
    issue_count: int,
) -> tuple[str, str, str, dict]:
    """Sunday-morning reminder when a manager has bye/OUT starters."""
    starter_word = "starter" if issue_count == 1 else "starters"
    return (
        "Set your lineup",
        f"Your {league_name} lineup has {issue_count} {starter_word} on bye or OUT. "
        "Tap to fix before kickoff.",
        "LINEUP_NOT_SET",
        {},
    )


def close_game_push(
    user_team_name: str,
    user_pts: float,
    opp_team_name: str,
    opp_pts: float,
) -> tuple[str, str, str, dict]:
    """Primetime close-game alert (within 10 points, players still active)."""
    return (
        "Close game alert 🔥",
        f"{user_team_name} {user_pts:.1f}–{opp_pts:.1f} {opp_team_name}. "
        "Tap to watch the finish.",
        "CLOSE_GAME",
        {},
    )


def worldcup_clinched_push(division_name: str) -> tuple[str, str, str, dict]:
    return (
        "🏆 You clinched a World Cup spot!",
        f"{division_name} — guaranteed top 2.",
        "WORLDCUP_CLINCHED",
        {},
    )


def worldcup_eliminated_push(division_name: str) -> tuple[str, str, str, dict]:
    return (
        "World Cup hopes ended",
        f"{division_name} — mathematically eliminated.",
        "WORLDCUP_ELIMINATED",
        {},
    )


def worldcup_line_moved_push(
    division_name: str,
    crossed_into: bool,
) -> tuple[str, str, str, dict]:
    direction = "in" if crossed_into else "out of"
    return (
        "World Cup line moved",
        f"You're now {direction} the cutoff in {division_name}.",
        "WORLDCUP_LINE_MOVED",
        {},
    )
