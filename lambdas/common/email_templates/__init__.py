"""
Xomper Email Templates
======================
HTML email templates for fantasy football notifications.
Table-based layouts with inline CSS for email client compatibility.
"""

from .taxi_steal_league import (
    generate_taxi_steal_league_email,
    generate_taxi_steal_league_email_plain_text,
)
from .taxi_steal_owner import (
    generate_taxi_steal_owner_email,
    generate_taxi_steal_owner_email_plain_text,
)
from .rule_proposed import (
    generate_rule_proposed_email,
    generate_rule_proposed_email_plain_text,
)
from .rule_accepted import (
    generate_rule_accepted_email,
    generate_rule_accepted_email_plain_text,
)
from .rule_denied import (
    generate_rule_denied_email,
    generate_rule_denied_email_plain_text,
)
from .lineup_not_set import (
    generate_lineup_not_set_email,
    generate_lineup_not_set_email_plain_text,
)
from .weekly_recap import (
    generate_weekly_recap_email,
    generate_weekly_recap_email_plain_text,
)

__all__ = [
    "generate_taxi_steal_league_email",
    "generate_taxi_steal_league_email_plain_text",
    "generate_taxi_steal_owner_email",
    "generate_taxi_steal_owner_email_plain_text",
    "generate_rule_proposed_email",
    "generate_rule_proposed_email_plain_text",
    "generate_rule_accepted_email",
    "generate_rule_accepted_email_plain_text",
    "generate_rule_denied_email",
    "generate_rule_denied_email_plain_text",
    "generate_lineup_not_set_email",
    "generate_lineup_not_set_email_plain_text",
    "generate_weekly_recap_email",
    "generate_weekly_recap_email_plain_text",
]
