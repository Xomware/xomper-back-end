"""
Who a scheduled job should email, and about which league.

Replaces the pair every notification lambda calls today:
`get_active_whitelisted_league()` (one league, from Supabase) and
`get_active_whitelisted_users()` (every whitelisted user, regardless of
whether they are in that league). That shape is why the crons are
single-league -- there is exactly one active row to find.

Here a job asks for audiences and gets one entry per league that somebody
actually follows, carrying only the followers of that league. A league nobody
follows never appears, which is the cost control the follow table was built
for: no Sleeper fan-out and no SES spend for an audience of zero.

Nothing calls this yet. Flipping a cron over changes who receives email, so
each one moves separately and deliberately.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lambdas.common import platform_follows, platform_users
from lambdas.common.logger import get_logger

log = get_logger(__name__)


@dataclass
class Recipient:
    user_id: str
    email: str
    display_name: str
    sleeper_user_id: str


@dataclass
class Audience:
    league_id: str
    recipients: list[Recipient] = field(default_factory=list)

    @property
    def by_sleeper_id(self) -> dict[str, Recipient]:
        """Recipients keyed the way roster data arrives.

        Sleeper rosters carry an owner_id, so a job matching a roster to a
        person needs this direction. Anyone who has not linked a handle is
        absent rather than keyed on an empty string.
        """
        return {r.sleeper_user_id: r for r in self.recipients if r.sleeper_user_id}


def _recipient(user_id: str) -> Recipient | None:
    record = platform_users.get_user(user_id)
    if not record:
        return None

    # No email, no notification. This is the opt-in: the record exists from
    # the moment someone signs in, and the address is what makes them
    # reachable.
    email = str(record.get("email") or "")
    if not email:
        return None

    return Recipient(
        user_id=user_id,
        email=email,
        display_name=str(
            record.get("displayName") or record.get("sleeperUsername") or "Someone"
        ),
        sleeper_user_id=str(record.get("sleeperUserId") or ""),
    )


def audiences() -> list[Audience]:
    """One entry per followed league, with the followers who can be emailed.

    Leagues whose followers all turn out to be unreachable are dropped, so a
    caller can treat a returned Audience as having someone to send to.
    """
    resolved: dict[str, Recipient | None] = {}
    out = []

    for league_id, user_ids in platform_follows.all_followed_leagues().items():
        recipients = []
        for user_id in user_ids:
            # One league's followers overlap the next; resolving each user
            # once keeps this a fixed number of reads rather than one per
            # (league, follower) pair.
            if user_id not in resolved:
                resolved[user_id] = _recipient(user_id)
            found = resolved[user_id]
            if found:
                recipients.append(found)

        if recipients:
            out.append(Audience(league_id=league_id, recipients=recipients))

    log.info(f"audience: {len(out)} league(s) with a reachable follower")
    return out


def audience_for(league_id: str) -> Audience | None:
    """The audience for one league, or None if nobody reachable follows it."""
    recipients = [r for r in map(_recipient, platform_follows.followers_of(league_id)) if r]
    return Audience(league_id=league_id, recipients=recipients) if recipients else None
