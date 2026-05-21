"""
League Lore
===========
Personality fuel for the AI Review feature. Codifies the 12 manager
profiles from the Xomper league as structured Python data, keyed by
their confirmed Sleeper `user_id`.

The lore is intentionally personal and a little mean — the AI Review
prompt asks Claude to roast, joke, and make fun of the managers in a
post-draft / preseason / weekly recap voice. All content here is
sourced from the user-authored brief (issue #79 + the user's
follow-up confirmations in the F0 plan, Appendix A).

PII / privacy note
------------------
This module ships in a private repo and is consumed only by
backend lambdas inside the Xomper league. Do not log the full lore
blob; the league-wide profile dump should only ever be sent inside
the (cached, server-side) system prompt to Anthropic.

Schema
------
`ManagerProfile` carries the personality scaffolding for a single
manager. `LEAGUE_LORE` is a `dict[str, ManagerProfile]` keyed by
Sleeper `user_id`.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ManagerProfile:
    """Per-manager personality bundle used by the AI Review prompt."""

    display_name: str
    """How the bot refers to this manager in copy (first name / nickname)."""

    nicknames: list[str] = field(default_factory=list)
    """Alt names the bot can mix in — keeps the writing varied."""

    favorite_teams: list[str] = field(default_factory=list)
    """NFL + college + MLB + NBA mixed; tagged inline like 'Ravens (NFL)'."""

    school: str | None = None
    """Primary undergrad / professional school. Use the most recent /
    most-bragged-about institution; secondary schools belong in
    `notable_stories` or `life_events`."""

    notable_stories: list[str] = field(default_factory=list)
    """Evergreen embarrassments / running jokes. One-liners with
    personality — e.g. 'fell asleep on the toilet at SEC championship'."""

    life_events: list[str] = field(default_factory=list)
    """Time-bounded context the lambda may want to reference — marriages,
    engagements, jobs, moves. Yearly review fodder."""


LEAGUE_LORE: dict[str, ManagerProfile] = {
    # Order matches Appendix A of the F0 plan. All 12 user_ids confirmed
    # by the user on 2026-05-21.

    "594625531702460416": ManagerProfile(  # domgiordano
        display_name="Dominick",
        nicknames=["Dom"],
        favorite_teams=[
            "Baltimore Ravens (NFL)",
            "Baltimore Orioles (MLB)",
            "Charlotte Hornets (NBA)",
        ],
        school="UNC Chapel Hill",
        notable_stories=[
            "Tar Heels diehard who will work UNC into any conversation",
            "Charlotte sports fan, which means professional suffering",
        ],
        life_events=[],
    ),

    "418511574492270592": ManagerProfile(  # reesegriffin
        display_name="Reese",
        nicknames=[],
        favorite_teams=[
            "Chicago Bears (NFL)",
            "Chicago Bulls (NBA)",
            "Chicago Cubs (MLB)",
            "Chicago Blackhawks (NHL)",
        ],
        school="UNC Chapel Hill",
        notable_stories=[
            "All-in on Chicago sports — owns every flavor of Chicago heartbreak",
        ],
        life_events=[],
    ),

    "867213342836711424": ManagerProfile(  # gtatich
        display_name="Grant",
        nicknames=["Grant Tatich"],
        favorite_teams=[
            "Carolina Panthers (NFL)",
            "Atlanta Braves (MLB)",
        ],
        school="UNC Chapel Hill",
        notable_stories=[
            "Panthers fan, which is its own punishment",
            "Kyle's younger brother — and reminded of it regularly",
        ],
        life_events=[],
    ),

    "1132215311643787264": ManagerProfile(  # ktatich
        display_name="Kyle",
        nicknames=["Kyle Tatich"],
        favorite_teams=[
            "Carolina Panthers (NFL)",
        ],
        school="Notre Dame Law",
        notable_stories=[
            "Went to Wake Forest undergrad, then Notre Dame for law — claims the better school depending on the room",
            "The older, self-described cooler brother to Grant Tatich",
        ],
        life_events=[
            "Engaged",
        ],
    ),

    "867213779035906048": ManagerProfile(  # lukenovak
        display_name="Luke",
        nicknames=["Luke Novak"],
        favorite_teams=[
            "Pittsburgh Steelers (NFL)",
            "Pittsburgh Pirates (MLB)",
            "Pittsburgh Penguins (NHL)",
        ],
        school="USC (South Carolina)",
        notable_stories=[
            "All Pittsburgh sports — natural Ravens nemesis in this league",
            "Alex's younger brother",
        ],
        life_events=[],
    ),

    "609168618525110272": ManagerProfile(  # alexnovak02
        display_name="Alex",
        nicknames=["Alex Novak"],
        favorite_teams=[
            "Pittsburgh Steelers (NFL) — losers",
            "South Carolina Gamecocks (NCAA)",
        ],
        school="USC (South Carolina)",
        notable_stories=[
            "BIG Gamecocks fan — team name 'Shane Beamer's Burner' confirms USC allegiance",
            "Steelers fan, which the rest of the league enjoys reminding him about",
            "Luke's older brother",
        ],
        life_events=[],
    ),

    "865328062403870720": ManagerProfile(  # cfolk
        display_name="Connor",
        nicknames=["Connor Folk"],
        favorite_teams=[
            "Miami Dolphins (NFL)",
        ],
        school="UNC Chapel Hill",
        notable_stories=[
            "Dolphins fan — universally agreed to be sad",
        ],
        life_events=[],
    ),

    "1001254799347658752": ManagerProfile(  # mwynne16
        display_name="Michael",
        nicknames=["Michael Wynne", "Wynne"],
        favorite_teams=[
            "Carolina Panthers (NFL)",
        ],
        school=None,
        notable_stories=[
            "Played baseball in college",
            "Panthers fan, sharing in the Tatich pain",
        ],
        life_events=[
            "Married Ashley",
        ],
    ),

    "992955241630896128": ManagerProfile(  # Tibor100
        display_name="Tibor",
        nicknames=["The Goffather"],
        favorite_teams=[],
        school=None,
        notable_stories=[
            "Big golfer — team name 'The Goffather' is on-brand",
        ],
        life_events=[],
    ),

    "741140723985997824": ManagerProfile(  # dmurchis
        display_name="Duncan",
        nicknames=["Duncan Murchison"],
        favorite_teams=[
            "Washington Commanders (NFL)",
        ],
        school="Michigan Law",
        notable_stories=[
            "UNC undergrad turned Michigan Law — became a fake huge Michigan fan overnight (go blue.)",
            "Commanders fan",
        ],
        life_events=[],
    ),

    "1127420529155227648": ManagerProfile(  # andrewga23
        display_name="Tony",
        nicknames=["Anthony Hendricks", "Tony Hendricks"],
        favorite_teams=[
            "Georgia Bulldogs (NCAA)",
        ],
        school="UGA (University of Georgia)",
        notable_stories=[
            "Big Bulldogs fan — UGA over everything",
            "Once fell asleep on the toilet at the SEC championship — and the league will never let that go",
        ],
        life_events=[],
    ),

    "866444821185843200": ManagerProfile(  # gniadek
        display_name="Jim",
        nicknames=["Jimbo"],
        favorite_teams=[],
        school="UNC Chapel Hill",
        notable_stories=[
            "From Hickory, NC — loves Hickory and reminds the league regularly",
        ],
        life_events=[],
    ),
}


# Convenience re-exports for tests and prompt builders.
ALL_USER_IDS: list[str] = list(LEAGUE_LORE.keys())
ALL_DISPLAY_NAMES: list[str] = [p.display_name for p in LEAGUE_LORE.values()]
