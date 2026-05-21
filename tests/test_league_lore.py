"""
Tests for lambdas.common.league_lore.

Locks down the schema of the 12 manager profiles — these are
consumed verbatim by the AI Review prompt builder in F1/F2/F3, so
shape regressions need to fail loudly here before they ever reach
Claude.
"""
from __future__ import annotations

import pytest

from lambdas.common.league_lore import (
    ALL_DISPLAY_NAMES,
    ALL_USER_IDS,
    LEAGUE_LORE,
    ManagerProfile,
)


EXPECTED_MANAGER_COUNT = 12


class TestLeagueLoreShape:
    def test_count(self) -> None:
        assert len(LEAGUE_LORE) == EXPECTED_MANAGER_COUNT

    def test_user_ids_unique(self) -> None:
        # dict keys are inherently unique — this guards against a
        # copy/paste merging two entries under one key.
        assert len(set(LEAGUE_LORE.keys())) == EXPECTED_MANAGER_COUNT

    def test_user_ids_look_real(self) -> None:
        """All keys should be non-empty Sleeper user_id strings, not
        placeholders like '# TODO' or 'unknown'."""
        for uid in LEAGUE_LORE.keys():
            assert isinstance(uid, str), f"user_id is not a str: {uid!r}"
            assert uid, "user_id is empty"
            assert uid.isdigit(), f"user_id is not all-digits: {uid!r}"
            assert "TODO" not in uid.upper(), f"placeholder user_id leaked: {uid!r}"

    def test_all_values_are_manager_profiles(self) -> None:
        for uid, profile in LEAGUE_LORE.items():
            assert isinstance(profile, ManagerProfile), (
                f"{uid} value is not a ManagerProfile: {type(profile).__name__}"
            )

    def test_display_names_present_and_unique(self) -> None:
        names = [p.display_name for p in LEAGUE_LORE.values()]
        for n in names:
            assert isinstance(n, str)
            assert n.strip(), "display_name is empty"
            assert "TODO" not in n.upper(), f"placeholder display_name: {n!r}"
        # We tolerate dups in principle but in practice all 12 are
        # distinct first names. If this ever stops being true, drop
        # this assertion.
        assert len(set(names)) == len(names)

    def test_list_fields_are_lists_of_strings(self) -> None:
        list_fields = ("nicknames", "favorite_teams", "notable_stories", "life_events")
        for uid, profile in LEAGUE_LORE.items():
            for field_name in list_fields:
                value = getattr(profile, field_name)
                assert isinstance(value, list), (
                    f"{uid}.{field_name} is not a list: {type(value).__name__}"
                )
                for item in value:
                    assert isinstance(item, str), (
                        f"{uid}.{field_name} contains non-string: {item!r}"
                    )
                    assert item.strip(), (
                        f"{uid}.{field_name} contains empty string"
                    )
                    assert "TODO" not in item.upper(), (
                        f"{uid}.{field_name} has placeholder: {item!r}"
                    )

    def test_school_is_str_or_none(self) -> None:
        for uid, profile in LEAGUE_LORE.items():
            assert profile.school is None or isinstance(profile.school, str), (
                f"{uid}.school is not str|None: {type(profile.school).__name__}"
            )
            if isinstance(profile.school, str):
                assert profile.school.strip(), f"{uid}.school is empty"
                assert "TODO" not in profile.school.upper()

    def test_profile_is_frozen(self) -> None:
        """`@dataclass(frozen=True)` means consumers can't mutate the
        lore at runtime — guards against accidental edits inside the
        prompt builder."""
        profile = next(iter(LEAGUE_LORE.values()))
        with pytest.raises(Exception):
            profile.display_name = "Mutated"  # type: ignore[misc]


class TestLeagueLoreConvenience:
    def test_all_user_ids_matches_keys(self) -> None:
        assert ALL_USER_IDS == list(LEAGUE_LORE.keys())

    def test_all_display_names_matches_profiles(self) -> None:
        assert ALL_DISPLAY_NAMES == [p.display_name for p in LEAGUE_LORE.values()]


class TestLeagueLoreContent:
    """Spot-check that the user's verbatim personality details survived
    the codification. Not exhaustive — just enough to catch a blank
    template ship."""

    def test_tony_sec_toilet_story_present(self) -> None:
        tony = LEAGUE_LORE["1127420529155227648"]
        assert tony.display_name == "Tony"
        joined = " | ".join(tony.notable_stories).lower()
        assert "sec championship" in joined
        assert "toilet" in joined

    def test_duncan_fake_michigan_fan(self) -> None:
        duncan = LEAGUE_LORE["741140723985997824"]
        joined = " | ".join(duncan.notable_stories).lower()
        assert "michigan" in joined
        assert "fake" in joined

    def test_alex_steelers_losers(self) -> None:
        alex = LEAGUE_LORE["609168618525110272"]
        teams_joined = " | ".join(alex.favorite_teams).lower()
        stories_joined = " | ".join(alex.notable_stories).lower()
        assert "steelers" in teams_joined or "steelers" in stories_joined
        # The user explicitly wanted the "losers" jab preserved.
        assert "losers" in teams_joined or "losers" in stories_joined

    def test_dominick_carries_ravens_and_unc(self) -> None:
        dom = LEAGUE_LORE["594625531702460416"]
        assert dom.display_name == "Dominick"
        teams_joined = " | ".join(dom.favorite_teams).lower()
        assert "ravens" in teams_joined
        assert dom.school == "UNC Chapel Hill"
