"""
Warehouse values — compute a league's player values on demand.

Reads the nightly projections Parquet from the warehouse and applies THIS
league's scoring and roster shape. No stored grid: values are a function of
scoring_settings and roster_positions, both arbitrary, and computing one
league measured ~36 ms once the Parquet is resident.

Split of work, deliberately
---------------------------
SQL does the set-based heavy lifting: a dot product over ~216k stat rows
against up to ~45 scoring rules, then per-position ranking and value over
replacement. Both were proven byte-identical to the TypeScript engine the app
ships (xomper-frontend/tools/duckdb-spike/).

Python does one thing SQL is bad at: flex allocation. Deciding which position
gets each flex slot is a greedy simulation — for every flex seat in the
league, take whichever position's next-best available player scores highest.
That is iterative by nature, it is only a few hundred steps, and porting it
rather than approximating it is what keeps superflex correct.

Why that matters concretely: with flex ignored, a superflex league counts only
its dedicated QB slots, so QB demand is understated, QB replacement level
lands too high, and quarterbacks come out undervalued. The first draft of this
returned a running back as the most valuable asset in a superflex league,
which is the wrong answer.
"""
from typing import Any

import duckdb

VALUED_POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF")

# Slots that hold no starter.
NON_STARTING_SLOTS = frozenset({"BN", "IR", "TAXI", "RES"})

# Which positions each slot can be filled by. Mirrors slotEligibility() in
# projections.model.ts — keep the two in step.
SLOT_ELIGIBILITY: dict[str, tuple[str, ...]] = {
    "QB": ("QB",),
    "RB": ("RB",),
    "WR": ("WR",),
    "TE": ("TE",),
    "K": ("K",),
    "DEF": ("DEF",),
    "DST": ("DEF",),
    "FLEX": ("RB", "WR", "TE"),
    "WRRB_FLEX": ("RB", "WR"),
    "REC_FLEX": ("WR", "TE"),
    "WRTE_FLEX": ("WR", "TE"),
    "SUPER_FLEX": ("QB", "RB", "WR", "TE"),
}


def _scoring_rows(scoring: dict[str, float]) -> list[tuple[str, float]]:
    """Real scoring rules only — adp_* and pts_* are metadata, not inputs."""
    return [
        (key, float(weight))
        for key, weight in scoring.items()
        if key not in ("gp", "gms_active")
        and not key.startswith(("adp_", "pts_"))
    ]


def score_players(
    con: duckdb.DuckDBPyConnection,
    parquet_uri: str,
    scoring: dict[str, float],
    ppr: float,
) -> list[tuple[str, str, float]]:
    """Apply the league's scoring to every projected player."""
    con.execute("CREATE OR REPLACE TABLE sc(stat_key VARCHAR, weight DOUBLE)")
    rows = _scoring_rows(scoring)
    if rows:
        con.executemany("INSERT INTO sc VALUES (?, ?)", rows)

    con.execute(f"""
        CREATE OR REPLACE TABLE stat_long AS
        SELECT * FROM read_parquet('{parquet_uri}')
    """)

    # K and DEF fall back to Sleeper's precomputed totals: their scoring keys
    # (FG distance buckets, points-allowed tiers) are mostly absent from
    # projections, and a partial dot product would quietly under-count them.
    con.execute(f"""
        CREATE OR REPLACE TABLE scored AS
        WITH dot AS (
          SELECT player_id,
                 sum(stat_value * weight) AS pts,
                 count(*)                 AS matched
          FROM stat_long JOIN sc USING (stat_key)
          WHERE stat_value IS NOT NULL
          GROUP BY player_id
        ),
        pre AS (
          SELECT DISTINCT player_id, position,
            max(CASE WHEN stat_key = 'pts_std'      THEN stat_value END)
              OVER (PARTITION BY player_id) AS std,
            max(CASE WHEN stat_key = 'pts_half_ppr' THEN stat_value END)
              OVER (PARTITION BY player_id) AS half,
            max(CASE WHEN stat_key = 'pts_ppr'      THEN stat_value END)
              OVER (PARTITION BY player_id) AS full
          FROM stat_long
        )
        -- coalesce to 0, not NULL. A player with neither matched scoring
        -- stats nor a precomputed total is projected for nothing, and a NULL
        -- here propagates into the ranking and out through the API.
        SELECT p.player_id, p.position,
               coalesce(
                 CASE
                   WHEN p.position IN ('K', 'DEF') OR coalesce(d.matched, 0) = 0
                     THEN CASE WHEN {ppr} >= 0.75 THEN p.full
                               WHEN {ppr} >= 0.25 THEN p.half
                               ELSE p.std END
                   ELSE d.pts
                 END, 0) AS points
        FROM pre p LEFT JOIN dot d USING (player_id)
    """)

    return con.execute(
        "SELECT player_id, position, coalesce(points, 0) FROM scored"
    ).fetchall()


def starters_by_position(
    roster_positions: list[str],
    num_teams: int,
    scored: list[tuple[str, str, float]],
) -> dict[str, int]:
    """
    How many starters of each position the whole league fields.

    Port of startersByPosition() in vor.model.ts, including the greedy flex
    simulation. Approximating flex with a fixed split gets superflex wrong.
    """
    counts = {p: 0 for p in VALUED_POSITIONS}
    flex_slots: list[str] = []

    for raw in roster_positions:
        slot = raw.upper()
        if slot in NON_STARTING_SLOTS:
            continue
        eligible = SLOT_ELIGIBILITY.get(slot)
        if not eligible:
            continue
        if len(eligible) == 1:
            counts[eligible[0]] += num_teams
        else:
            flex_slots.append(slot)

    if not flex_slots:
        return counts

    by_position: dict[str, list[float]] = {p: [] for p in VALUED_POSITIONS}
    for _, position, points in scored:
        if position in by_position:
            by_position[position].append(points)
    for pool in by_position.values():
        pool.sort(reverse=True)

    taken = dict(counts)

    # Each flex seat in the league goes to whichever eligible position has the
    # best player still on the board. In superflex the marginal seat goes to a
    # QB; in a TE-premium league it can go to a tight end.
    for slot in flex_slots:
        eligible = SLOT_ELIGIBILITY[slot]
        for _ in range(num_teams):
            best_pos = None
            best_points = float("-inf")
            for position in eligible:
                pool = by_position.get(position) or []
                index = taken.get(position, 0)
                if index < len(pool) and pool[index] > best_points:
                    best_points = pool[index]
                    best_pos = position
            if best_pos is None:
                break
            taken[best_pos] += 1

    return taken


def values_for(
    con: duckdb.DuckDBPyConnection,
    starters: dict[str, int],
) -> list[dict[str, Any]]:
    """Value over replacement, scaled so the best asset sits at 10000."""
    con.execute("CREATE OR REPLACE TABLE st(position VARCHAR, n INTEGER)")
    con.executemany("INSERT INTO st VALUES (?, ?)", list(starters.items()))

    # Replacement is the best player who does NOT start — rank N+1 for N
    # starters, since row_number() is 1-based. Using rank N instead picks the
    # last starter and shifts every value at that position.
    con.execute("""
        CREATE OR REPLACE TABLE valued AS
        WITH ranked AS (
          SELECT player_id, position, points,
                 row_number() OVER (PARTITION BY position ORDER BY points DESC) AS rk,
                 count(*)     OVER (PARTITION BY position)                      AS cnt
          FROM scored
        ),
        levels AS (
          SELECT r.position,
                 max(CASE WHEN r.rk = CASE
                             WHEN coalesce(st.n, 0) <= 0 THEN 1
                             ELSE least(coalesce(st.n, 0) + 1, r.cnt)
                           END
                          THEN r.points END) AS lvl
          FROM ranked r LEFT JOIN st USING (position)
          GROUP BY r.position
        ),
        v AS (
          SELECT r.player_id, r.position, r.points,
                 r.points - coalesce(l.lvl, 0) AS vor
          FROM ranked r LEFT JOIN levels l USING (position)
        )
        SELECT player_id, position, points, vor,
               CASE WHEN (SELECT greatest(max(vor), 0) FROM v) > 0
                    THEN greatest(0, round(vor * 10000.0 / (SELECT max(vor) FROM v)))
                    ELSE 0 END AS value
        FROM v
    """)

    return [
        {
            "playerId": row[0],
            "position": row[1],
            "points": round(row[2], 2),
            "value": int(row[4]),
        }
        for row in con.execute(
            "SELECT player_id, position, points, vor, value FROM valued"
        ).fetchall()
    ]
