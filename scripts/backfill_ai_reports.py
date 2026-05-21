#!/usr/bin/env python3
"""
AI Review backfill driver
=========================
Drives historical post-draft + weekly AI Review generation against
the deployed admin endpoints. Used once-ish to seed the 2024 + 2025
report archive (and stress-test the Claude prompts against real
finished-season data) ahead of the 2026 launch.

This script is OPT-IN and manually run. It never auto-fires from CI
and never touches infra. Re-runnable (use --force to overwrite
existing reports; defaults to dry-run mode for safety).

Usage
-----
    # Required env vars
    export XOMPER_API_BASE_URL="https://api.xomper.xomware.com"
    export XOMPER_ADMIN_JWT="eyJ..."            # JWT from Supabase auth
    export XOMPER_ADMIN_SLEEPER_USER_ID="..."   # admin sleeper_user_id

    # Dry-run (sends only to Dominick — tone calibration)
    python3 scripts/backfill_ai_reports.py --dry-run

    # Real backfill (broadcasts to all 12 — only do this once)
    python3 scripts/backfill_ai_reports.py --no-dry-run --force

    # Just 2024, just postDraft
    python3 scripts/backfill_ai_reports.py --years 2024 --postdraft-only

    # Just one week
    python3 scripts/backfill_ai_reports.py --years 2025 --weeks 5

Flags
-----
    --years            comma-separated, default "2024,2025"
    --weeks            comma-separated, default "1-17"
    --postdraft-only   skip weekly calls
    --weekly-only      skip post-draft calls
    --dry-run          send only to Dominick (DEFAULT — safe)
    --no-dry-run       broadcast to all 12 managers (be sure)
    --force            overwrite existing reports for this period
    --sleep            seconds between calls (default 1.0)
    --timeout          request timeout in seconds (default 300)

Cost estimate
-------------
Per the F1 / F3 plans, each Claude Haiku 4.5 call is ~$0.02. For
2 years x (1 postDraft + ~17 weekly) = ~36 calls, total ~$0.72.
The script prints a running total as it goes.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

import requests

# Per-call cost estimate (USD). Conservative — Haiku 4.5 with
# heavy prompt-cache reuse trends below this in practice.
COST_PER_CALL_USD = 0.02


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill AI Review reports for past seasons.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--years",
        default="2024,2025",
        help="Comma-separated calendar years to backfill (default 2024,2025).",
    )
    p.add_argument(
        "--weeks",
        default="1-17",
        help=(
            "Comma-separated week list or range. Default '1-17' "
            "covers the full regular season. Use '1-22' to include "
            "playoffs."
        ),
    )
    p.add_argument(
        "--postdraft-only",
        action="store_true",
        help="Only run post-draft (skip weekly recaps).",
    )
    p.add_argument(
        "--weekly-only",
        action="store_true",
        help="Only run weekly recaps (skip post-draft).",
    )
    p.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="Send only to the admin (default).",
    )
    p.add_argument(
        "--no-dry-run",
        dest="dry_run",
        action="store_false",
        help="Broadcast to all 12 managers.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing reports for this period.",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Seconds to sleep between calls (default 1.0).",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Request timeout in seconds (default 300).",
    )
    return p.parse_args()


def _parse_year_list(raw: str) -> list[int]:
    out: list[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        out.append(int(chunk))
    return out


def _parse_week_list(raw: str) -> list[int]:
    out: list[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            lo, hi = chunk.split("-", 1)
            for w in range(int(lo), int(hi) + 1):
                out.append(w)
        else:
            out.append(int(chunk))
    return out


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        sys.stderr.write(
            f"\nERROR: missing required env var {name!r}.\n"
            f"Set it before running this script:\n"
            f"    export {name}=...\n\n"
        )
        sys.exit(2)
    return value


def _post(
    *,
    url: str,
    body: dict[str, Any],
    headers: dict[str, str],
    timeout: int,
) -> tuple[int, dict[str, Any]]:
    try:
        response = requests.post(
            url, json=body, headers=headers, timeout=timeout
        )
    except requests.RequestException as err:
        return 0, {"_request_error": str(err)}
    try:
        payload = response.json()
    except ValueError:
        payload = {"_raw": response.text}
    return response.status_code, payload


def _summary(status: int, payload: dict[str, Any]) -> str:
    if status == 0:
        return f"NET FAIL: {payload.get('_request_error')!r}"
    if status >= 400:
        msg = (
            payload.get("Message")
            or payload.get("message")
            or payload.get("error")
            or payload.get("_raw")
            or ""
        )
        return f"HTTP {status} — {str(msg)[:120]}"
    token_usage = payload.get("token_usage") or {}
    in_tok = token_usage.get("input_tokens")
    out_tok = token_usage.get("output_tokens")
    return (
        f"OK status={payload.get('status')} "
        f"period={payload.get('period')} "
        f"delivery={payload.get('delivery_count')} "
        f"in={in_tok} out={out_tok}"
    )


def main() -> int:
    args = _parse_args()

    base_url = _require_env("XOMPER_API_BASE_URL").rstrip("/")
    jwt = _require_env("XOMPER_ADMIN_JWT")
    sleeper_user_id = _require_env("XOMPER_ADMIN_SLEEPER_USER_ID")

    years = _parse_year_list(args.years)
    weeks = _parse_week_list(args.weeks)

    if not years:
        sys.stderr.write("ERROR: --years produced an empty list\n")
        return 2
    if not args.postdraft_only and not weeks:
        sys.stderr.write("ERROR: --weeks produced an empty list\n")
        return 2

    headers = {
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
        # Both supported by the admin gate — pass both so the call
        # works regardless of authorizer claim shape on the deployed
        # API GW.
        "X-Sleeper-User-Id": sleeper_user_id,
    }

    postdraft_url = f"{base_url}/admin/ai-review-postdraft-trigger"
    weekly_url = f"{base_url}/admin/ai-review-weekly-trigger"

    # The "current" active league is the 2026 league. seasons_back for
    # weekly: 2026 -> 0 hops, 2025 -> 1 hop, 2024 -> 2 hops, etc.
    # The user can override this by adjusting the year list.
    def _seasons_back_for(year: int) -> int:
        current = 2026
        diff = current - year
        return max(0, diff)

    total_calls = 0
    ok_calls = 0
    print(
        f"\nBackfill driver — base={base_url} dry_run={args.dry_run} "
        f"force={args.force}"
    )
    print(f"  years={years} weeks={weeks if not args.postdraft_only else '[skipped]'}")
    print()

    for year in years:
        # --- POST DRAFT ----
        if not args.weekly_only:
            body = {
                "sleeper_user_id": sleeper_user_id,
                "dry_run": args.dry_run,
                "force": args.force,
                "target_year": year,
            }
            print(
                f"[postDraft {year}] POST {postdraft_url} "
                f"body={json.dumps({k: v for k, v in body.items() if k != 'sleeper_user_id'})}"
            )
            status, payload = _post(
                url=postdraft_url,
                body=body,
                headers=headers,
                timeout=args.timeout,
            )
            total_calls += 1
            if 200 <= status < 300:
                ok_calls += 1
            print(f"  -> {_summary(status, payload)}")
            time.sleep(args.sleep)

        # --- WEEKLY ----
        if not args.postdraft_only:
            sb = _seasons_back_for(year)
            for week in weeks:
                body = {
                    "sleeper_user_id": sleeper_user_id,
                    "week": week,
                    "dry_run": args.dry_run,
                    "force": args.force,
                    "seasons_back": sb,
                }
                print(
                    f"[weekly {year}W{week:02d}] POST {weekly_url} "
                    f"seasons_back={sb}"
                )
                status, payload = _post(
                    url=weekly_url,
                    body=body,
                    headers=headers,
                    timeout=args.timeout,
                )
                total_calls += 1
                if 200 <= status < 300:
                    ok_calls += 1
                print(f"  -> {_summary(status, payload)}")
                time.sleep(args.sleep)

    est_cost = total_calls * COST_PER_CALL_USD
    print()
    print(f"DONE — {ok_calls}/{total_calls} calls succeeded.")
    print(f"Estimated cost: ~${est_cost:.2f} ({total_calls} calls * ${COST_PER_CALL_USD:.3f}).")
    if total_calls - ok_calls:
        print(
            f"WARNING: {total_calls - ok_calls} call(s) returned non-2xx — "
            f"re-run with --force to retry."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
