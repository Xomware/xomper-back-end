-- ============================================================================
-- league_announcements migration
-- ============================================================================
--
-- announcements feature (#100): admin-editable league announcements that
-- replace the hardcoded `LeagueAnnouncements.current` array shipped in
-- Season Refocus F2. Public read endpoint (`GET /announcements/list`)
-- returns active+unexpired rows; admin CRUD endpoints
-- (`/admin/announcements-{list,create,update,delete}`) drive the iOS
-- admin sub-screen.
--
-- This file is the historical record of the schema. Apply mechanism (v1)
-- is the Supabase dashboard SQL editor (the project doesn't yet have
-- Terraform-Supabase integration). After applying, verify:
--
--     select count(*) from public.league_announcements;   -- expect >= 3
--
-- Reapplication is safe (idempotent guards via `if not exists` /
-- `on conflict do nothing`). Seed UUIDs are deterministic so a re-run
-- won't duplicate the three preserved announcements.
-- ============================================================================

create table if not exists public.league_announcements (
    id            uuid        primary key default gen_random_uuid(),
    title         text        not null,
    body          text        not null,
    priority      text        not null default 'info'
                              check (priority in ('critical', 'info')),
    expires_at    timestamptz,
    is_active     boolean     not null default true,
    display_order int         not null default 0,
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now()
);

-- Partial index speeds up the public-read filter
-- (is_active=true AND (expires_at IS NULL OR expires_at > now())).
create index if not exists idx_announcements_active
    on public.league_announcements (is_active, expires_at)
    where is_active = true;

-- Seed three deterministic-UUID rows that mirror the current hardcoded
-- LeagueAnnouncements.current entries so Landing content is preserved on
-- cutover. `on conflict (id) do nothing` keeps this idempotent: re-running
-- the migration won't clobber any edits the admin has already made.
insert into public.league_announcements
    (id, title, body, priority, expires_at, is_active, display_order)
values
    (
        '00000000-0000-0000-0000-000000000001',
        '2026 Rookie Draft',
        'July 6, 2026 — 6:30pm ET sharp. ~1 day per pick.',
        'critical',
        '2026-07-07 00:00:00+00',
        true,
        0
    ),
    (
        '00000000-0000-0000-0000-000000000002',
        '2026 Season Start',
        'Week 1 kicks off Sunday September 8, 2026. Set your lineups.',
        'info',
        '2026-09-09 00:00:00+00',
        true,
        1
    ),
    (
        '00000000-0000-0000-0000-000000000003',
        'Rule Proposals',
        'Reverse-HPP draft order proposal is in the Rules tray. Vote / give feedback.',
        'info',
        null,
        true,
        2
    )
on conflict (id) do nothing;

-- RLS: read + write restricted to service_role only. Lambdas authenticate
-- with the service-role key (SSM /xomper/api/SUPABASE_SERVICE_KEY). End
-- users have zero direct table access; the public-read endpoint is
-- JWT-gated and the admin endpoints additionally enforce `is_admin`.
alter table public.league_announcements enable row level security;
