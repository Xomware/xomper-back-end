-- ============================================================================
-- admin_cron_settings migration
-- ============================================================================
--
-- admin-cron-settings: per-lambda kill switch + test-mode flag for each
-- scheduled notif lambda. One row per lambda. Read by every scheduled
-- lambda on cold start via `lambdas/common/cron_settings.get_cron_setting`
-- and updated by the admin endpoints `api_admin_cron_settings_list` +
-- `api_admin_cron_settings_update`.
--
-- This file is the historical record of the schema. Apply mechanism (v1)
-- is the Supabase dashboard SQL editor (the project doesn't yet have
-- Terraform-Supabase integration). After applying, verify:
--
--     select count(*) from public.admin_cron_settings;   -- expect 5
--
-- Reapplication is safe (idempotent guards via `if not exists` /
-- `on conflict do nothing`).
-- ============================================================================

create table if not exists public.admin_cron_settings (
    cron_key     text        primary key,
    enabled      boolean     not null default true,
    test_mode    boolean     not null default false,
    description  text,
    updated_at   timestamptz not null default now()
);

-- Seed the five scheduled notif lambdas. `on conflict do nothing` keeps
-- this idempotent: re-running the migration won't clobber any setting
-- the admin has already toggled.
insert into public.admin_cron_settings (cron_key, enabled, test_mode, description) values
    ('notif_weekly_recap',       true, false, 'Weekly matchup recap — Tue 9am ET'),
    ('notif_lineup_not_set',     true, false, 'Lineup-not-set alerts — Sun 11am ET'),
    ('notif_close_game_alert',   true, false, 'Close-game alerts — Sun + Mon 8pm ET'),
    ('notif_worldcup_movement',  true, false, 'World Cup transitions — Tue 10am ET'),
    ('notif_ai_review_weekly',   true, false, 'AI weekly newsletter — Tue 2pm ET')
on conflict (cron_key) do nothing;

-- RLS: read + write restricted to service_role only. Lambdas authenticate
-- with the service-role key (SSM /xomper/api/SUPABASE_SERVICE_KEY). End
-- users have zero access to cron settings; admin clients fetch + patch
-- via the admin-gated /admin/cron-settings-list + /admin/cron-settings-update
-- lambdas which gate on `is_admin`.
alter table public.admin_cron_settings enable row level security;
