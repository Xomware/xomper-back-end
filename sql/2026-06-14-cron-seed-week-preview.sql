-- 2026-06-14 — seed the missing notif_week_preview cron key
-- ==========================================================
-- admin_cron_settings_migration.sql seeded 5 rows but there are 6
-- cron-keyed notif lambdas. notif_week_preview was never seeded, so it
-- never appears in the admin cron-settings list and is therefore
-- un-toggleable from the iOS/web admin UI — it silently runs on the
-- get_cron_setting() safe default (enabled=True). This backfills the
-- missing row so every scheduled notif is visible + controllable.
--
-- Idempotent: `on conflict do nothing` leaves an existing row (and any
-- admin toggle) untouched.
--
-- Apply: run against the Supabase project SQL editor (service role).
--   select cron_key, enabled from public.admin_cron_settings order by cron_key;
--   -- expect 6 rows after this runs.

insert into public.admin_cron_settings (cron_key, enabled, test_mode, description) values
    ('notif_week_preview', true, false, 'Week preview newsletter — Wed 9am ET')
on conflict (cron_key) do nothing;
