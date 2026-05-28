-- ============================================================================
-- admin_audit migration
-- ============================================================================
--
-- F4 (admin-portal): canonical audit log for every mutating admin action.
-- One row per call from a mutating admin lambda (test-email, reports-flag,
-- users-update, leagues-update, future actions). JSONB before/after columns
-- hold the keys that changed; the metadata column carries any per-action
-- extras (e.g. SES message_id).
--
-- This file is the historical record of the schema. Apply mechanism in v1
-- is the Supabase dashboard SQL editor (the project doesn't yet have
-- Terraform-Supabase integration). After applying, verify:
--
--     select count(*) from public.admin_audit;   -- expect 0
--
-- Reapplication is safe (idempotent guards via `if not exists`).
-- ============================================================================

create table if not exists public.admin_audit (
    id              uuid primary key default gen_random_uuid(),
    created_at      timestamptz not null default now(),
    actor_user_id   text        not null,
    action          text        not null,
    target_table    text,
    target_id       text,
    before          jsonb,
    after           jsonb,
    metadata        jsonb
);

-- Pagination index: the audit-list endpoint orders by created_at desc
-- and cursors on the timestamp of the last row of the previous page.
create index if not exists idx_admin_audit_created_at_desc
    on public.admin_audit (created_at desc);

-- Filter indexes: the audit feed UI lets admins narrow by action or actor.
create index if not exists idx_admin_audit_actor
    on public.admin_audit (actor_user_id);

create index if not exists idx_admin_audit_action
    on public.admin_audit (action);

-- RLS: read + write restricted to service_role only. Lambdas authenticate
-- with the service-role key (SSM /xomper/api/SUPABASE_SERVICE_KEY). End
-- users have zero access to audit data; admin clients fetch via the
-- /admin/audit-list lambda which gates on `is_admin`.
alter table public.admin_audit enable row level security;
