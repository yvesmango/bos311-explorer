-- BOS311 transition-era schema migration
-- Adds the columns already applied directly in Supabase so the repo matches
-- the live database before the translator layer lands.

ALTER TABLE public.tickets
    ADD COLUMN IF NOT EXISTS source_system varchar DEFAULT 'legacy_boston_311',
    ADD COLUMN IF NOT EXISTS case_topic text,
    ADD COLUMN IF NOT EXISTS service_name varchar,
    ADD COLUMN IF NOT EXISTS assigned_team varchar,
    ADD COLUMN IF NOT EXISTS closure_comments text,
    ADD COLUMN IF NOT EXISTS street_number varchar,
    ADD COLUMN IF NOT EXISTS full_street_address text;

COMMENT ON COLUMN public.tickets.source_system IS
    'Tracks whether the record originated from the legacy 311 system or the new vendor system.';

COMMENT ON COLUMN public.tickets.case_topic IS
    'Abstracted field. Contains legacy CASE_TITLE (free text) or new Case Topic (preselected). Normalized via categories lookup table.';

COMMENT ON COLUMN public.tickets.service_name IS
    'New system field: type of service to be rendered.';

COMMENT ON COLUMN public.tickets.assigned_team IS
    'New system field: specific team within the department.';

COMMENT ON COLUMN public.tickets.closure_comments IS
    'New system field: employee comments on findings or work done to close the case.';

COMMENT ON COLUMN public.tickets.street_number IS
    'New system field: numeric street address component.';

COMMENT ON COLUMN public.tickets.full_street_address IS
    'New system field: complete street address of the reported location.';
