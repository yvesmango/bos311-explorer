-- BOS311 Explorer schema
-- Canonical warehouse schema for raw, normalized, and audit tables.

DROP FUNCTION IF EXISTS set_updated_at();

DROP TABLE IF EXISTS ticket_status_history CASCADE;
DROP TABLE IF EXISTS tickets CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS raw_311_tickets CASCADE;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS raw_311_tickets (
    id BIGSERIAL PRIMARY KEY,
    case_enquiry_id BIGINT NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE raw_311_tickets IS 'Raw Boston 311 payloads stored for auditability and replay.';
COMMENT ON COLUMN raw_311_tickets.payload IS 'Exact JSON payload received from the CKAN API.';

CREATE TABLE IF NOT EXISTS departments (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE departments IS 'Normalized department lookup table.';
COMMENT ON COLUMN departments.name IS 'Canonical department name.';

CREATE TABLE IF NOT EXISTS categories (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    department_id BIGINT REFERENCES departments(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE categories IS 'Normalized ticket category lookup table.';
COMMENT ON COLUMN categories.department_id IS 'Optional owning department for the category.';

CREATE TABLE IF NOT EXISTS tickets (
    id BIGSERIAL PRIMARY KEY,
    case_enquiry_id BIGINT NOT NULL UNIQUE,
    raw_payload_id BIGINT REFERENCES raw_311_tickets(id) ON DELETE SET NULL,
    department_id BIGINT REFERENCES departments(id) ON DELETE SET NULL,
    category_id BIGINT REFERENCES categories(id) ON DELETE SET NULL,
    case_status TEXT,
    street_name TEXT,
    neighborhood TEXT,
    ward TEXT,
    precinct TEXT,
    city_council_district TEXT,
    source TEXT,
    description TEXT,
    subject TEXT,
    request_type TEXT,
    open_dt TIMESTAMPTZ,
    closed_dt TIMESTAMPTZ,
    sla_target_dt TIMESTAMPTZ,
    due_date TIMESTAMPTZ,
    on_time BOOLEAN,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    geo_point GEOGRAPHY(POINT, 4326),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_system VARCHAR NOT NULL DEFAULT 'legacy_boston_311',
    case_topic TEXT,
    service_name VARCHAR,
    assigned_team VARCHAR,
    closure_comments TEXT,
    street_number VARCHAR,
    full_street_address TEXT
);

COMMENT ON TABLE tickets IS 'Clean source-of-truth ticket records normalized from raw Boston 311 data.';
COMMENT ON COLUMN tickets.geo_point IS 'PostGIS geography point built from latitude and longitude.';
COMMENT ON COLUMN tickets.case_enquiry_id IS 'Natural key for upserts and lineage.';
COMMENT ON COLUMN tickets.on_time IS 'Whether the request met its SLA target.';
COMMENT ON COLUMN tickets.source_system IS 'Tracks whether the record originated from the legacy or new 311 system.';
COMMENT ON COLUMN tickets.case_topic IS 'Abstracted issue title used for the citizen-facing story of the ticket.';
COMMENT ON COLUMN tickets.service_name IS 'New-system service name.';
COMMENT ON COLUMN tickets.assigned_team IS 'New-system team routing field.';
COMMENT ON COLUMN tickets.closure_comments IS 'New-system closure notes.';
COMMENT ON COLUMN tickets.street_number IS 'New-system street number field.';
COMMENT ON COLUMN tickets.full_street_address IS 'New-system full location string.';

CREATE TABLE IF NOT EXISTS ticket_status_history (
    id BIGSERIAL PRIMARY KEY,
    ticket_id BIGINT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    case_status TEXT NOT NULL,
    status_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE ticket_status_history IS 'Status change audit log for ticket lifecycle analysis.';

CREATE INDEX IF NOT EXISTS idx_tickets_geo
    ON tickets
    USING GIST (geo_point);

CREATE INDEX IF NOT EXISTS idx_tickets_status_date
    ON tickets (case_status, open_dt DESC);

CREATE INDEX IF NOT EXISTS idx_tickets_council_district
    ON tickets (city_council_district);

CREATE INDEX IF NOT EXISTS idx_tickets_department
    ON tickets (department_id);

CREATE INDEX IF NOT EXISTS idx_ticket_status_history_ticket_id
    ON ticket_status_history (ticket_id, status_changed_at DESC);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_departments_updated_at ON departments;
CREATE TRIGGER trg_departments_updated_at
BEFORE UPDATE ON departments
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_categories_updated_at ON categories;
CREATE TRIGGER trg_categories_updated_at
BEFORE UPDATE ON categories
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_tickets_updated_at ON tickets;
CREATE TRIGGER trg_tickets_updated_at
BEFORE UPDATE ON tickets
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
