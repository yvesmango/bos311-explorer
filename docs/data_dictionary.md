# Data Dictionary

This project keeps a small warehouse schema on purpose. The goal is clarity: one raw table for auditability, one clean ticket table for analysis, and a couple of lookup tables for normalization.

## Tables

### `raw_311_tickets`

Stores the exact JSON payload received from the Boston 311 source.

- `id`: surrogate primary key
- `case_enquiry_id`: raw external ticket identifier
- `payload`: exact CKAN payload for replay and auditing
- `ingested_at`: when the record entered the warehouse

### `departments`

Canonical department names used for clean joins and filtering.

- `id`: surrogate primary key
- `name`: canonical department name
- `created_at`, `updated_at`: audit timestamps

### `categories`

Normalized issue categories linked to departments.

- `id`: surrogate primary key
- `name`: canonical category name
- `department_id`: optional owning department
- `created_at`, `updated_at`: audit timestamps

### `tickets`

The clean source of truth for analysis and dashboards.

- `case_enquiry_id`: natural key used for upserts
- `raw_payload_id`: lineage back to `raw_311_tickets`
- `department_id`, `category_id`: normalized lookup keys
- `case_status`: normalized status vocabulary
- `street_name`, `neighborhood`, `ward`, `precinct`, `city_council_district`: geography and jurisdiction fields
- `source`: report source
- `description`: legacy `reason` field
- `subject`: broad department or service area
- `case_topic`: human-readable issue title
- `request_type`: request classification
- `open_dt`, `closed_dt`, `sla_target_dt`, `due_date`: timeline fields
- `on_time`: SLA compliance flag
- `latitude`, `longitude`, `geo_point`: map-ready location fields
- `source_system`: `legacy_boston_311` or `new_boston_311`
- `service_name`, `assigned_team`, `closure_comments`, `street_number`, `full_street_address`: transition-era fields from the newer vendor
- `created_at`, `updated_at`: warehouse timestamps

### `ticket_status_history`

Tracks status changes over time for audit and lifecycle analysis.

- `id`: surrogate primary key
- `ticket_id`: foreign key to `tickets`
- `case_status`: status value at that point in time
- `status_changed_at`: when the status changed
- `created_at`: audit timestamp

## Important field meanings

- `open_dt` is the ticket’s real service timestamp and drives timelines, trend lines, and recency filters.
- `created_at` is when the warehouse row was written, so it is useful for auditing but not for civic analysis.
- `subject` is the broad service area.
- `description` stores the legacy `reason` field and provides issue context.
- `case_topic` is the human-readable issue title used in dashboards and map popups.
- `source_system` shows whether a row came from the legacy or new 311 vendor.
- `geo_point` is the PostGIS geography point used for mapping.

## Transition mapping

During the vendor transition, the pipeline normalizes both source systems into the same warehouse fields.

| Warehouse field | Legacy source | New source |
|---|---|---|
| `case_enquiry_id` | `case_enquiry_id` | datastore `_id` |
| `open_dt` | `open_dt` | `open_date` |
| `closed_dt` | `closed_dt` | `close_date` |
| `sla_target_dt` | `sla_target_dt` | `target close date` |
| `on_time` | boolean or `t`/`f` | `On Time?` |
| `case_status` | `case_status` | `Case Status` |
| `subject` | `subject` | `assigned department` or similar department field |
| `description` | `reason` | `description` |
| `case_topic` | `case_title` | `case topic` |
| `source` | `source` | `report source` |
| `neighborhood` | `neighborhood` | `neighborhood` |

The warehouse keeps the clean fields stable so downstream reporting does not need to care which vendor produced the original ticket.
