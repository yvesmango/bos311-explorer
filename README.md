# BOS311 Civic Data Explorer

BOS311 Explorer is a compact Boston 311 civic data project that turns public service-request data from the City of Boston's open data portal into a clean, queryable warehouse and a Metabase-ready analytics layer.


## What it does

- Ingests Boston 311 data from the City of Boston open data portal
- Normalizes records across the legacy-to-new vendor transition
- Keeps raw payloads for auditability and replay
- Loads a clean warehouse schema for analytics and dashboarding
- Supports a public-facing Metabase exploration workflow
- Keeps the runnable ingest script in `pipeline/ingest_311.py`
- Publishes the ingest pipeline as a Markdown walkthrough in `pipeline/ingest_311.md`



## Data Sources

This project relies on a single public dataset maintained by the City of Boston through the [Analyze Boston](https://data.boston.gov/) open data portal, powered by CKAN:

- **311 Service Requests** — every service request submitted by Boston residents, including issue type, location, timestamps, SLA targets, and closure details.

During the current vendor transition, the city maintains **two distinct resource endpoints** under the same dataset umbrella:

| Resource | Status |
|---|---|
| Legacy 311 system (`1a0b420d-...`) | Historical data through mid-2026 |
| New 311 system (`254adca6-...`) | Live data as services migrate |

The pipeline ingests from both endpoints and merges them into a single, unified warehouse so that users see a seamless historical view regardless of which vendor generated each ticket.

---

## Methodology

## Local Run

From the repository root:

```bash
cd pipeline
caffeinate -d -- uv run ingest_311.py
```

If you prefer to stay at the repo root, use:

```bash
caffeinate -d -- uv run --project pipeline ingest_311.py
```

### The Unified Civic Archive

Rather than building separate pipelines for the legacy and new systems, this project treats them as two dialects of the same language. A Python-based ETL pipeline pulls from both CKAN resource IDs, applies a normalization layer, and loads the results into a single Supabase data warehouse.

The warehouse is built around three core principles:

1. **Abstraction over duplication.** Columns like `case_topic` and `case_status` are normalized so that a journalist querying the warehouse never needs to know which system generated a record.
2. **Hierarchy preservation.** The legacy data's 4-level taxonomy (`subject` → `reason` → `case_title` → `queue`) is preserved through hierarchical category mapping, allowing users to query at whatever level of granularity they need.
3. **Auditability.** Every record is tagged with a `source_system` field, and raw payloads are retained in a JSONB staging table so any transformation decision can be traced back to the original data.

### The Dashboard

The Metabase dashboard is split into two tabs:

- **Volume Tracker** — topline KPIs, trend comparisons, and geographic heatmaps for the big picture.
- **Ticket Explorer** — deep-dive charts into departments, case topics, neighborhoods, and SLA compliance.

Every visualization is interactive and filterable by date range, status, and neighborhood.

---

## Key Challenges

### 1. A Hybrid, Transitional Data Source

Boston's 311 system is migrating from a legacy vendor to a new one on a staggered schedule through late 2026. Different services transitioned on different dates, meaning the live CKAN API returns a fragmented mix of two taxonomies.

**How we overcame it:** Instead of building fragile row-by-row detection logic, the pipeline iterates over two distinct resource IDs and tags every record with its source system. This turns a messy data problem into a clean, endpoint-driven ETL architecture that scales as more services migrate.

### 2. A Hidden Data Hierarchy

Initial assumptions about the legacy schema were wrong. Data profiling revealed a 4-level hierarchy (`subject` → `reason` → `case_title` → `queue`) that wasn't obvious from column names alone. `case_title` and `type` turned out to be duplicates; `subject` was the broad department, not a description; `reason` was the sub-department context.

**How we overcame it:** We redesigned the `categories` lookup table to store hierarchical, concatenated categories (e.g., "Housing - Pest Infestation - Residential") so that journalists can query at any level of granularity. The map popup was redesigned to surface only the human-readable layers.


### 3. Inconsistent Department Naming

The legacy CKAN data stores the same departments under slightly different names (e.g., "Public Works Department" and "Public Works Department (PWD)"), fragmenting accountability metrics.

**How we overcame it:** We documented the issue and designed a normalization layer in the ingestion script to consolidate known variants into canonical names. This fix is staged for the next pipeline run and will immediately clean up the dashboard's department-level metrics.
