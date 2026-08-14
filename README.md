# BOS311 Civic Data Explorer

**Goal**: To turn Boston's public 311 service request data into a clean, queryable warehouse that supports storyfinding, accountability journalism, and civic insight.

## Introduction

This project ingests 311 data from the City of Boston's open data portal, normalizes records across the city's vendor transition, and loads everything into a Supabase warehouse for analysis and dashboarding.

It's built for newsrooms, civic analysts, and curious residents who want to ask questions like:

- Where are overdue tickets clustering?
- Which service categories are slowest to resolve?
- Are certain neighborhoods or districts getting faster or slower responses over time?

## Data Source

The project relies on a single public dataset maintained by the City of Boston through [Analyze Boston](https://data.boston.gov/):

- **311 Service Requests** — every service request submitted by Boston residents, including issue type, location, timestamps, SLA targets, and closure details.

During the current vendor transition, the city maintains two separate endpoints (legacy and new). The pipeline ingests from both and merges them into a unified warehouse so that users see a seamless historical view regardless of which system generated a ticket.

## Methodology

A Python ETL pipeline pulls from both CKAN endpoints, applies a normalization layer, and loads the results into a single warehouse. The warehouse is built around three core principles:

1. **Abstraction over duplication** — columns like `case_topic` and `case_status` are normalized so that users never need to know which system generated a record.
2. **Hierarchy preservation** — the legacy data's 4-level taxonomy (`subject` → `reason` → `case_title` → `queue`) is preserved through hierarchical category mapping.
3. **Auditability** — every record is tagged with a `source_system` field, and raw payloads are retained so any transformation decision can be traced back to the original data.

## Key Challenges

### 1. A Hybrid, Transitional Data Source
Different services migrated from legacy to new systems on different dates, creating a fragmented mix of two taxonomies in the live API.

**Solution:** The pipeline iterates over two distinct resource IDs and tags every record with its source system, turning a messy data problem into a clean, endpoint-driven architecture.

### 2. A Hidden Data Hierarchy
Initial assumptions about the legacy schema were wrong — what looked like a simple structure turned out to be a 4‑level hierarchy.

**Solution:** We redesigned the lookup table to store hierarchical categories, allowing users to query at any level of granularity (from broad department down to specific queue).

### 3. Inconsistent Department Naming
The same department appears under slightly different names (e.g., "Public Works Department" and "Public Works Department (PWD)").

**Solution:** We documented the issue and designed a normalization layer to consolidate known variants into canonical names.


## License

MIT License