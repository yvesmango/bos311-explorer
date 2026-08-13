# BOS311 Explorer

BOS311 Explorer is a compact Boston 311 civic data project that turns public service-request data into a clean, queryable warehouse and a Metabase-ready analytics layer.

It is designed to be easy to read, easy to rebuild, and easy to explain to hiring managers in journalism and civic tech.

## What it does

- Ingests Boston 311 data from the City of Boston open data portal
- Normalizes records across the legacy-to-new vendor transition
- Keeps raw payloads for auditability and replay
- Loads a clean warehouse schema for analytics and dashboarding
- Supports a public-facing Metabase exploration workflow

## Repository layout

```text
bos311-explorer/
├── README.md
├── executive-summary.md
├── .gitignore
├── pipeline/
│   ├── ingest_311.py
│   └── pyproject.toml
├── sql/
│   └── schema.sql
└── docs/
    └── data_dictionary.md
```

## Data model

The warehouse uses a small, readable set of tables:

- `raw_311_tickets` for exact payload retention
- `departments` and `categories` for lookup-based normalization
- `tickets` for the clean source of truth
- `ticket_status_history` for status-change auditing

The schema keeps the important civic fields in one place:

- `open_dt`, `closed_dt`, and `sla_target_dt` for timelines
- `case_status` and `on_time` for service performance
- `latitude`, `longitude`, and `geo_point` for maps
- `subject`, `description`, and `case_topic` for the human-readable story of each ticket
- `source_system` for lineage across the vendor transition

## Pipeline

The Python pipeline lives in `pipeline/ingest_311.py` and is managed with `uv`.

From the repository root:

```bash
cd pipeline
uv run ingest_311.py
```

Helpful environment variables:

- `DATABASE_URL` for the Supabase/PostgreSQL connection string
- `CKAN_SQL_ENDPOINT` if the Boston open data endpoint changes
- `INGESTION_TARGET_YEAR` to switch the load year
- `INGESTION_BATCH_SIZE` to tune batch size
- `INGESTION_MAX_RECORDS` for capped smoke tests
- `INGESTION_MODE` for `incremental` or `backfill`
- `INGESTION_CHECKPOINT_PATH` to move the local resume file

The pipeline is idempotent and checkpoint-aware, so reruns resume from the last committed batch instead of replaying the entire source every time.

## Dashboard workflow

Metabase is the intended analysis layer for the repo.

The warehouse is the source of truth; Metabase is the read-only surface for maps, charts, tables, and future sharing.

## Scope

This repo is intentionally small:

- no dashboard app source
- no API layer
- no extra infrastructure

The point is to show a clean civic data engineering workflow, not a sprawling internal toolchain.
