# BOS311 Data Warehouse

Boston 311 civic data warehouse inspired by The Public Ledger.

## What this is

This project turns Boston 311 service request data into a clean, queryable warehouse that can support both public-facing maps and analytics-ready datasets.

## Current baseline

- Python project scaffolded with `uv`
- Local secrets kept in `.env`
- Git repository initialized locally on `main`

## Next milestones

1. Build the ingestion pipeline from the Boston CKAN API.
2. Load data into PostgreSQL/Supabase.
3. Stand up Metabase as the primary exploration layer.

## Exploring with Metabase

See [`docs/metabase-setup.md`](docs/metabase-setup.md) for the local Docker command,
Supabase Session Pooler connection format, and the five starter questions in the
"BOS311 Explorer" collection.

The warehouse remains the source of truth; Metabase is the read-only analysis
surface for maps, charts, tables, and future embeds.

## Ingestion

The current pilot focuses on 2026 YTD only.

Run the 2026 pilot ingestion with:

```bash
uv run scripts/ingest_311.py
```

For a smaller test run, cap the source rows traversed:

```bash
INGESTION_MAX_RECORDS=1000 uv run scripts/ingest_311.py
```

To override the pilot year, set `INGESTION_TARGET_YEAR`, but we are keeping
this project on 2026 until that slice is proven stable.

The default batch size is `10000` rows. For larger future backfills, runtime
will depend on network and Supabase performance.

The fetcher uses keyset pagination plus retry/backoff so it is gentler on the
CKAN source than deep `OFFSET` paging. If the source pushes back, the script
will automatically shrink the batch size and keep moving forward.

If an ingestion run is interrupted, simply rerun the script. The current design
is idempotent thanks to `ON CONFLICT` upserts. A persisted resume marker is a
future enhancement, not a requirement for correctness today.
