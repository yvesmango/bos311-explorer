"""Ingest Boston 311 ticket data from CKAN into Supabase/PostgreSQL.

The script ingests the target year dataset in batches:
- raw CKAN payloads are stored for auditability
- lookup tables are upserted by natural key
- ticket rows are upserted by `case_enquiry_id`
- each batch is committed independently so the run can continue past failures

Required environment variables:
- `DATABASE_URL`: PostgreSQL connection string for Supabase

Optional environment variables:
- `CKAN_SQL_ENDPOINT`: override the Boston CKAN SQL endpoint
- `CKAN_BASE_URL`: alternate Boston CKAN base URL used to derive the SQL endpoint
- `CKAN_LEGACY_RESOURCE_ID`: override the legacy CKAN datastore resource id
- `CKAN_NEW_RESOURCE_ID`: override the new-system CKAN datastore resource id
- `CKAN_RESOURCE_ID`: legacy alias for `CKAN_LEGACY_RESOURCE_ID`
- `INGESTION_TARGET_YEAR`: source year to ingest for the pilot (default: `2026`)
- `INGESTION_BATCH_SIZE`: rows to fetch per CKAN batch
- `INGESTION_MAX_RECORDS`: cap on total source rows traversed for testing
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import psycopg2
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from psycopg2.extras import Json
from urllib3.util.retry import Retry


LOGGER = logging.getLogger("bos311.ingest")
DEFAULT_CKAN_SQL_ENDPOINT = "https://data.boston.gov/api/3/action/datastore_search_sql"
DEFAULT_LEGACY_CKAN_RESOURCE_ID = "1a0b420d-99f1-4887-9851-990b2a5a6e17"
DEFAULT_NEW_CKAN_RESOURCE_ID = "254adca6-64ab-4c5c-9fc0-a6da622be185"
DEFAULT_TARGET_YEAR = 2026
DEFAULT_BATCH_SIZE = 10000
LEGACY_SOURCE_SYSTEM = "legacy_boston_311"
NEW_SOURCE_SYSTEM = "new_boston_311"


@dataclass(frozen=True)
class IngestionConfig:
    database_url: str
    ckan_sql_endpoint: str
    source_resources: tuple["SourceResource", ...]
    target_year: int
    batch_size: int
    max_records: int | None
    apply_schema: bool


@dataclass(frozen=True)
class RunSummary:
    source_rows_examined: int
    successful_rows: int
    successful_batches: int
    failed_batches: int


@dataclass(frozen=True)
class SourceResource:
    resource_id: str
    source_system: str


@dataclass(frozen=True)
class NormalizedTicket:
    case_enquiry_id: int
    source_system: str
    subject: str | None
    description: str | None
    case_topic: str | None
    department_name: str | None
    category_name: str | None
    case_status: str | None
    street_name: str | None
    neighborhood: str | None
    ward: str | None
    precinct: str | None
    city_council_district: str | None
    source: str | None
    request_type: str | None
    service_name: str | None
    assigned_team: str | None
    closure_comments: str | None
    street_number: str | None
    full_street_address: str | None
    open_dt: datetime | None
    closed_dt: datetime | None
    sla_target_dt: datetime | None
    due_date: datetime | None
    on_time: bool | None
    latitude: float | None
    longitude: float | None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-size",
        "--limit",
        "--page-size",
        dest="batch_size",
        type=int,
        default=None,
        help="Rows to fetch per CKAN batch.",
    )
    parser.add_argument(
        "--max-records",
        "--max-rows",
        type=int,
        default=None,
        help="Optional cap on the total number of source rows traversed in a run.",
    )
    parser.add_argument(
        "--target-year",
        type=int,
        default=None,
        help="Source year to ingest for the pilot. Defaults to 2026.",
    )
    parser.add_argument(
        "--apply-schema",
        action="store_true",
        help="Apply sql/schema_v1.sql before ingesting.",
    )
    return parser


def load_config(args: argparse.Namespace) -> IngestionConfig:
    load_dotenv()

    database_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is missing. Set it in your .env file to the Supabase connection string."
        )

    ckan_sql_endpoint = os.getenv("CKAN_SQL_ENDPOINT")
    if not ckan_sql_endpoint:
        ckan_base_url = os.getenv("CKAN_BASE_URL")
        if ckan_base_url:
            ckan_sql_endpoint = f"{ckan_base_url.rstrip('/')}/datastore_search_sql"
        else:
            ckan_sql_endpoint = DEFAULT_CKAN_SQL_ENDPOINT

    legacy_resource_id = (
        os.getenv("CKAN_LEGACY_RESOURCE_ID")
        or os.getenv("CKAN_RESOURCE_ID")
        or DEFAULT_LEGACY_CKAN_RESOURCE_ID
    )
    new_resource_id = os.getenv("CKAN_NEW_RESOURCE_ID") or DEFAULT_NEW_CKAN_RESOURCE_ID
    source_resources = (
        SourceResource(resource_id=legacy_resource_id, source_system=LEGACY_SOURCE_SYSTEM),
        SourceResource(resource_id=new_resource_id, source_system=NEW_SOURCE_SYSTEM),
    )

    target_year = args.target_year
    if target_year is None:
        target_year = parse_optional_int(os.getenv("INGESTION_TARGET_YEAR")) or DEFAULT_TARGET_YEAR
    if target_year < 2000:
        raise RuntimeError("INGESTION_TARGET_YEAR must be a realistic four-digit year.")

    batch_size = args.batch_size
    if batch_size is None:
        batch_size = parse_optional_int(os.getenv("INGESTION_BATCH_SIZE")) or DEFAULT_BATCH_SIZE
    if batch_size <= 0:
        raise RuntimeError("INGESTION_BATCH_SIZE must be a positive integer.")

    max_records = args.max_records
    if max_records is None:
        max_records = parse_optional_int(os.getenv("INGESTION_MAX_RECORDS"))
    if max_records is not None and max_records <= 0:
        raise RuntimeError("INGESTION_MAX_RECORDS must be a positive integer when set.")

    return IngestionConfig(
        database_url=database_url,
        ckan_sql_endpoint=ckan_sql_endpoint,
        source_resources=source_resources,
        target_year=target_year,
        batch_size=batch_size,
        max_records=max_records,
        apply_schema=args.apply_schema,
    )


def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def build_payload_index(ticket: dict[str, Any]) -> dict[str, Any]:
    return {normalize_key(str(key)): value for key, value in ticket.items()}


def payload_value(index: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = index.get(normalize_key(name))
        if value not in (None, ""):
            return value
    return None


def parse_case_enquiry_id(value: Any) -> int:
    if value in (None, ""):
        raise ValueError("CKAN ticket is missing case_enquiry_id.")
    return int(value)


def timestamp_literal(value: datetime | None) -> str:
    if value is None:
        return "TIMESTAMPTZ 'infinity'"
    normalized = value.astimezone(timezone.utc).isoformat().replace("'", "''")
    return f"TIMESTAMPTZ '{normalized}'"


def year_bounds(target_year: int) -> tuple[str, str]:
    start = datetime(target_year, 1, 1, tzinfo=timezone.utc).isoformat().replace("'", "''")
    end = datetime(target_year + 1, 1, 1, tzinfo=timezone.utc).isoformat().replace("'", "''")
    return start, end


def build_sql_query(
    resource_id: str,
    limit: int,
    target_year: int,
    cursor: tuple[datetime, int] | None,
) -> str:
    start_literal, end_literal = year_bounds(target_year)
    where_clause = f"WHERE open_dt >= TIMESTAMPTZ '{start_literal}' AND open_dt < TIMESTAMPTZ '{end_literal}' "
    if cursor is not None:
        cursor_open_dt, cursor_case_enquiry_id = cursor
        where_clause += (
            "AND "
            f"(open_dt, case_enquiry_id) > ({timestamp_literal(cursor_open_dt)}, {cursor_case_enquiry_id}) "
        )
    return (
        f'SELECT * FROM "{resource_id}" '
        f"{where_clause}"
        "ORDER BY open_dt ASC, case_enquiry_id ASC "
        f"LIMIT {limit}"
    )


def extract_from_ckan(
    session: requests.Session,
    endpoint: str,
    resource_id: str,
    target_year: int,
    limit: int,
    cursor: tuple[datetime, int] | None,
) -> list[dict[str, Any]]:
    query = build_sql_query(resource_id, limit, target_year, cursor)
    url = f"{endpoint}?{urlencode({'sql': query})}"
    LOGGER.info("Fetching CKAN batch", extra={"cursor": cursor, "limit": limit})
    response = session.get(url, timeout=120)
    response.raise_for_status()
    payload = response.json()

    if not payload.get("success"):
        raise RuntimeError(f"CKAN request failed: {json.dumps(payload, ensure_ascii=False)}")

    records = payload.get("result", {}).get("records", [])
    if not isinstance(records, list):
        raise RuntimeError("CKAN response did not contain a record list.")
    return records


def parse_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return int(stripped)


def normalize_case_status(value: Any) -> str | None:
    text = normalize_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"open", "in progress"}:
        return "open"
    if lowered == "closed":
        return "closed"
    return lowered


def normalize_on_time(value: Any) -> bool | None:
    text = normalize_text(value)
    if text is None:
        return to_bool(value)
    lowered = text.lower()
    if lowered == "ontime":
        return True
    if lowered == "overdue":
        return False
    return to_bool(value)


def normalize_report_source(value: Any) -> str | None:
    text = normalize_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered == "constituent call":
        return "call"
    if lowered == "call":
        return "call"
    return lowered


def translate_ticket(ticket: dict[str, Any], source_system: str) -> NormalizedTicket:
    index = build_payload_index(ticket)

    case_enquiry_id = parse_case_enquiry_id(
        payload_value(index, "case_enquiry_id", "case id")
    )

    if source_system == LEGACY_SOURCE_SYSTEM:
        subject = normalize_text(payload_value(index, "subject"))
        description = normalize_text(payload_value(index, "reason"))
        case_topic = normalize_text(payload_value(index, "case_title")) or description
        department_name = normalize_text(payload_value(index, "department")) or subject
        category_name = case_topic
        street_name = normalize_text(payload_value(index, "location_street_name"))
        neighborhood = normalize_text(payload_value(index, "neighborhood"))
        ward = normalize_text(payload_value(index, "ward"))
        precinct = normalize_text(payload_value(index, "precinct"))
        city_council_district = normalize_text(payload_value(index, "city_council_district"))
        source = normalize_report_source(payload_value(index, "source"))
        request_type = normalize_text(payload_value(index, "type"))
        service_name = None
        assigned_team = None
        closure_comments = None
        street_number = None
        full_street_address = None
        open_dt = to_datetime(payload_value(index, "open_dt"))
        closed_dt = to_datetime(payload_value(index, "closed_dt"))
        sla_target_dt = to_datetime(payload_value(index, "sla_target_dt"))
        due_date = to_datetime(payload_value(index, "due_date"))
        on_time = normalize_on_time(payload_value(index, "on_time"))
        latitude = to_float(payload_value(index, "latitude"))
        longitude = to_float(payload_value(index, "longitude"))
    else:
        subject = normalize_text(payload_value(index, "subject", "assigned department"))
        description = normalize_text(payload_value(index, "description"))
        case_topic = normalize_text(payload_value(index, "case_topic", "case topic", "type")) or description
        department_name = normalize_text(payload_value(index, "assigned department")) or subject
        category_name = case_topic
        street_name = normalize_text(payload_value(index, "street name", "location"))
        neighborhood = normalize_text(payload_value(index, "neighborhood"))
        ward = normalize_text(payload_value(index, "ward"))
        precinct = normalize_text(payload_value(index, "precinct"))
        city_council_district = normalize_text(payload_value(index, "city council district"))
        source = normalize_report_source(payload_value(index, "report source", "source"))
        service_name = normalize_text(payload_value(index, "service name"))
        request_type = normalize_text(payload_value(index, "type")) or service_name
        assigned_team = normalize_text(payload_value(index, "assigned team"))
        closure_comments = normalize_text(payload_value(index, "closure comments"))
        street_number = normalize_text(payload_value(index, "street number"))
        full_street_address = normalize_text(payload_value(index, "full street address", "location"))
        open_dt = to_datetime(payload_value(index, "open date"))
        closed_dt = to_datetime(payload_value(index, "close date"))
        sla_target_dt = to_datetime(payload_value(index, "target close date"))
        due_date = to_datetime(payload_value(index, "due date"))
        on_time = normalize_on_time(payload_value(index, "on time?"))
        latitude = to_float(payload_value(index, "latitude y", "latitude"))
        longitude = to_float(payload_value(index, "longitude x", "longitude"))

    return NormalizedTicket(
        case_enquiry_id=case_enquiry_id,
        source_system=source_system,
        subject=subject,
        description=description,
        case_topic=case_topic,
        department_name=department_name,
        category_name=category_name,
        case_status=normalize_case_status(payload_value(index, "case_status", "case status")),
        street_name=street_name,
        neighborhood=neighborhood,
        ward=ward,
        precinct=precinct,
        city_council_district=city_council_district,
        source=source,
        request_type=request_type,
        service_name=service_name,
        assigned_team=assigned_team,
        closure_comments=closure_comments,
        street_number=street_number,
        full_street_address=full_street_address,
        open_dt=open_dt,
        closed_dt=closed_dt,
        sla_target_dt=sla_target_dt,
        due_date=due_date,
        on_time=on_time,
        latitude=latitude,
        longitude=longitude,
    )


def to_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    return None


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"t", "true", "1", "yes", "y"}:
            return True
        if normalized in {"f", "false", "0", "no", "n"}:
            return False
    return None


def normalize_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def make_geo_point_sql(latitude: Any, longitude: Any) -> tuple[float | None, float | None]:
    lat = to_float(latitude)
    lon = to_float(longitude)
    if lat is None or lon is None:
        return None, None
    return lat, lon


def execute_schema(conn) -> None:
    schema_path = Path(__file__).resolve().parents[1] / "sql" / "schema_v1.sql"
    with schema_path.open("r", encoding="utf-8") as handle:
        schema_sql = handle.read()
    with conn.cursor() as cursor:
        for statement in split_sql_statements(schema_sql):
            LOGGER.info("Applying schema statement", extra={"statement": statement.splitlines()[0][:120]})
            cursor.execute(statement)


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_dollar_block = False
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.count("$$") % 2 == 1:
            in_dollar_block = not in_dollar_block
        current.append(line)
        if not in_dollar_block and stripped.endswith(";"):
            statement = "\n".join(current).strip()
            if statement and not statement.startswith("--"):
                statements.append(statement)
            current = []
    remainder = "\n".join(current).strip()
    if remainder:
        statements.append(remainder)
    return statements


def upsert_lookup(cursor, table: str, name: str, extra: dict[str, Any] | None = None) -> int | None:
    if not name:
        return None
    extra = extra or {}
    columns = ["name", *extra.keys()]
    values = [name, *extra.values()]
    update_clause = ", ".join(f"{column} = EXCLUDED.{column}" for column in extra.keys())
    if update_clause:
        update_clause = f"DO UPDATE SET {update_clause}"
    else:
        update_clause = "DO UPDATE SET updated_at = NOW()"
    sql = f"""
        INSERT INTO {table} ({", ".join(columns)})
        VALUES ({", ".join(["%s"] * len(values))})
        ON CONFLICT (name)
        {update_clause}
        RETURNING id
    """
    cursor.execute(sql, values)
    row = cursor.fetchone()
    return row[0] if row else None


def upsert_ticket(
    cursor,
    ticket: NormalizedTicket,
    raw_payload_id: int,
    department_id: int | None,
    category_id: int | None,
) -> None:
    latitude, longitude = make_geo_point_sql(ticket.latitude, ticket.longitude)
    geo_point_sql = (
        "ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography" if latitude is not None and longitude is not None else "NULL"
    )
    params: list[Any] = [
        ticket.case_enquiry_id,
        raw_payload_id,
        department_id,
        category_id,
        ticket.case_status,
        ticket.street_name,
        ticket.neighborhood,
        ticket.ward,
        ticket.precinct,
        ticket.city_council_district,
        ticket.source,
        ticket.description,
        ticket.subject,
        ticket.request_type,
        ticket.open_dt,
        ticket.closed_dt,
        ticket.sla_target_dt,
        ticket.due_date,
        ticket.on_time,
        latitude,
        longitude,
        ticket.source_system,
        ticket.case_topic,
        ticket.service_name,
        ticket.assigned_team,
        ticket.closure_comments,
        ticket.street_number,
        ticket.full_street_address,
    ]
    if latitude is not None and longitude is not None:
        params.extend([longitude, latitude])

    sql = f"""
        INSERT INTO tickets (
            case_enquiry_id,
            raw_payload_id,
            department_id,
            category_id,
            case_status,
            street_name,
            neighborhood,
            ward,
            precinct,
            city_council_district,
            source,
            description,
            subject,
            request_type,
            open_dt,
            closed_dt,
            sla_target_dt,
            due_date,
            on_time,
            latitude,
            longitude,
            source_system,
            case_topic,
            service_name,
            assigned_team,
            closure_comments,
            street_number,
            full_street_address,
            geo_point
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            {geo_point_sql}
        )
        ON CONFLICT (case_enquiry_id)
        DO UPDATE SET
            raw_payload_id = EXCLUDED.raw_payload_id,
            department_id = EXCLUDED.department_id,
            category_id = EXCLUDED.category_id,
            case_status = EXCLUDED.case_status,
            street_name = EXCLUDED.street_name,
            neighborhood = EXCLUDED.neighborhood,
            ward = EXCLUDED.ward,
            precinct = EXCLUDED.precinct,
            city_council_district = EXCLUDED.city_council_district,
            source = EXCLUDED.source,
            description = EXCLUDED.description,
            subject = EXCLUDED.subject,
            request_type = EXCLUDED.request_type,
            open_dt = EXCLUDED.open_dt,
            closed_dt = EXCLUDED.closed_dt,
            sla_target_dt = EXCLUDED.sla_target_dt,
            due_date = EXCLUDED.due_date,
            on_time = EXCLUDED.on_time,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            geo_point = EXCLUDED.geo_point,
            source_system = EXCLUDED.source_system,
            case_topic = EXCLUDED.case_topic,
            service_name = EXCLUDED.service_name,
            assigned_team = EXCLUDED.assigned_team,
            closure_comments = EXCLUDED.closure_comments,
            street_number = EXCLUDED.street_number,
            full_street_address = EXCLUDED.full_street_address
    """
    cursor.execute(sql, params)


def load_payload(cursor, ticket: NormalizedTicket, raw_ticket: dict[str, Any]) -> int:
    cursor.execute(
        """
        INSERT INTO raw_311_tickets (case_enquiry_id, payload)
        VALUES (%s, %s)
        ON CONFLICT (case_enquiry_id)
        DO UPDATE SET payload = EXCLUDED.payload
        RETURNING id
        """,
        (ticket.case_enquiry_id, Json(raw_ticket)),
    )
    row = cursor.fetchone()
    if not row:
        raise RuntimeError("Failed to insert raw payload.")
    return int(row[0])


def ingest_batch(cursor, rows: Iterable[dict[str, Any]]) -> int:
    ingested_count = 0
    for ticket in rows:
        normalized_ticket = translate_ticket(ticket)
        raw_payload_id = load_payload(cursor, normalized_ticket, ticket)
        department_id = (
            upsert_lookup(cursor, "departments", normalized_ticket.department_name)
            if normalized_ticket.department_name
            else None
        )
        category_id = None
        if normalized_ticket.category_name:
            category_id = upsert_lookup(
                cursor,
                "categories",
                normalized_ticket.category_name,
                extra={"department_id": department_id} if department_id else None,
            )
        upsert_ticket(cursor, normalized_ticket, raw_payload_id, department_id, category_id)
        ingested_count += 1
    return ingested_count


def ingest_resource(
    conn,
    session: requests.Session,
    ckan_sql_endpoint: str,
    resource: SourceResource,
    target_year: int,
    batch_size: int,
    max_records: int | None,
    source_rows_examined: int,
    successful_rows: int,
    successful_batches: int,
    failed_batches: int,
    starting_batch_number: int,
) -> tuple[int, int, int, int, int]:
    cursor: tuple[datetime, int] | None = None
    current_batch_size = batch_size
    batch_number = starting_batch_number

    while True:
        if max_records is not None and source_rows_examined >= max_records:
            LOGGER.info("Reached INGESTION_MAX_RECORDS cap", extra={"max_records": max_records})
            break

        remaining = None if max_records is None else max_records - source_rows_examined
        current_limit = current_batch_size if remaining is None else min(current_batch_size, remaining)
        if current_limit <= 0:
            break

        batch_number += 1
        batch_source_rows = 0
        try:
            rows = extract_from_ckan(
                session,
                ckan_sql_endpoint,
                resource.resource_id,
                target_year,
                current_limit,
                cursor,
            )
            batch_source_rows = len(rows)
            source_rows_examined += batch_source_rows
            if not rows:
                LOGGER.info(
                    "No more CKAN records after batch %s for %s.",
                    batch_number,
                    resource.source_system,
                )
                break

            with conn.cursor() as db_cursor:
                batch_success = ingest_batch(db_cursor, rows)
            conn.commit()
            successful_rows += batch_success
            successful_batches += 1
            current_batch_size = batch_size
            last_ticket = rows[-1]
            last_open_dt = to_datetime(last_ticket.get("open_dt"))
            last_case_enquiry_id = last_ticket.get("case_enquiry_id")
            if last_open_dt is None:
                raise ValueError("CKAN batch ended without an open_dt within the pilot year.")
            if last_case_enquiry_id in (None, ""):
                raise ValueError("CKAN batch ended without a case_enquiry_id.")
            cursor = (last_open_dt, int(last_case_enquiry_id))
            LOGGER.info(
                "Fetched batch %s from %s: %s records; committed %s records (running total %s).",
                batch_number,
                resource.source_system,
                batch_source_rows,
                batch_success,
                successful_rows,
            )
            if batch_source_rows < current_limit:
                LOGGER.info(
                    "Source returned a partial batch (%s of %s) for %s; ingestion is complete.",
                    batch_source_rows,
                    current_limit,
                    resource.source_system,
                )
                break
        except Exception:
            conn.rollback()
            failed_batches += 1
            if batch_source_rows == 0:
                source_rows_examined += current_limit
            LOGGER.exception(
                "Batch %s failed for %s at cursor %s. Continuing with the next batch.",
                batch_number,
                resource.source_system,
                cursor,
            )
            if current_batch_size > 1000:
                next_batch_size = max(1000, current_batch_size // 2)
                if next_batch_size != current_batch_size:
                    LOGGER.warning(
                        "Reducing batch size after failure from %s to %s to ease source load.",
                        current_batch_size,
                        next_batch_size,
                    )
                current_batch_size = next_batch_size
            time.sleep(min(30, 2 ** min(failed_batches, 5)))

    return source_rows_examined, successful_rows, successful_batches, failed_batches, batch_number


def run_ingestion_cycle(config: IngestionConfig, session: requests.Session) -> RunSummary:
    source_rows_examined = 0
    successful_rows = 0
    successful_batches = 0
    failed_batches = 0

    with psycopg2.connect(config.database_url) as conn:
        conn.autocommit = False
        if config.apply_schema:
            execute_schema(conn)
            conn.commit()

        batch_number = 0
        for resource in config.source_resources:
            LOGGER.info(
                "Starting CKAN resource %s for %s.",
                resource.resource_id,
                resource.source_system,
            )
            (
                source_rows_examined,
                successful_rows,
                successful_batches,
                failed_batches,
                batch_number,
            ) = ingest_resource(
                conn,
                session,
                config.ckan_sql_endpoint,
                resource,
                config.target_year,
                config.batch_size,
                config.max_records,
                source_rows_examined,
                successful_rows,
                successful_batches,
                failed_batches,
                batch_number,
            )

    return RunSummary(
        source_rows_examined=source_rows_examined,
        successful_rows=successful_rows,
        successful_batches=successful_batches,
        failed_batches=failed_batches,
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config(args)

    session = build_http_session()
    summary = run_ingestion_cycle(config, session)

    LOGGER.info(
        "Ingestion complete: %s records ingested across %s successful batches; %s batches failed; %s source rows examined.",
        summary.successful_rows,
        summary.successful_batches,
        summary.failed_batches,
        summary.source_rows_examined,
    )
    if summary.successful_rows == 0 and summary.failed_batches > 0:
        LOGGER.error("No records were ingested successfully and at least one batch failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
