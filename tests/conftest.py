"""Shared pytest fixtures.

The SQL-based tests run against the live Postgres configured in .env, but inside a throwaway
schema that is created and dropped per test, so the real `augury_exam` schema is never touched.
If the database is unreachable, those tests skip (they do not fail).
"""
from uuid import uuid4

import pandas as pd
import pytest

from pg_connector import PostgresConnector


# --- DDL for the throwaway test schema (kept inline so tests do not depend on sql_tables/*.sql) ---
def _table_ddls(schema: str) -> list[str]:
    return [
        f"""
        CREATE TABLE {schema}.raw_events (
            event_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            sensor_id TEXT,
            event_ts TIMESTAMP NOT NULL,
            ingestion_ts TIMESTAMP NOT NULL,
            metric TEXT,
            value DOUBLE PRECISION,
            unit TEXT,
            factory_id TEXT,
            line_id TEXT,
            work_order_id TEXT,
            status TEXT,
            machine_type TEXT,
            priority TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
        f"""
        CREATE TABLE {schema}.dead_letter_events (
            event_id TEXT,
            machine_id TEXT,
            reason TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
        f"""
        CREATE TABLE {schema}.machine_signal_hourly (
            machine_id TEXT NOT NULL,
            metric TEXT NOT NULL,
            hour_start TIMESTAMP NOT NULL,
            sample_count INTEGER NOT NULL,
            avg_value DOUBLE PRECISION,
            min_value DOUBLE PRECISION,
            max_value DOUBLE PRECISION,
            last_event_ts TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (machine_id, metric, hour_start)
        );
        """,
        f"""
        CREATE TABLE {schema}.context_edges (
            edge_id TEXT PRIMARY KEY,
            source_node_id TEXT NOT NULL,
            source_node_type TEXT NOT NULL,
            relationship TEXT NOT NULL,
            target_node_id TEXT NOT NULL,
            target_node_type TEXT NOT NULL,
            valid_from TIMESTAMP NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
    ]


@pytest.fixture
def require_db():
    """Skip the test if the configured Postgres is not reachable."""
    try:
        PostgresConnector.execute_query("SELECT 1", return_df=True)
    except Exception as exc:  # psycopg2 OperationalError, pool failures, etc.
        pytest.skip(f"Postgres not available: {exc}")


@pytest.fixture
def test_schema(require_db):
    """Create an isolated schema with the 4 pipeline tables; drop it on teardown."""
    schema = f"test_augury_{uuid4().hex[:8]}"
    PostgresConnector.execute_query(f"CREATE SCHEMA {schema}")
    try:
        for ddl in _table_ddls(schema):
            PostgresConnector.execute_query(ddl)
        yield schema
    finally:
        PostgresConnector.execute_query(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.fixture
def seed_raw_events():
    """Return a helper that bulk-loads raw_events rows into the given schema."""
    def _seed(schema: str, rows: list[dict]) -> None:
        df = pd.DataFrame(rows)
        PostgresConnector.upsert_query(
            schema=schema, table="raw_events", df=df, unique_columns=["event_id"]
        )
    return _seed
