"""DB-backed tests for the SQL pipeline (run against an isolated schema in the .env Postgres).

These use the `test_schema` fixture, which creates/drops a throwaway schema and skips entirely
when the database is unreachable.
"""
import pandas as pd

from augury_exam import build_machine_signal_hourly, build_context_edges
from pg_connector import PostgresConnector


def _signal(event_id, machine_id, sensor_id, event_ts, ingestion_ts, metric, value):
    return {
        "event_id": event_id, "source": "machine_signal", "machine_id": machine_id,
        "sensor_id": sensor_id, "event_ts": event_ts, "ingestion_ts": ingestion_ts,
        "metric": metric, "value": value,
    }


def test_hourly_aggregation(test_schema, seed_raw_events):
    seed_raw_events(test_schema, [
        _signal("e1", "M-100", "S-1", "2026-04-30 08:05:00", "2026-04-30 08:06:00", "vibration", 80.0),
        _signal("e2", "M-100", "S-1", "2026-04-30 08:25:00", "2026-04-30 08:26:00", "vibration", 82.0),
    ])

    build_machine_signal_hourly(schema=test_schema)

    rows = PostgresConnector.execute_query(
        f"SELECT * FROM {test_schema}.machine_signal_hourly", return_df=True
    )

    assert len(rows) == 1
    row = rows.iloc[0]
    assert row["machine_id"] == "M-100"
    assert row["metric"] == "vibration"
    assert row["hour_start"] == pd.Timestamp("2026-04-30 08:00:00")
    assert row["sample_count"] == 2
    assert row["avg_value"] == 81.0
    assert row["min_value"] == 80.0
    assert row["max_value"] == 82.0
    assert row["last_event_ts"] == pd.Timestamp("2026-04-30 08:25:00")


def test_late_event_updates_bucket(test_schema, seed_raw_events):
    # initial on-time events in the 08:00 bucket
    seed_raw_events(test_schema, [
        _signal("e1", "M-100", "S-1", "2026-04-30 08:05:00", "2026-04-30 08:06:00", "vibration", 80.0),
        _signal("e2", "M-100", "S-1", "2026-04-30 08:25:00", "2026-04-30 08:26:00", "vibration", 82.0),
    ])
    build_machine_signal_hourly(schema=test_schema)

    before = PostgresConnector.execute_query(
        f"SELECT sample_count FROM {test_schema}.machine_signal_hourly", return_df=True
    )
    assert int(before.iloc[0]["sample_count"]) == 2

    # a late event: event_ts in the 08:00 hour, but ingested ~2h later (new watermark)
    seed_raw_events(test_schema, [
        _signal("e3", "M-100", "S-1", "2026-04-30 08:50:00", "2026-04-30 10:45:00", "vibration", 100.0),
    ])
    build_machine_signal_hourly(schema=test_schema)

    rows = PostgresConnector.execute_query(
        f"SELECT * FROM {test_schema}.machine_signal_hourly", return_df=True
    )
    assert len(rows) == 1
    row = rows.iloc[0]
    # late event folded in AND earlier on-time samples retained (3, not 1)
    assert row["sample_count"] == 3
    assert row["max_value"] == 100.0
    assert row["last_event_ts"] == pd.Timestamp("2026-04-30 08:50:00")


def test_context_edges_no_duplicates(test_schema, seed_raw_events):
    seed_raw_events(test_schema, [
        # two signals from the same sensor -> the S-1->M-100 edge must appear once
        _signal("e1", "M-100", "S-1", "2026-04-30 08:05:00", "2026-04-30 08:06:00", "vibration", 80.0),
        _signal("e2", "M-100", "S-1", "2026-04-30 08:25:00", "2026-04-30 08:26:00", "vibration", 82.0),
        _signal("e3", "M-100", "S-2", "2026-04-30 09:05:00", "2026-04-30 09:06:00", "temperature", 60.0),
        # metadata -> machine->line and line->factory edges
        {"event_id": "m1", "source": "machine_metadata", "machine_id": "M-100",
         "event_ts": "2026-04-30 07:00:00", "ingestion_ts": "2026-04-30 07:01:00",
         "line_id": "L-2", "factory_id": "F-1", "machine_type": "pump"},
        # work order -> workorder->machine edge
        {"event_id": "w1", "source": "cmms_work_order", "machine_id": "M-100",
         "event_ts": "2026-04-30 10:00:00", "ingestion_ts": "2026-04-30 10:02:00",
         "work_order_id": "WO-9001", "status": "open", "priority": "high"},
    ])

    build_context_edges(schema=test_schema)

    edges = PostgresConnector.execute_query(
        f"SELECT * FROM {test_schema}.context_edges", return_df=True
    )

    # the duplicated S-1->M-100 signal collapses to a single edge, valid_from = earliest event_ts
    s1 = edges[edges["source_node_id"] == "sensor:S-1"]
    assert len(s1) == 1
    assert s1.iloc[0]["valid_from"] == pd.Timestamp("2026-04-30 08:05:00")

    # expected edge counts per relationship
    counts = edges["relationship"].value_counts().to_dict()
    assert counts == {"MONITORS": 2, "BELONGS_TO": 2, "AFFECTS": 1}

    # no duplicate edges at all
    assert edges["edge_id"].is_unique
    total = len(edges)

    # idempotent: a second build produces no new/duplicate rows
    build_context_edges(schema=test_schema)
    edges2 = PostgresConnector.execute_query(
        f"SELECT * FROM {test_schema}.context_edges", return_df=True
    )
    assert len(edges2) == total
