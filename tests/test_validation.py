"""Pure (no-DB) tests for the event validation / dedup transform."""
import pandas as pd

from augury_exam import validate_and_split_events


def _df(rows: list[dict]) -> pd.DataFrame:
    # Ensure every column the transform reads exists, even when a row omits it.
    columns = ["event_id", "source", "machine_id", "event_ts", "ingestion_ts", "metric", "value"]
    return pd.DataFrame(rows, columns=columns)


def test_dedup_by_event_id():
    rows = [
        # same event_id twice, different ingestion_ts -> keep the latest ingestion_ts
        {"event_id": "evt-1001", "source": "machine_signal", "machine_id": "M-100",
         "event_ts": "2026-04-30 08:05:00", "ingestion_ts": "2026-04-30 08:06:00",
         "metric": "vibration", "value": 80.0},
        {"event_id": "evt-1001", "source": "machine_signal", "machine_id": "M-100",
         "event_ts": "2026-04-30 08:05:00", "ingestion_ts": "2026-04-30 09:00:00",
         "metric": "vibration", "value": 99.0},
        # an unrelated valid row that must survive
        {"event_id": "evt-2001", "source": "machine_signal", "machine_id": "M-101",
         "event_ts": "2026-04-30 09:20:00", "ingestion_ts": "2026-04-30 09:21:00",
         "metric": "vibration", "value": 70.0},
    ]

    valid, _dead = validate_and_split_events(_df(rows))

    # exactly one row per event_id
    assert valid["event_id"].is_unique
    assert set(valid["event_id"]) == {"evt-1001", "evt-2001"}

    # the kept evt-1001 is the one with the latest ingestion_ts
    kept = valid.loc[valid["event_id"] == "evt-1001"].iloc[0]
    assert kept["ingestion_ts"] == pd.Timestamp("2026-04-30 09:00:00")
    assert kept["value"] == 99.0


def test_invalid_event_goes_to_dead_letter():
    rows = [
        # valid row -> should pass through
        {"event_id": "good-1", "source": "machine_signal", "machine_id": "M-100",
         "event_ts": "2026-04-30 08:05:00", "ingestion_ts": "2026-04-30 08:06:00",
         "metric": "vibration", "value": 80.0},
        # machine_signal missing value
        {"event_id": "bad-1", "source": "machine_signal", "machine_id": "M-100",
         "event_ts": "2026-04-30 11:00:00", "ingestion_ts": "2026-04-30 11:01:00",
         "metric": "vibration", "value": None},
        # invalid event_ts
        {"event_id": "bad-2", "source": "machine_metadata", "machine_id": "M-100",
         "event_ts": "not-a-timestamp", "ingestion_ts": "2026-04-30 11:01:00",
         "metric": None, "value": None},
        # unsupported source
        {"event_id": "bad-3", "source": "unknown_source", "machine_id": "M-100",
         "event_ts": "2026-04-30 11:00:00", "ingestion_ts": "2026-04-30 11:01:00",
         "metric": None, "value": None},
    ]

    valid, dead = validate_and_split_events(_df(rows))

    # all three bad rows are quarantined, each with a non-empty reason
    assert set(dead["event_id"]) == {"bad-1", "bad-2", "bad-3"}
    assert (dead["reason"].str.len() > 0).all()

    # only the clean row survives; no bad row leaks into valid
    assert set(valid["event_id"]) == {"good-1"}
