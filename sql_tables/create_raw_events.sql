CREATE TABLE IF NOT EXISTS augury_exam.raw_events (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('machine_signal', 'machine_metadata', 'cmms_work_order')),
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

CREATE INDEX IF NOT EXISTS augury_exam_raw_events_source_idx ON augury_exam.raw_events USING btree (source);
CREATE INDEX IF NOT EXISTS augury_exam_raw_events_machine_id_idx ON augury_exam.raw_events USING btree (machine_id);
CREATE INDEX IF NOT EXISTS augury_exam_raw_events_sensor_id_idx ON augury_exam.raw_events USING btree (sensor_id);
CREATE INDEX IF NOT EXISTS augury_exam_raw_events_event_ts_idx ON augury_exam.raw_events USING btree (event_ts);
CREATE INDEX IF NOT EXISTS augury_exam_raw_events_ingestion_ts_idx ON augury_exam.raw_events USING btree (ingestion_ts);
CREATE INDEX IF NOT EXISTS augury_exam_raw_events_metric_idx ON augury_exam.raw_events USING btree (metric);
CREATE INDEX IF NOT EXISTS augury_exam_raw_events_unit_idx ON augury_exam.raw_events USING btree (unit);
CREATE INDEX IF NOT EXISTS augury_exam_raw_events_factory_id_idx ON augury_exam.raw_events USING btree (factory_id);
