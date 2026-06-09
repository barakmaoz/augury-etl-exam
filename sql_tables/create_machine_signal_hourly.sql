CREATE TABLE IF NOT EXISTS augury_exam.machine_signal_hourly (
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


CREATE INDEX IF NOT EXISTS augury_exam_machine_signal_hourly_machine_id_idx ON augury_exam.machine_signal_hourly USING btree (machine_id);
CREATE INDEX IF NOT EXISTS augury_exam_machine_signal_hourly_metric_idx ON augury_exam.machine_signal_hourly USING btree (metric);
CREATE INDEX IF NOT EXISTS augury_exam_machine_signal_hourly_hour_start_idx ON augury_exam.machine_signal_hourly USING btree (hour_start);