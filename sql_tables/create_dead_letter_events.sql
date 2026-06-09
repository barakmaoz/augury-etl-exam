CREATE TABLE IF NOT EXISTS augury_exam.dead_letter_events (
    event_id TEXT,
    machine_id TEXT,
    reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);



