CREATE TABLE IF NOT EXISTS augury_exam.context_edges (
    edge_id          TEXT PRIMARY KEY,
    source_node_id   TEXT NOT NULL,
    source_node_type TEXT NOT NULL,
    relationship     TEXT NOT NULL,
    target_node_id   TEXT NOT NULL,
    target_node_type TEXT NOT NULL,
    valid_from       TIMESTAMP NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- traversal indexes (out-edges, in-edges, by relationship type)
CREATE INDEX IF NOT EXISTS augury_exam_context_edges_source_idx ON augury_exam.context_edges USING btree (source_node_id);
CREATE INDEX IF NOT EXISTS augury_exam_context_edges_target_idx ON augury_exam.context_edges USING btree (target_node_id);
CREATE INDEX IF NOT EXISTS augury_exam_context_edges_rel_idx    ON augury_exam.context_edges USING btree (relationship);
