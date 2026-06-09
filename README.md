# augury-etl-exam

A small batch ETL over a unified IoT event stream. It ingests raw events, validates them, builds an
incremental hourly signal aggregate, and derives a typed knowledge graph of the plant. PostgreSQL +
pandas; all pipeline logic lives in [src/augury_exam.py](src/augury_exam.py).

## How to run

1. Create and activate a virtual environment:
   ```bash
   python -m venv env
   source env/Scripts/activate        # Windows (Git Bash);  use: source env/bin/activate on macOS/Linux
   pip install --no-cache-dir -r requirements.txt
   ```
2. Configure the database connection: copy `.env.example` to `.env` and set
   `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`.
3. Create the schema and tables (run the files in [sql_tables/](sql_tables/) against your PGSQL database):
   `create_augury_data.sql` (schema), then `create_raw_events.sql`, `create_dead_letter_events.sql`,
   `create_machine_signal_hourly.sql`, `create_context_edges.sql`.
4. Run the pipeline (reads `augury_data/events.jsonl`):
   ```bash
   python src/augury_exam.py
   ```
   This runs three steps in order: **(1)** ingest + validate raw events, **(2)** build the hourly
   aggregate, **(3)** build the context-graph edges.
5. (Optional) Run the test suite:
   ```bash
   python -m pytest
   ```
   Pure validation tests run anywhere; the SQL tests run against the `.env` Postgres in a throwaway
   schema and **skip** automatically if the DB is unreachable.

Example analytical queries over the resulting tables are in [queries.sql](queries.sql).

## DB schema

All tables live in the `augury_exam` schema (DDL in [sql_tables/](sql_tables/)).

- **`raw_events`** — the validated event stream; one row per `event_id` (PK). A single wide table for
  all three sources, distinguished by `source ∈ {machine_signal, machine_metadata, cmms_work_order}`.
  Key columns: `machine_id`, `sensor_id`, `event_ts` (business time), `ingestion_ts` (pipeline time),
  `metric`, `value`, `unit`, `factory_id`, `line_id`, `work_order_id`, `status`, `machine_type`,
  `priority`.
- **`dead_letter_events`** — rows rejected during validation: `event_id`, `machine_id`, `reason`
  (a human-readable concatenation of every rule that failed).
- **`machine_signal_hourly`** — the incremental aggregate. PK `(machine_id, metric, hour_start)`;
  columns `sample_count`, `avg_value`, `min_value`, `max_value`, `last_event_ts`.
- **`context_edges`** — the knowledge graph. PK `edge_id` (a deterministic hash); columns
  `source_node_id`, `source_node_type`, `relationship`, `target_node_id`, `target_node_type`,
  `valid_from`. Node ids are namespaced and globally unique as `'<type>:<natural_id>'`
  (e.g. `machine:M-100`, `sensor:S-1`), so the same node resolves identically across edges.

## Aggregation strategy (`machine_signal_hourly`)

Incremental, idempotent, and late-event safe, using a **trailing reprocessing window keyed on
`ingestion_ts`** with a **full recompute of every affected bucket** (no separate watermark/state
table). Each run:

1. `wm = MAX(ingestion_ts)` over `machine_signal` rows — the data's own high-water mark.
2. `reprocess_from = wm - 2 hours` (the allowed lateness).
3. **Affected buckets** = the distinct `(machine_id, metric, hour(event_ts))` of rows with
   `ingestion_ts >= reprocess_from` — this captures both new and late arrivals.
4. **Fully recompute** each affected bucket from *all* `machine_signal` rows in that hour (the
   `ingestion_ts` filter only selects *which* buckets are dirty; the recompute is filtered by
   `event_ts`, so no on-time samples are dropped).
5. **Upsert** the recomputed buckets, keyed on `(machine_id, metric, hour_start)`.

Re-running with no new data yields the same window and overwrites buckets with identical values, so
the table is reproducible. Trade-off: this assumes `ingestion_ts` is roughly time-ordered; events
arriving more than 2h late are out of scope per the requirement (the window is a single tunable
constant).

## Duplicates, invalid rows, and late events

- **Duplicates** — `raw_events` is deduplicated by `event_id` (sort by `ingestion_ts` desc, keep
  first) before loading, and `event_id` is the primary key. The hourly aggregate dedups implicitly
  via `GROUP BY`. Context edges are deduped with `GROUP BY` + a deterministic `edge_id`
  (`md5(source_node_id | relationship | target_node_id)`), so re-ingestion is idempotent.
- **Invalid rows** — validation (`validate_and_split_events`) routes any row failing a rule to
  `dead_letter_events` with the reason(s), instead of dropping it silently. Rules: `event_id`,
  `machine_id`, and a supported `source` must be present; `event_ts`/`ingestion_ts` must parse as
  timestamps; `machine_signal` rows must have `metric` and `value`.
- **Late events** — handled by the trailing 2h window above: a late event (old `event_ts`, recent
  `ingestion_ts`) lands inside the window, marks its `event_ts` hour as dirty, and that whole hour is
  recomputed — folding the late sample in while keeping the earlier ones. For edges, `valid_from` is
  `MIN(event_ts)` per edge, so a late earlier observation correctly backdates the edge.

## What I would change for production scale

- **Streaming / incremental loads.** Replace the full-file `read_json` + delete-then-COPY upsert with
  a streamed source (Kafka) and load only new partitions rather than rescanning.
- **Partitioning & retention.** Partition `raw_events` and `machine_signal_hourly` by time
  (e.g. daily) for cheap pruning and rollover; add retention/rollup policies.
- **Orchestration & observability.** Run under an orchestrator (Airflow) with retries,
  alerting, data-quality checks, and dead-letter monitoring; emit row-count/lag metrics.
- **Config & secrets.** Move DB credentials to a secrets manager; make schema/paths/lateness-window
  configurable per environment. Connection pooling already lazy-initializes in
  [src/pg_connector.py](src/pg_connector.py).
- **Testing/CI.** Run the SQL tests against an ephemeral Postgres in CI (e.g. a container) so they
  execute rather than skip.
