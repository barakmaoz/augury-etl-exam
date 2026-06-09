import pandas as pd
import os
import pendulum
from pg_connector import PostgresConnector
pd.options.mode.copy_on_write = True

def validate_and_split_events(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Coerce timestamps, apply validation rules, split, and dedup the raw events.

    Returns (valid_events, dead_letter_events). Pure transform (no I/O), so it can be unit-tested.

    Expected validation:
    - event_id must be unique.
    - event_id must exist.
    - source must be one of the supported values.
    - machine_id must exist.
    - event_ts and ingestion_ts must be valid timestamps.
    - For machine_signal, metric and value must exist.
    """

    df = df.copy()

    df['event_ts'] = pd.to_datetime(df['event_ts'], errors='coerce')
    df['ingestion_ts'] = pd.to_datetime(df['ingestion_ts'], errors='coerce')

    SUPPORTED_SOURCES = ['machine_signal', 'machine_metadata', 'cmms_work_order']

    df['reason'] = ""

    # 3. Apply Validation Rules
    # Rule: event_id must exist
    df.loc[df['event_id'].isna(), 'reason'] += "Missing event_id; "

    # Rule: source must be one of the supported values
    df.loc[~df['source'].isin(SUPPORTED_SOURCES), 'reason'] += "Invalid or missing source; "

    # Rule: machine_id must exist
    df.loc[df['machine_id'].isna(), 'reason'] += "Missing machine_id; "

    # Rule: event_ts and ingestion_ts must be valid timestamps
    df.loc[df['event_ts'].isna(), 'reason'] += "Invalid or missing event_ts; "
    df.loc[df['ingestion_ts'].isna(), 'reason'] += "Invalid or missing ingestion_ts; "

    # Rule: For machine_signal, metric and value must exist
    # Create a mask for rows that are 'machine_signal' BUT are missing 'metric' OR 'value'
    invalid_signal_mask = (df['source'] == 'machine_signal') & (df['metric'].isna() | df['value'].isna())
    df.loc[invalid_signal_mask, 'reason'] += "Missing metric or value for machine_signal; "

    # 4. Split the data into Valid and Dead Letter tables
    dead_letter_events = df[df['reason'] != ""].copy()[['event_id','machine_id','reason']]
    valid_events = df[df['reason'] == ""].copy()

    # Clean up: remove the reason column from the valid events table
    valid_events = valid_events.drop(columns=['reason'])

    # STEP 5: clean duplicate event_id
    valid_events.sort_values(by=['event_id','ingestion_ts'],ascending=[True,False],inplace=True)
    valid_events = valid_events.drop_duplicates(subset=['event_id'],keep='first',inplace=False,ignore_index=True) # the same as DISTINCT ON

    return valid_events, dead_letter_events


def ingest_raw_events_into_db(path: str = "augury_data/events.jsonl", schema: str = "augury_exam") -> None:
    """Read events.jsonl, validate/split via validate_and_split_events, and load into the DB."""

    df = pd.read_json(path, lines=True)
    valid_events, dead_letter_events = validate_and_split_events(df)

    # Upsert the valid_events into <schema>.raw_events
    PostgresConnector.upsert_query(schema=schema,table='raw_events',df=valid_events,unique_columns=['event_id'])
    print(f"Insert into {schema}.raw_events Successfully completed!") # log

    # Upsert the dead_letter_events into <schema>.dead_letter_events
    PostgresConnector.insert_query(schema=schema,table='dead_letter_events',df=dead_letter_events)
    print(f"Insert into {schema}.dead_letter_events Successfully completed!") # log


    print(f"Total valid events: {len(valid_events)}")
    print(f"Total dead letter events: {len(dead_letter_events)}")
    print("\n--- Sample of Dead Letter Events ---")
    print(dead_letter_events.head())


def build_machine_signal_hourly(schema: str = "augury_exam") -> None:
    """Build the hourly machine_signal aggregate (incremental, idempotent, late-event safe).

    Watermarking / reprocessing strategy: trailing window keyed on ingestion_ts, with a
    full recompute of every affected bucket (no separate watermark/state table).
      1. wm = MAX(ingestion_ts) over machine_signal rows in raw_events (data's high-water mark).
      2. reprocess_from = wm - 2 hours (the allowed lateness).
      3. Affected buckets = DISTINCT (machine_id, metric, hour(event_ts)) for machine_signal rows
         with ingestion_ts >= reprocess_from -> captures new AND late arrivals.
      4. Fully recompute each affected bucket from ALL machine_signal rows in raw_events so the
         aggregates reflect the complete hour, not only the recent events.
      5. Upsert the recomputed buckets keyed on (machine_id, metric, hour_start).

    Incremental  -> only buckets touched within the trailing 2h ingestion window are recomputed.
    Idempotent   -> a re-run with no new data yields the same wm/window and overwrites buckets with
                    identical values.
    Late events  -> any event ingested within 2h of wm forces its event_ts hour bucket to recompute.
    Trade-off    -> relies on ingestion_ts being roughly time-ordered; events arriving > 2h late are
                    out of scope per the requirement.
    """

    query = f"""
        WITH wm AS (
            SELECT MAX(ingestion_ts) AS max_ing
            FROM {schema}.raw_events
            WHERE source = 'machine_signal'
        ),
        affected AS (
            SELECT DISTINCT
                machine_id,
                metric,
                date_trunc('hour', event_ts) AS hour_start
            FROM {schema}.raw_events, wm
            WHERE source = 'machine_signal'
              AND ingestion_ts >= wm.max_ing - INTERVAL '2 hours'
        )
        SELECT
            e.machine_id,
            e.metric,
            date_trunc('hour', e.event_ts) AS hour_start,
            COUNT(*)        AS sample_count,
            AVG(e.value)    AS avg_value,
            MIN(e.value)    AS min_value,
            MAX(e.value)    AS max_value,
            MAX(e.event_ts) AS last_event_ts
        FROM {schema}.raw_events e
        JOIN affected a
          ON e.machine_id = a.machine_id
         AND e.metric     = a.metric
         AND date_trunc('hour', e.event_ts) = a.hour_start
        WHERE e.source = 'machine_signal'
        GROUP BY e.machine_id, e.metric, date_trunc('hour', e.event_ts);
    """

    agg_df = PostgresConnector.execute_query(query, return_df=True)

    # No machine_signal rows in the reprocessing window (or empty table) -> nothing to upsert.
    if agg_df is None or agg_df.empty:
        print("No machine_signal buckets to update in machine_signal_hourly.")
        return

    PostgresConnector.upsert_query(
        schema=schema,
        table='machine_signal_hourly',
        df=agg_df,
        unique_columns=['machine_id', 'metric', 'hour_start'],
    )
    print(f"Upserted {len(agg_df)} into {schema}.machine_signal_hourly Successfully completed!")  # log


def build_context_edges(schema: str = "augury_exam") -> None:
    """Build the context graph edges (a small knowledge graph over raw_events).

    Four typed, directed edge types, each derived from one source:
      Sensor    -> MONITORS   -> Machine   (from machine_signal:   sensor_id,     machine_id)
      Machine   -> BELONGS_TO -> Line      (from machine_metadata: machine_id,    line_id)
      Line      -> BELONGS_TO -> Factory   (from machine_metadata: line_id,       factory_id)
      WorkOrder -> AFFECTS    -> Machine   (from cmms_work_order:   work_order_id, machine_id)

    Design (serves an AI agent / knowledge graph):
      - Node ids are namespaced & globally unique ('<type>:<natural_id>', e.g. 'machine:M-100'), so
        the same node resolves identically across edges -> stable join key for multi-hop traversal.
      - edge_id = md5(source_node_id | relationship | target_node_id) is deterministic -> idempotent
        re-ingestion, natural dedup, and a stable handle an agent/cache can reference.
      - Each edge repeats across many raw rows, so we GROUP BY the full edge tuple and take
        valid_from = MIN(event_ts): the earliest business time the relationship was observed.
      - valid_from leaves a clean path to add valid_to later (bitemporal edge invalidation) without
        reshaping the table.
    """

    query = f"""
        WITH edges AS (
            -- Sensor MONITORS Machine
            SELECT 'sensor:'  || sensor_id  AS source_node_id, 'sensor'  AS source_node_type,
                   'MONITORS'               AS relationship,
                   'machine:' || machine_id AS target_node_id, 'machine' AS target_node_type,
                   event_ts
            FROM {schema}.raw_events
            WHERE source = 'machine_signal' AND sensor_id IS NOT NULL

            UNION ALL
            -- Machine BELONGS_TO Line
            SELECT 'machine:' || machine_id, 'machine', 'BELONGS_TO',
                   'line:'    || line_id,    'line',    event_ts
            FROM {schema}.raw_events
            WHERE source = 'machine_metadata' AND line_id IS NOT NULL

            UNION ALL
            -- Line BELONGS_TO Factory
            SELECT 'line:'    || line_id,    'line',    'BELONGS_TO',
                   'factory:' || factory_id, 'factory', event_ts
            FROM {schema}.raw_events
            WHERE source = 'machine_metadata' AND line_id IS NOT NULL AND factory_id IS NOT NULL

            UNION ALL
            -- WorkOrder AFFECTS Machine
            SELECT 'workorder:' || work_order_id, 'workorder', 'AFFECTS',
                   'machine:'   || machine_id,    'machine',   event_ts
            FROM {schema}.raw_events
            WHERE source = 'cmms_work_order' AND work_order_id IS NOT NULL
        )
        SELECT
            md5(source_node_id || '|' || relationship || '|' || target_node_id) AS edge_id,
            source_node_id,
            source_node_type,
            relationship,
            target_node_id,
            target_node_type,
            MIN(event_ts) AS valid_from
        FROM edges
        GROUP BY source_node_id, source_node_type, relationship, target_node_id, target_node_type;
    """

    edges_df = PostgresConnector.execute_query(query, return_df=True)

    # No edges derivable from raw_events (empty table) -> nothing to upsert.
    if edges_df is None or edges_df.empty:
        print("No context edges to update in context_edges.")
        return

    PostgresConnector.upsert_query(
        schema=schema,
        table='context_edges',
        df=edges_df,
        unique_columns=['edge_id'],
    )
    print(f"Upserted {len(edges_df)} into {schema}.context_edges Successfully completed!")  # log




if __name__ == '__main__':
    try:

        # STEP 1: read and clean the jsonl file and INSERT the valid events to raw_events and the invalid events to dead_letter_events
        ingest_raw_events_into_db()


        # STEP 2: Build an hourly machine signal aggregate table
        build_machine_signal_hourly()

        # STEP 3:  Build simple context graph edges
        build_context_edges()

    except Exception as e:
        print(f"Failed to finsih the Jira Upsert ETL because {e}")
