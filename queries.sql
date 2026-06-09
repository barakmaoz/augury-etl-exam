-- queries.sql
-- Analytical queries over the augury_exam schema.
--
-- Source tables (see sql_tables/ and src/augury_exam.py):
--   augury_exam.raw_events     - validated event stream (machine_signal / machine_metadata / cmms_work_order)
--   augury_exam.context_edges  - typed, directed knowledge-graph edges:
--       sensor:<id>    -MONITORS->   machine:<id>
--       machine:<id>   -BELONGS_TO-> line:<id>
--       line:<id>      -BELONGS_TO-> factory:<id>
--       workorder:<id> -AFFECTS->    machine:<id>
--
-- Node ids in context_edges are namespaced as '<type>:<natural_id>' (e.g. 'machine:M-100').


-- =====================================================================
-- 1. Top 5 machines by average vibration in the last 24 hours.
-- =====================================================================
-- Averages raw vibration readings (metric = 'vibration') over a trailing
-- 24h window keyed on the business event time (event_ts).
SELECT
    machine_id,
    AVG(value)  AS avg_vibration,
    COUNT(*)    AS sample_count
FROM augury_exam.raw_events
WHERE source = 'machine_signal'
  AND metric = 'vibration'
  AND event_ts >= NOW() - INTERVAL '24 hours'
GROUP BY machine_id
ORDER BY avg_vibration DESC
LIMIT 5;


-- =====================================================================
-- 2. Machines with more than one open high-priority work order.
-- =====================================================================
-- COUNT(DISTINCT work_order_id) guards against a work order appearing on
-- more than one raw row.
SELECT
    machine_id,
    COUNT(DISTINCT work_order_id) AS open_high_priority_count
FROM augury_exam.raw_events
WHERE source   = 'cmms_work_order'
  AND status   = 'open'
  AND priority = 'high'
GROUP BY machine_id
HAVING COUNT(DISTINCT work_order_id) > 1
ORDER BY open_high_priority_count DESC;


-- =====================================================================
-- 3. Machines that had no signal events in the last hour
--    but had signal events before that.
-- =====================================================================
-- Conditional aggregation over machine_signal rows: keep machines whose
-- most recent reading is older than 1h (none in the last hour) yet that
-- have at least one reading from before the window (i.e. they went quiet).
SELECT
    machine_id,
    MAX(event_ts) AS last_signal_ts
FROM augury_exam.raw_events
WHERE source = 'machine_signal'
GROUP BY machine_id
HAVING COUNT(*) FILTER (WHERE event_ts >= NOW() - INTERVAL '1 hour') = 0
   AND COUNT(*) FILTER (WHERE event_ts <  NOW() - INTERVAL '1 hour') > 0
ORDER BY last_signal_ts DESC;


-- =====================================================================
-- 4. For a given machine, return its factory, line, sensors, and open
--    work orders using context_edges.
-- =====================================================================
-- Traverses the context graph starting from machine:<machine_id>:
--   machine -BELONGS_TO-> line -BELONGS_TO-> factory   (out-edges)
--   sensor  -MONITORS->   machine                      (in-edges)
--   workorder -AFFECTS->  machine                      (in-edges)
-- Work-order status lives on raw_events, so AFFECTS edges are joined back
-- to raw_events to keep only the open ones.
-- Replace 'M-100' with any target id.


WITH params AS (
    SELECT 'machine:' || 'M-100' AS machine_node
)
-- Line the machine belongs to
SELECT 
  'line' AS item_type, 
   e.target_node_id AS node_id
FROM augury_exam.context_edges e, params p
WHERE e.source_node_id = p.machine_node
  AND e.relationship   = 'BELONGS_TO'
  AND e.target_node_type = 'line'

UNION ALL
-- Factory (machine -> line -> factory, two hops)
SELECT 'factory', f.target_node_id
FROM augury_exam.context_edges m
JOIN augury_exam.context_edges f
  ON f.source_node_id = m.target_node_id
 AND f.relationship   = 'BELONGS_TO'
 AND f.target_node_type = 'factory'
, params p
WHERE m.source_node_id = p.machine_node
  AND m.relationship   = 'BELONGS_TO'
  AND m.target_node_type = 'line'

UNION ALL
-- Sensors that monitor the machine (in-edges)
SELECT 'sensor', e.source_node_id
FROM augury_exam.context_edges e, params p
WHERE e.target_node_id = p.machine_node
  AND e.relationship   = 'MONITORS'

UNION ALL
-- Open work orders affecting the machine (in-edges, filtered via raw_events)
SELECT 'open_work_order', e.source_node_id
FROM augury_exam.context_edges e
JOIN augury_exam.raw_events wo
  ON wo.source        = 'cmms_work_order'
 AND 'workorder:' || wo.work_order_id = e.source_node_id
 AND wo.status        = 'open'
, params p
WHERE e.target_node_id = p.machine_node
  AND e.relationship   = 'AFFECTS';
