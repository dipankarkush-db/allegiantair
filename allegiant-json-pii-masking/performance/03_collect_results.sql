-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Performance framework — 03 · Collect results
-- MAGIC Creates `perf_results` and pulls the tagged queries' server-side timings from
-- MAGIC `system.query.history` (there is a short ingestion delay — if rows are missing, wait ~1–2
-- MAGIC min and re-run this cell). De-dupes on `statement_id`, so it is safe to run repeatedly.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dkushari_uc.allegiant_air_perf.perf_results (
  collected_at TIMESTAMP, scale STRING, variant STRING, pattern STRING, iter INT,
  execution_ms BIGINT, total_ms BIGINT, compilation_ms BIGINT,
  read_rows BIGINT, read_bytes BIGINT, io_cache_pct DOUBLE,
  statement_id STRING, started_at TIMESTAMP
) USING DELTA;

-- COMMAND ----------

INSERT INTO dkushari_uc.allegiant_air_perf.perf_results
SELECT current_timestamp() AS collected_at,
  regexp_extract(statement_text, 'scale=([0-9a-z]+)', 1)   AS scale,
  regexp_extract(statement_text, 'variant=([a-z]+)', 1)    AS variant,
  regexp_extract(statement_text, 'pattern=([a-z_]+)', 1)   AS pattern,
  cast(regexp_extract(statement_text, 'iter=([0-9]+)', 1) AS INT) AS iter,
  execution_duration_ms, total_duration_ms, compilation_duration_ms,
  read_rows, read_bytes, read_io_cache_percent,
  statement_id, start_time
FROM system.query.history
WHERE statement_text LIKE '/* PERFTEST%'
  AND statement_type = 'SELECT'
  AND start_time >= current_timestamp() - INTERVAL 6 HOURS
  AND statement_id NOT IN (SELECT statement_id FROM dkushari_uc.allegiant_air_perf.perf_results);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quick summary — median execution time & masking overhead vs baseline

-- COMMAND ----------

WITH med AS (
  SELECT scale, variant, pattern,
         percentile(execution_ms, 0.5) AS p50_ms
  FROM dkushari_uc.allegiant_air_perf.perf_results GROUP BY scale, variant, pattern
)
SELECT m.scale, m.pattern, m.variant, round(m.p50_ms,0) AS p50_ms,
       round(m.p50_ms - b.p50_ms, 0) AS overhead_ms,
       round(100.0 * (m.p50_ms - b.p50_ms) / nullif(b.p50_ms,0), 1) AS overhead_pct
FROM med m JOIN med b
  ON m.scale = b.scale AND m.pattern = b.pattern AND b.variant = 'baseline'
ORDER BY CASE m.scale WHEN '1m' THEN 1 WHEN '10m' THEN 2 ELSE 3 END, m.pattern,
         CASE m.variant WHEN 'baseline' THEN 1 WHEN 'dynamic' THEN 2 ELSE 3 END;
