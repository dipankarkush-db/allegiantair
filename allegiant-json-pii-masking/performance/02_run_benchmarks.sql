-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Performance framework — 02 · Run benchmarks
-- MAGIC Runs realistic query patterns against every scale × variant. Each query carries a
-- MAGIC `/* PERFTEST ... */` marker so `03_collect_results` can pull its server-side timing
-- MAGIC from `system.query.history`.
-- MAGIC
-- MAGIC **Run this notebook 4–5 times** to get stable medians — each run is one sample.
-- MAGIC `use_cached_result = false` ensures every run really executes (no result-cache short-circuit).

-- COMMAND ----------

SET use_cached_result = false;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 0. Warm up the warehouse (run first)
-- MAGIC Serverless warehouses scale up on demand — the **first** queries after an idle period run on a cold, not-yet-
-- MAGIC scaled cluster and report inflated times. Run this once so the timed queries below measure steady state.

-- COMMAND ----------

SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2
FROM dkushari_uc.allegiant_air_perf.events_100m_baseline
UNION ALL
SELECT count(*), sum(length(hit_payload)), sum(length(user_payload)) FROM dkushari_uc.allegiant_air_perf.events_100m_dynamic;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern: `full_scan_mask`

-- COMMAND ----------

/* PERFTEST scale=1m variant=baseline pattern=full_scan_mask */ SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2 FROM dkushari_uc.allegiant_air_perf.events_1m_baseline;

-- COMMAND ----------

/* PERFTEST scale=1m variant=dynamic pattern=full_scan_mask */ SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2 FROM dkushari_uc.allegiant_air_perf.events_1m_dynamic;

-- COMMAND ----------

/* PERFTEST scale=1m variant=materialized pattern=full_scan_mask */ SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2 FROM dkushari_uc.allegiant_air_perf.events_1m_materialized;

-- COMMAND ----------

/* PERFTEST scale=10m variant=baseline pattern=full_scan_mask */ SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2 FROM dkushari_uc.allegiant_air_perf.events_10m_baseline;

-- COMMAND ----------

/* PERFTEST scale=10m variant=dynamic pattern=full_scan_mask */ SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2 FROM dkushari_uc.allegiant_air_perf.events_10m_dynamic;

-- COMMAND ----------

/* PERFTEST scale=10m variant=materialized pattern=full_scan_mask */ SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2 FROM dkushari_uc.allegiant_air_perf.events_10m_materialized;

-- COMMAND ----------

/* PERFTEST scale=100m variant=baseline pattern=full_scan_mask */ SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2 FROM dkushari_uc.allegiant_air_perf.events_100m_baseline;

-- COMMAND ----------

/* PERFTEST scale=100m variant=dynamic pattern=full_scan_mask */ SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2 FROM dkushari_uc.allegiant_air_perf.events_100m_dynamic;

-- COMMAND ----------

/* PERFTEST scale=100m variant=materialized pattern=full_scan_mask */ SELECT count(*) AS c, sum(length(hit_payload)) AS s1, sum(length(user_payload)) AS s2 FROM dkushari_uc.allegiant_air_perf.events_100m_materialized;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern: `point_lookup`

-- COMMAND ----------

/* PERFTEST scale=1m variant=baseline pattern=point_lookup */ SELECT hit_payload, user_payload FROM dkushari_uc.allegiant_air_perf.events_1m_baseline WHERE hit_id = 424242;

-- COMMAND ----------

/* PERFTEST scale=1m variant=dynamic pattern=point_lookup */ SELECT hit_payload, user_payload FROM dkushari_uc.allegiant_air_perf.events_1m_dynamic WHERE hit_id = 424242;

-- COMMAND ----------

/* PERFTEST scale=1m variant=materialized pattern=point_lookup */ SELECT hit_payload, user_payload FROM dkushari_uc.allegiant_air_perf.events_1m_materialized WHERE hit_id = 424242;

-- COMMAND ----------

/* PERFTEST scale=10m variant=baseline pattern=point_lookup */ SELECT hit_payload, user_payload FROM dkushari_uc.allegiant_air_perf.events_10m_baseline WHERE hit_id = 424242;

-- COMMAND ----------

/* PERFTEST scale=10m variant=dynamic pattern=point_lookup */ SELECT hit_payload, user_payload FROM dkushari_uc.allegiant_air_perf.events_10m_dynamic WHERE hit_id = 424242;

-- COMMAND ----------

/* PERFTEST scale=10m variant=materialized pattern=point_lookup */ SELECT hit_payload, user_payload FROM dkushari_uc.allegiant_air_perf.events_10m_materialized WHERE hit_id = 424242;

-- COMMAND ----------

/* PERFTEST scale=100m variant=baseline pattern=point_lookup */ SELECT hit_payload, user_payload FROM dkushari_uc.allegiant_air_perf.events_100m_baseline WHERE hit_id = 424242;

-- COMMAND ----------

/* PERFTEST scale=100m variant=dynamic pattern=point_lookup */ SELECT hit_payload, user_payload FROM dkushari_uc.allegiant_air_perf.events_100m_dynamic WHERE hit_id = 424242;

-- COMMAND ----------

/* PERFTEST scale=100m variant=materialized pattern=point_lookup */ SELECT hit_payload, user_payload FROM dkushari_uc.allegiant_air_perf.events_100m_materialized WHERE hit_id = 424242;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern: `filter_nonpii`

-- COMMAND ----------

/* PERFTEST scale=1m variant=baseline pattern=filter_nonpii */ SELECT count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_1m_baseline WHERE get_json_object(hit_payload,'$.appInfo.appName') = 'abc2Go';

-- COMMAND ----------

/* PERFTEST scale=1m variant=dynamic pattern=filter_nonpii */ SELECT count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_1m_dynamic WHERE get_json_object(hit_payload,'$.appInfo.appName') = 'abc2Go';

-- COMMAND ----------

/* PERFTEST scale=1m variant=materialized pattern=filter_nonpii */ SELECT count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_1m_materialized WHERE get_json_object(hit_payload,'$.appInfo.appName') = 'abc2Go';

-- COMMAND ----------

/* PERFTEST scale=10m variant=baseline pattern=filter_nonpii */ SELECT count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_10m_baseline WHERE get_json_object(hit_payload,'$.appInfo.appName') = 'abc2Go';

-- COMMAND ----------

/* PERFTEST scale=10m variant=dynamic pattern=filter_nonpii */ SELECT count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_10m_dynamic WHERE get_json_object(hit_payload,'$.appInfo.appName') = 'abc2Go';

-- COMMAND ----------

/* PERFTEST scale=10m variant=materialized pattern=filter_nonpii */ SELECT count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_10m_materialized WHERE get_json_object(hit_payload,'$.appInfo.appName') = 'abc2Go';

-- COMMAND ----------

/* PERFTEST scale=100m variant=baseline pattern=filter_nonpii */ SELECT count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_100m_baseline WHERE get_json_object(hit_payload,'$.appInfo.appName') = 'abc2Go';

-- COMMAND ----------

/* PERFTEST scale=100m variant=dynamic pattern=filter_nonpii */ SELECT count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_100m_dynamic WHERE get_json_object(hit_payload,'$.appInfo.appName') = 'abc2Go';

-- COMMAND ----------

/* PERFTEST scale=100m variant=materialized pattern=filter_nonpii */ SELECT count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_100m_materialized WHERE get_json_object(hit_payload,'$.appInfo.appName') = 'abc2Go';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern: `groupby_nonpii`

-- COMMAND ----------

/* PERFTEST scale=1m variant=baseline pattern=groupby_nonpii */ SELECT get_json_object(user_payload,'$.loyalty.tier') AS tier, count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_1m_baseline GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

/* PERFTEST scale=1m variant=dynamic pattern=groupby_nonpii */ SELECT get_json_object(user_payload,'$.loyalty.tier') AS tier, count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_1m_dynamic GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

/* PERFTEST scale=1m variant=materialized pattern=groupby_nonpii */ SELECT get_json_object(user_payload,'$.loyalty.tier') AS tier, count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_1m_materialized GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

/* PERFTEST scale=10m variant=baseline pattern=groupby_nonpii */ SELECT get_json_object(user_payload,'$.loyalty.tier') AS tier, count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_10m_baseline GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

/* PERFTEST scale=10m variant=dynamic pattern=groupby_nonpii */ SELECT get_json_object(user_payload,'$.loyalty.tier') AS tier, count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_10m_dynamic GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

/* PERFTEST scale=10m variant=materialized pattern=groupby_nonpii */ SELECT get_json_object(user_payload,'$.loyalty.tier') AS tier, count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_10m_materialized GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

/* PERFTEST scale=100m variant=baseline pattern=groupby_nonpii */ SELECT get_json_object(user_payload,'$.loyalty.tier') AS tier, count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_100m_baseline GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

/* PERFTEST scale=100m variant=dynamic pattern=groupby_nonpii */ SELECT get_json_object(user_payload,'$.loyalty.tier') AS tier, count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_100m_dynamic GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

/* PERFTEST scale=100m variant=materialized pattern=groupby_nonpii */ SELECT get_json_object(user_payload,'$.loyalty.tier') AS tier, count(*) AS c FROM dkushari_uc.allegiant_air_perf.events_100m_materialized GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern: `extract_pii`

-- COMMAND ----------

/* PERFTEST scale=1m variant=baseline pattern=extract_pii */ SELECT get_json_object(hit_payload,'$.appInfo.name') AS nm FROM dkushari_uc.allegiant_air_perf.events_1m_baseline LIMIT 1000;

-- COMMAND ----------

/* PERFTEST scale=1m variant=dynamic pattern=extract_pii */ SELECT get_json_object(hit_payload,'$.appInfo.name') AS nm FROM dkushari_uc.allegiant_air_perf.events_1m_dynamic LIMIT 1000;

-- COMMAND ----------

/* PERFTEST scale=1m variant=materialized pattern=extract_pii */ SELECT get_json_object(hit_payload,'$.appInfo.name') AS nm FROM dkushari_uc.allegiant_air_perf.events_1m_materialized LIMIT 1000;

-- COMMAND ----------

/* PERFTEST scale=10m variant=baseline pattern=extract_pii */ SELECT get_json_object(hit_payload,'$.appInfo.name') AS nm FROM dkushari_uc.allegiant_air_perf.events_10m_baseline LIMIT 1000;

-- COMMAND ----------

/* PERFTEST scale=10m variant=dynamic pattern=extract_pii */ SELECT get_json_object(hit_payload,'$.appInfo.name') AS nm FROM dkushari_uc.allegiant_air_perf.events_10m_dynamic LIMIT 1000;

-- COMMAND ----------

/* PERFTEST scale=10m variant=materialized pattern=extract_pii */ SELECT get_json_object(hit_payload,'$.appInfo.name') AS nm FROM dkushari_uc.allegiant_air_perf.events_10m_materialized LIMIT 1000;

-- COMMAND ----------

/* PERFTEST scale=100m variant=baseline pattern=extract_pii */ SELECT get_json_object(hit_payload,'$.appInfo.name') AS nm FROM dkushari_uc.allegiant_air_perf.events_100m_baseline LIMIT 1000;

-- COMMAND ----------

/* PERFTEST scale=100m variant=dynamic pattern=extract_pii */ SELECT get_json_object(hit_payload,'$.appInfo.name') AS nm FROM dkushari_uc.allegiant_air_perf.events_100m_dynamic LIMIT 1000;

-- COMMAND ----------

/* PERFTEST scale=100m variant=materialized pattern=extract_pii */ SELECT get_json_object(hit_payload,'$.appInfo.name') AS nm FROM dkushari_uc.allegiant_air_perf.events_100m_materialized LIMIT 1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern: `no_masked_col`

-- COMMAND ----------

/* PERFTEST scale=1m variant=baseline pattern=no_masked_col */ SELECT count(hit_id) AS c, min(hit_id) AS mn, max(hit_id) AS mx FROM dkushari_uc.allegiant_air_perf.events_1m_baseline;

-- COMMAND ----------

/* PERFTEST scale=1m variant=dynamic pattern=no_masked_col */ SELECT count(hit_id) AS c, min(hit_id) AS mn, max(hit_id) AS mx FROM dkushari_uc.allegiant_air_perf.events_1m_dynamic;

-- COMMAND ----------

/* PERFTEST scale=1m variant=materialized pattern=no_masked_col */ SELECT count(hit_id) AS c, min(hit_id) AS mn, max(hit_id) AS mx FROM dkushari_uc.allegiant_air_perf.events_1m_materialized;

-- COMMAND ----------

/* PERFTEST scale=10m variant=baseline pattern=no_masked_col */ SELECT count(hit_id) AS c, min(hit_id) AS mn, max(hit_id) AS mx FROM dkushari_uc.allegiant_air_perf.events_10m_baseline;

-- COMMAND ----------

/* PERFTEST scale=10m variant=dynamic pattern=no_masked_col */ SELECT count(hit_id) AS c, min(hit_id) AS mn, max(hit_id) AS mx FROM dkushari_uc.allegiant_air_perf.events_10m_dynamic;

-- COMMAND ----------

/* PERFTEST scale=10m variant=materialized pattern=no_masked_col */ SELECT count(hit_id) AS c, min(hit_id) AS mn, max(hit_id) AS mx FROM dkushari_uc.allegiant_air_perf.events_10m_materialized;

-- COMMAND ----------

/* PERFTEST scale=100m variant=baseline pattern=no_masked_col */ SELECT count(hit_id) AS c, min(hit_id) AS mn, max(hit_id) AS mx FROM dkushari_uc.allegiant_air_perf.events_100m_baseline;

-- COMMAND ----------

/* PERFTEST scale=100m variant=dynamic pattern=no_masked_col */ SELECT count(hit_id) AS c, min(hit_id) AS mn, max(hit_id) AS mx FROM dkushari_uc.allegiant_air_perf.events_100m_dynamic;

-- COMMAND ----------

/* PERFTEST scale=100m variant=materialized pattern=no_masked_col */ SELECT count(hit_id) AS c, min(hit_id) AS mn, max(hit_id) AS mx FROM dkushari_uc.allegiant_air_perf.events_100m_materialized;
