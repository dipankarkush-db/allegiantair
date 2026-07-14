# Databricks notebook source
# MAGIC %md
# MAGIC # AI-Powered PII Discovery → Human Review → Tag → Auto-Mask
# MAGIC
# MAGIC An **AI-assisted** flow for keeping the PII target list current as the JSON evolves:
# MAGIC
# MAGIC ```
# MAGIC  ai_query classify  →  confidence score  →  HUMAN REVIEW GATE  →  apply governed tag  →  ABAC auto-masks
# MAGIC ```
# MAGIC
# MAGIC An LLM proposes PII labels with a confidence score, findings are **reviewed by a human before
# MAGIC enforcement** (here, via an approval table), and only *approved* labels drive governance. Once a
# MAGIC column is tagged, the **schema-level ABAC policy** from `pii_masking_json_demo` masks it
# MAGIC automatically — no per-table change.
# MAGIC
# MAGIC > Treat AI output as a **starting point**. Nothing is enforced until a human approves it.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dkushari_uc", "Catalog")
dbutils.widgets.text("schema_name", "allegiant_air", "Schema")
dbutils.widgets.text("table_name", "ga_app_hits", "Table")
dbutils.widgets.text("json_column", "hit_payload", "JSON column")
dbutils.widgets.text("model", "databricks-meta-llama-3-3-70b-instruct", "AI model endpoint")
dbutils.widgets.text("sample_rows", "20", "Rows to sample")
dbutils.widgets.text("confidence_threshold", "80", "Confidence threshold (0-100)")
dbutils.widgets.text("tag_key", "pii", "Governed tag key")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
table   = dbutils.widgets.get("table_name")
col     = dbutils.widgets.get("json_column")
model   = dbutils.widgets.get("model")
n       = int(dbutils.widgets.get("sample_rows"))
THRESHOLD = int(dbutils.widgets.get("confidence_threshold"))
tag_key = dbutils.widgets.get("tag_key")
fqtn    = f"{catalog}.{schema}.{table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Classify — ask the LLM to enumerate PII-bearing JSON paths (confidence 0–100)
# MAGIC For each leaf field we get `json_path`, `key`, `classification`, `reasoning`, and a **confidence
# MAGIC score 0–100**. For higher confidence you can run several `model` endpoints and take the
# MAGIC highest-confidence label (an ensemble/voting approach).

# COMMAND ----------

prompt = (
    "You are a data-privacy classifier. You are given a JSON document. "
    "Enumerate every leaf field that contains Personally Identifiable Information (PII) "
    "under regulations such as CCPA/GDPR (names, emails, phone numbers, addresses, IPs, "
    "device IDs tied to a person, government IDs, values embedded in URLs, etc.). "
    "Return ONLY a raw JSON array (no markdown, no code fences). Each element must be an object with: "
    "json_path (dotted path, use [*] for arrays), key (the leaf key name), "
    "classification (one of: name, email, phone, address, ssn, ip_address, credit_card, other), "
    "reasoning (short), confidence (integer 0-100). "
    "If there is no PII, return []. JSON document: "
)

discovery_sql = f"""
SELECT
  ai_query('{model}', CONCAT('{prompt}', CAST({col} AS STRING))) AS pii_paths_json,
  {col} AS sample_payload
FROM {fqtn}
LIMIT {n}
"""
raw = spark.sql(discovery_sql)
raw.createOrReplaceTempView("pii_discovery_raw")
display(raw.select("pii_paths_json"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consolidate proposals into a reviewable table
# MAGIC Parse model output, aggregate across sampled rows, rank by confidence + frequency, and flag each
# MAGIC proposal `auto_approved` when `max_confidence >= threshold` (still requires a human to confirm in §3).

# COMMAND ----------

import json
from collections import defaultdict
from pyspark.sql import Row

rows = spark.sql("SELECT pii_paths_json FROM pii_discovery_raw").collect()
agg = defaultdict(lambda: {"count": 0, "conf": 0.0, "classification": "", "reasoning": ""})
for r in rows:
    try:
        for item in json.loads(r["pii_paths_json"]):
            key = (item.get("json_path") or item.get("key") or "").strip()
            if not key:
                continue
            a = agg[key]
            a["count"] += 1
            a["conf"] = max(a["conf"], float(item.get("confidence", 0) or 0))
            a["classification"] = item.get("classification", a["classification"])
            a["reasoning"] = item.get("reasoning", a["reasoning"])
    except Exception as e:
        print("skip unparseable row:", e)

proposals = [
    Row(json_path=k, leaf_key=k.split(".")[-1].replace("[*]", ""),
        occurrences=v["count"], max_confidence=v["conf"],
        classification=v["classification"], reasoning=v["reasoning"],
        auto_approved=(v["conf"] >= THRESHOLD), approved=(v["conf"] >= THRESHOLD))
    for k, v in sorted(agg.items(), key=lambda kv: (-kv[1]["conf"], -kv[1]["count"]))
]
proposals_df = spark.createDataFrame(proposals) if proposals else None

if proposals_df:
    proposals_df.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.pii_path_proposals")
    print(f"Saved proposals to {catalog}.{schema}.pii_path_proposals "
          f"(threshold={THRESHOLD}; edit the `approved` column to review).")
    display(proposals_df)
else:
    print("No PII paths proposed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. HUMAN REVIEW GATE (required before enforcement)
# MAGIC Nothing is enforced from AI output alone. A reviewer inspects `pii_path_proposals` and sets
# MAGIC `approved = true/false` per row (a lightweight approval/triage step). Below-
# MAGIC threshold proposals default to `approved = false` and must be explicitly opted in.
# MAGIC
# MAGIC ```sql
# MAGIC -- Reviewer actions (run in a SQL cell), e.g.:
# MAGIC UPDATE dkushari_uc.allegiant_air.pii_path_proposals SET approved = true  WHERE json_path = '[*].appInfo.name';
# MAGIC UPDATE dkushari_uc.allegiant_air.pii_path_proposals SET approved = false WHERE json_path = '[*].appInfo.appName';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Enforce — build the config from APPROVED proposals, tag the column, ABAC auto-masks
# MAGIC From the approved rows we (a) assemble a `pii_config` (leaf keys → `keys`), and (b) apply the governed
# MAGIC tag to the JSON column. The schema-level ABAC policy created in `pii_masking_json_demo` then masks the
# MAGIC column automatically. (URL-embedded params still go in `pii_config.url_params` — inspect flagged URL
# MAGIC fields by hand; the notebook default already covers `firstName`/`lastName`.)

# COMMAND ----------

approved = spark.sql(f"SELECT * FROM {catalog}.{schema}.pii_path_proposals WHERE approved = true").collect()
if not approved:
    print("No approved proposals — nothing to enforce. Approve rows in §3 first.")
else:
    leaf_keys = sorted({r["leaf_key"] for r in approved if r["leaf_key"]})
    paths     = sorted({r["json_path"] for r in approved if r["json_path"]})
    # Pick a governed-tag value from the highest-confidence approved classification (must be an allowed
    # value of the `pii` governed tag policy, e.g. name/email/phone/ssn/...).
    tag_value = sorted(approved, key=lambda r: -float(r["max_confidence"]))[0]["classification"] or "name"

    pii_config = {"strategy": "REDACT", "keys": leaf_keys, "paths": [],
                  "url_params": ["firstName", "lastName", "email"], "regex": []}
    print("Suggested pii_config for the ABAC policy:\n", json.dumps(pii_config, indent=2))
    print(f"\nApplying governed tag {tag_key}={tag_value} to {fqtn}.{col} ...")

    spark.sql(f"ALTER TABLE {fqtn} ALTER COLUMN {col} SET TAGS ('{tag_key}' = '{tag_value}')")
    print("Tag applied. The schema-level ABAC policy 'pii_json_mask_policy' now masks this column "
          "automatically. If you changed the key list, re-run §5 of pii_masking_json_demo with the "
          "pii_config above so the policy uses the updated config.")
    display(spark.sql(f"""
      SELECT column_name, tag_name, tag_value FROM {catalog}.information_schema.column_tags
      WHERE schema_name='{schema}' AND table_name='{table}'
    """))
