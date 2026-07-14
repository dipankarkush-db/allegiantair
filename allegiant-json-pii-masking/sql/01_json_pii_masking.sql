-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Configurable JSON PII Masking for Nested JSON — native SQL  (Allegiant Air POC)
-- MAGIC
-- MAGIC SQL-only reference for the schema-preserving in-JSON masking. The runnable, widget-driven version is
-- MAGIC the notebook `pii_masking_json_demo`; this file shows the same objects as plain SQL.
-- MAGIC
-- MAGIC **Problem.** Allegiant ingests Google-Analytics-style events as deeply nested JSON in a single column.
-- MAGIC PII lives *inside* the JSON — a `name`, and first/last name (+ email) embedded in a
-- MAGIC `manage-travel` URL. Goal: mask *only* the PII values while **preserving the JSON structure/schema**.
-- MAGIC
-- MAGIC **Runtime = native SQL (`regexp_replace`)** — stays in Photon.
-- MAGIC The notebook *compiles* a config (`pii_policies`: `tag_value -> {keys, url_params, regex, strategy}`) into
-- MAGIC a `regexp_replace` chain baked into one SQL mask function per tag value. Strategies: `REDACT`, `NULLIFY`,
-- MAGIC `PARTIAL`. (HASH is not expressible in `regexp_replace`; do deterministic hashing in a batch/materialize
-- MAGIC step, not at read time.) The concrete functions below are what the compiler emits for `name` + `email`.

-- COMMAND ----------

-- MAGIC %md ## 0. Parameters

-- COMMAND ----------

CREATE WIDGET TEXT catalog_name DEFAULT "dkushari_uc";
CREATE WIDGET TEXT schema_name  DEFAULT "allegiant_air";
CREATE WIDGET TEXT table_name   DEFAULT "ga_app_hits";

CREATE CATALOG IF NOT EXISTS ${catalog_name};
CREATE SCHEMA  IF NOT EXISTS ${catalog_name}.${schema_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Native mask functions (one per PII type) — compiled from the config
-- MAGIC Each function's body is a `regexp_replace` chain. `keys` masks `"<key>":"..."` at any depth (quote-anchored,
-- MAGIC so `name` never matches `appName`); `url_params` masks `?param=`/`&param=` values inside a string.

-- COMMAND ----------

-- name policy: mask the "name" key + firstName/lastName/email URL params (REDACT)
CREATE OR REPLACE FUNCTION ${catalog_name}.${schema_name}.mask_pii_name(payload STRING)
RETURNS STRING
COMMENT 'Native regexp_replace column mask for pii=name.'
RETURN regexp_replace(
         regexp_replace(payload, '("name" *: *)"[^"]*"', '$1"***MASKED***"'),
         '([?&](firstName|lastName|email)=)[^&#"]*', '$1***MASKED***');

-- email policy: mask the "email" key (REDACT)
CREATE OR REPLACE FUNCTION ${catalog_name}.${schema_name}.mask_pii_email(payload STRING)
RETURNS STRING
COMMENT 'Native regexp_replace column mask for pii=email.'
RETURN regexp_replace(payload, '("email" *: *)"[^"]*"', '$1"***MASKED***"');

-- COMMAND ----------

-- MAGIC %md ## 2. Quick test (native, no table needed)

-- COMMAND ----------

SELECT ${catalog_name}.${schema_name}.mask_pii_name(
  '{"appInfo":{"name":"Richard Jones","appName":"abc2Go"},"documentLocation":"https://x/manage-travel?firstName=Richard&lastName=Jones&email=richard.jones@example.com&pnr=AB12CD"}'
) AS masked;
-- -> name + firstName/lastName/email masked; appName + pnr preserved.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Governed tag + tag the columns
-- MAGIC `pii` is a governed tag (fixed allowed-value list). Ensure it allows every value you use, then tag columns.
-- MAGIC Inspect: `SHOW GOVERNED TAGS;`  Create/alter needs account-level CREATE (account/workspace admin).

-- COMMAND ----------

-- CREATE GOVERNED TAG pii DESCRIPTION 'PII classification for ABAC column masking' VALUES ('name', 'email');
-- ALTER GOVERNED TAG pii SET VALUES ('name', 'email', 'ssn');   -- declarative: replaces the value set

ALTER TABLE ${catalog_name}.${schema_name}.${table_name} ALTER COLUMN hit_payload  SET TAGS ('pii' = 'name');
ALTER TABLE ${catalog_name}.${schema_name}.${table_name} ALTER COLUMN user_payload SET TAGS ('pii' = 'email');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. MODE 1 (recommended) — one schema-level ABAC policy per tag value
-- MAGIC Each policy applies its native mask fn to every column carrying the tag — no per-table setup, and no
-- MAGIC `USING COLUMNS` (config is baked into the function). `TO account users EXCEPT <account_group>` lets a
-- MAGIC privileged group see raw (omit EXCEPT to mask everyone).

-- COMMAND ----------

CREATE OR REPLACE POLICY pii_json_mask_name
ON SCHEMA ${catalog_name}.${schema_name}
COMMENT 'ABAC: auto-mask columns tagged pii=name via native mask_pii_name.'
COLUMN MASK ${catalog_name}.${schema_name}.mask_pii_name
TO `account users`  -- add: EXCEPT `<account_group>`  (omit to mask everyone)
FOR TABLES
MATCH COLUMNS has_tag_value('pii', 'name') AS c
ON COLUMN c;

CREATE OR REPLACE POLICY pii_json_mask_email
ON SCHEMA ${catalog_name}.${schema_name}
COMMENT 'ABAC: auto-mask columns tagged pii=email via native mask_pii_email.'
COLUMN MASK ${catalog_name}.${schema_name}.mask_pii_email
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('pii', 'email') AS c
ON COLUMN c;

-- Inspect / remove:
-- SHOW EFFECTIVE POLICIES ON TABLE ${catalog_name}.${schema_name}.${table_name};
-- DROP POLICY pii_json_mask_name  ON SCHEMA ${catalog_name}.${schema_name};
-- DROP POLICY pii_json_mask_email ON SCHEMA ${catalog_name}.${schema_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4-alt. MODE 1 (legacy) — per-table column mask (no ABAC)
-- MAGIC If ABAC/governed tags are unavailable, apply the same native fn per table (repeat per table/column):

-- COMMAND ----------

-- ALTER TABLE ${catalog_name}.${schema_name}.${table_name} ALTER COLUMN hit_payload  SET MASK ${catalog_name}.${schema_name}.mask_pii_name;
-- ALTER TABLE ${catalog_name}.${schema_name}.${table_name} ALTER COLUMN user_payload SET MASK ${catalog_name}.${schema_name}.mask_pii_email;
-- ALTER TABLE ${catalog_name}.${schema_name}.${table_name} ALTER COLUMN hit_payload  DROP MASK;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. MODE 2 (fastest for "everyone masked") — materialize once
-- MAGIC Nobody sees raw, so skip per-query masking: rewrite once with the native fns and read plain columns.
-- MAGIC (Also the CCPA "permanent removal" path — the raw values are gone from the stored copy.)

-- COMMAND ----------

-- CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.${table_name}_masked AS
-- SELECT hit_id,
--        ${catalog_name}.${schema_name}.mask_pii_name(hit_payload)   AS hit_payload,
--        ${catalog_name}.${schema_name}.mask_pii_email(user_payload) AS user_payload
-- FROM   ${catalog_name}.${schema_name}.${table_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Notes
-- MAGIC - **HASH** isn't supported in native `regexp_replace` (can't `sha2()` a matched substring). If you need
-- MAGIC   deterministic hashing, do it in a batch/materialize step (`from_json`→struct→`sha2`→`to_json`) for a
-- MAGIC   known schema — not at read time.
-- MAGIC - **VARIANT columns:** make the mask fn `... RETURNS VARIANT RETURN parse_json(regexp_replace(to_json(payload), ...))`.
-- MAGIC - **Target keys at any depth** with `keys` (quote-anchored, so `name` never matches `appName`) rather
-- MAGIC   than exact dotted paths — a key matches wherever it appears in the JSON.
