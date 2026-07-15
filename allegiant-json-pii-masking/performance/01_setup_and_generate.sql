-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Performance framework — 01 · Setup & generate data
-- MAGIC Builds an **isolated** perf schema (`dkushari_uc.allegiant_air_perf`) and three tables per scale so masking overhead
-- MAGIC can be measured **side by side**. Existing demo objects are untouched.
-- MAGIC
-- MAGIC For each scale (1M / 10M / 100M rows) it creates:
-- MAGIC - `events_<N>_baseline` — untagged → queries return **raw** data (the no-mask control)
-- MAGIC - `events_<N>_dynamic` — a shallow clone, **tagged** so the schema ABAC policy masks it **at read time**
-- MAGIC - `events_<N>_materialized` — a physically **pre-masked** copy (zero read-time mask cost)
-- MAGIC
-- MAGIC Data shape mirrors the demo's GA events exactly. Reuses the existing governed tag `pii_aa` (values: name, email).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dkushari_uc.allegiant_air_perf;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION dkushari_uc.allegiant_air_perf.mask_pii_name(payload STRING)
RETURNS STRING
COMMENT 'Native regexp_replace column mask for pii_aa=name.'
RETURN regexp_replace(regexp_replace(payload, '("name" *: *)"[^"]*"', '$1"***MASKED***"'), '([?&](firstName|lastName|fname|lname|first_name|last_name|email)=)[^&#"]*', '$1***MASKED***');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION dkushari_uc.allegiant_air_perf.mask_pii_email(payload STRING)
RETURNS STRING
COMMENT 'Native regexp_replace column mask for pii_aa=email.'
RETURN regexp_replace(payload, '("email" *: *)"[^"]*"', '$1"***MASKED***"');

-- COMMAND ----------

CREATE OR REPLACE POLICY pii_json_mask_name
ON SCHEMA dkushari_uc.allegiant_air_perf
COMMENT 'ABAC: auto-mask columns tagged pii_aa=name via native mask_pii_name.'
COLUMN MASK dkushari_uc.allegiant_air_perf.mask_pii_name
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('pii_aa', 'name') AS c
ON COLUMN c;

-- COMMAND ----------

CREATE OR REPLACE POLICY pii_json_mask_email
ON SCHEMA dkushari_uc.allegiant_air_perf
COMMENT 'ABAC: auto-mask columns tagged pii_aa=email via native mask_pii_email.'
COLUMN MASK dkushari_uc.allegiant_air_perf.mask_pii_email
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('pii_aa', 'email') AS c
ON COLUMN c;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scale: 1m (1,000,000 rows)

-- COMMAND ----------

CREATE OR REPLACE TABLE dkushari_uc.allegiant_air_perf.events_1m_baseline AS
SELECT id AS hit_id,
  to_json(named_struct(
    'appInfo', named_struct('appId','com.lixar.abc','appName','abc2Go','name',concat(fn,' ',ln),
                            'version',ver,'screenName',scr),
    'customDimensions', array(named_struct('index',6,'value',element_at(array('GUEST','MEMBER'),cast(rand()*2 AS INT)+1))),
    'documentLocation', concat('https://www.allegiant.com/manage-travel?firstName=',fn,'&lastName=',ln,'&email=',lower(fn),'.',lower(ln),'@example.com&pnr=',pnr),
    'hitNumber', hn, 'type','APPVIEW'
  )) AS hit_payload,
  to_json(named_struct(
    'contact', named_struct('email', concat(lower(fn),'.',lower(ln),'@example.com'),
                            'phone', concat('+1', cast(cast(rand()*9000000000+1000000000 AS BIGINT) AS STRING))),
    'loyalty', named_struct('tier', element_at(array('GOLD','SILVER','BRONZE'),cast(rand()*3 AS INT)+1),
                            'points', cast(rand()*100000 AS INT))
  )) AS user_payload
FROM (
  SELECT id,
    element_at(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth','David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen'), cast(rand()*20 AS INT)+1) AS fn,
    element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin'), cast(rand()*20 AS INT)+1) AS ln,
    element_at(array('5.2.0','5.3.1','6.0.0','5.9.2'), cast(rand()*4 AS INT)+1) AS ver,
    element_at(array('Checkin Flow','Booking Flow','Seat Map','Payment','Confirmation','Home','Manage Travel'), cast(rand()*7 AS INT)+1) AS scr,
    upper(substr(md5(cast(rand() AS STRING)),1,6)) AS pnr, cast(rand()*6+1 AS INT) AS hn
  FROM range(1000000)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE dkushari_uc.allegiant_air_perf.events_1m_dynamic SHALLOW CLONE dkushari_uc.allegiant_air_perf.events_1m_baseline;

-- COMMAND ----------

ALTER TABLE dkushari_uc.allegiant_air_perf.events_1m_dynamic ALTER COLUMN hit_payload  SET TAGS ('pii_aa' = 'name');

-- COMMAND ----------

ALTER TABLE dkushari_uc.allegiant_air_perf.events_1m_dynamic ALTER COLUMN user_payload SET TAGS ('pii_aa' = 'email');

-- COMMAND ----------

CREATE OR REPLACE TABLE dkushari_uc.allegiant_air_perf.events_1m_materialized AS
SELECT hit_id, dkushari_uc.allegiant_air_perf.mask_pii_name(hit_payload) AS hit_payload,
               dkushari_uc.allegiant_air_perf.mask_pii_email(user_payload) AS user_payload
FROM dkushari_uc.allegiant_air_perf.events_1m_baseline;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scale: 10m (10,000,000 rows)

-- COMMAND ----------

CREATE OR REPLACE TABLE dkushari_uc.allegiant_air_perf.events_10m_baseline AS
SELECT id AS hit_id,
  to_json(named_struct(
    'appInfo', named_struct('appId','com.lixar.abc','appName','abc2Go','name',concat(fn,' ',ln),
                            'version',ver,'screenName',scr),
    'customDimensions', array(named_struct('index',6,'value',element_at(array('GUEST','MEMBER'),cast(rand()*2 AS INT)+1))),
    'documentLocation', concat('https://www.allegiant.com/manage-travel?firstName=',fn,'&lastName=',ln,'&email=',lower(fn),'.',lower(ln),'@example.com&pnr=',pnr),
    'hitNumber', hn, 'type','APPVIEW'
  )) AS hit_payload,
  to_json(named_struct(
    'contact', named_struct('email', concat(lower(fn),'.',lower(ln),'@example.com'),
                            'phone', concat('+1', cast(cast(rand()*9000000000+1000000000 AS BIGINT) AS STRING))),
    'loyalty', named_struct('tier', element_at(array('GOLD','SILVER','BRONZE'),cast(rand()*3 AS INT)+1),
                            'points', cast(rand()*100000 AS INT))
  )) AS user_payload
FROM (
  SELECT id,
    element_at(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth','David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen'), cast(rand()*20 AS INT)+1) AS fn,
    element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin'), cast(rand()*20 AS INT)+1) AS ln,
    element_at(array('5.2.0','5.3.1','6.0.0','5.9.2'), cast(rand()*4 AS INT)+1) AS ver,
    element_at(array('Checkin Flow','Booking Flow','Seat Map','Payment','Confirmation','Home','Manage Travel'), cast(rand()*7 AS INT)+1) AS scr,
    upper(substr(md5(cast(rand() AS STRING)),1,6)) AS pnr, cast(rand()*6+1 AS INT) AS hn
  FROM range(10000000)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE dkushari_uc.allegiant_air_perf.events_10m_dynamic SHALLOW CLONE dkushari_uc.allegiant_air_perf.events_10m_baseline;

-- COMMAND ----------

ALTER TABLE dkushari_uc.allegiant_air_perf.events_10m_dynamic ALTER COLUMN hit_payload  SET TAGS ('pii_aa' = 'name');

-- COMMAND ----------

ALTER TABLE dkushari_uc.allegiant_air_perf.events_10m_dynamic ALTER COLUMN user_payload SET TAGS ('pii_aa' = 'email');

-- COMMAND ----------

CREATE OR REPLACE TABLE dkushari_uc.allegiant_air_perf.events_10m_materialized AS
SELECT hit_id, dkushari_uc.allegiant_air_perf.mask_pii_name(hit_payload) AS hit_payload,
               dkushari_uc.allegiant_air_perf.mask_pii_email(user_payload) AS user_payload
FROM dkushari_uc.allegiant_air_perf.events_10m_baseline;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scale: 100m (100,000,000 rows)

-- COMMAND ----------

CREATE OR REPLACE TABLE dkushari_uc.allegiant_air_perf.events_100m_baseline AS
SELECT id AS hit_id,
  to_json(named_struct(
    'appInfo', named_struct('appId','com.lixar.abc','appName','abc2Go','name',concat(fn,' ',ln),
                            'version',ver,'screenName',scr),
    'customDimensions', array(named_struct('index',6,'value',element_at(array('GUEST','MEMBER'),cast(rand()*2 AS INT)+1))),
    'documentLocation', concat('https://www.allegiant.com/manage-travel?firstName=',fn,'&lastName=',ln,'&email=',lower(fn),'.',lower(ln),'@example.com&pnr=',pnr),
    'hitNumber', hn, 'type','APPVIEW'
  )) AS hit_payload,
  to_json(named_struct(
    'contact', named_struct('email', concat(lower(fn),'.',lower(ln),'@example.com'),
                            'phone', concat('+1', cast(cast(rand()*9000000000+1000000000 AS BIGINT) AS STRING))),
    'loyalty', named_struct('tier', element_at(array('GOLD','SILVER','BRONZE'),cast(rand()*3 AS INT)+1),
                            'points', cast(rand()*100000 AS INT))
  )) AS user_payload
FROM (
  SELECT id,
    element_at(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth','David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen'), cast(rand()*20 AS INT)+1) AS fn,
    element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin'), cast(rand()*20 AS INT)+1) AS ln,
    element_at(array('5.2.0','5.3.1','6.0.0','5.9.2'), cast(rand()*4 AS INT)+1) AS ver,
    element_at(array('Checkin Flow','Booking Flow','Seat Map','Payment','Confirmation','Home','Manage Travel'), cast(rand()*7 AS INT)+1) AS scr,
    upper(substr(md5(cast(rand() AS STRING)),1,6)) AS pnr, cast(rand()*6+1 AS INT) AS hn
  FROM range(100000000)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE dkushari_uc.allegiant_air_perf.events_100m_dynamic SHALLOW CLONE dkushari_uc.allegiant_air_perf.events_100m_baseline;

-- COMMAND ----------

ALTER TABLE dkushari_uc.allegiant_air_perf.events_100m_dynamic ALTER COLUMN hit_payload  SET TAGS ('pii_aa' = 'name');

-- COMMAND ----------

ALTER TABLE dkushari_uc.allegiant_air_perf.events_100m_dynamic ALTER COLUMN user_payload SET TAGS ('pii_aa' = 'email');

-- COMMAND ----------

CREATE OR REPLACE TABLE dkushari_uc.allegiant_air_perf.events_100m_materialized AS
SELECT hit_id, dkushari_uc.allegiant_air_perf.mask_pii_name(hit_payload) AS hit_payload,
               dkushari_uc.allegiant_air_perf.mask_pii_email(user_payload) AS user_payload
FROM dkushari_uc.allegiant_air_perf.events_100m_baseline;
