# Usage — running the per-subject DSAR erasure notebooks

Step-by-step guide to run the CCPA/DSAR erasure demo. See [`README.md`](README.md) for the solution overview
(what it is, how it differs from blanket masking, the design decisions).

## Prerequisites

- A Unity Catalog-enabled workspace and permission to create objects in your target catalog/schema.
- **Serverless** compute (or a cluster) — the notebooks are Python + Spark SQL.
- Permission to set **UC column tags** (`ALTER TABLE … ALTER COLUMN … SET TAGS`) and to run `VACUUM`.

Everything is **self-contained** — the notebooks create their own schema, demo tables, data, tags, request
table, and registry. No pre-provisioned assets are required. All names are widget-driven, so nothing is
hard-coded to any particular workspace.

## Steps

1. **Clone the repo as a Git folder.** In Databricks: **Workspace → Create → Git folder**, paste the repo URL,
   and clone it under your Home.
2. **Open** `allegiant-json-pii-masking/dsar_erasure/` and attach serverless (or a cluster).
3. **Run `00_setup_and_generate`.** Sets the widgets (below), then creates the isolated schema, the five demo
   tables + thousands of background subjects, **tags the PII columns** (`pii=<type>`), and seeds
   `dsar_request` with sample requests (a mix of `DELETE` and `OBFUSCATE`).
4. **Run `01_pii_column_registry`.** Reads the column tags from `information_schema.column_tags` and
   **auto-seeds `pii_column_registry`** (enriching each tagged column with its match/erase metadata via a
   small role map). This is the config that drives the engine.
5. **Then choose one path:**
   - **Step-by-step demo:** run `02_subject_erasure_engine` (erase — honours each request's `request_type`),
     then `03_physical_purge` (physically remove the raw bytes + scrub the request table). Inspect the
     outputs at each step.
   - **One-shot production path:** run `04_orchestrate_and_validate` **instead of 02+03** — it does erase →
     purge → **no-trace validation** → report inline, the way the scheduled monthly job would.

> **Run order:** always `00` → `01` first, then **either** `02` + `03` **or** `04`. Not both — `04`
> re-implements 02+03. To re-run from a clean state, re-run `00` + `01` to reset requests to `PENDING`.

## What you'll see

- **`02` / `04`** print a per-(request, table) audit showing, for each subject, how many rows were
  **obfuscated** vs **deleted**. Spot-checks show:
  - an **OBFUSCATE** subject → row still present, PII cells replaced with the redaction token, non-PII (e.g.
    revenue) preserved; JSON payloads keep their structure with only `name` + URL params redacted;
  - a **DELETE** subject → the matched rows are gone (count 0);
  - a background subject → completely untouched.
- **`04`** ends with `✅ VALIDATION PASSED — no trace of any processed subject remains` (or a table of
  residual findings if anything was missed). This is your per-run compliance evidence.
- The `dsar_request` table ends with its raw identifiers scrubbed and `status=COMPLETE`.

## Widget reference

**`00_setup_and_generate`**

| Widget | Meaning |
|---|---|
| `catalog` / `schema` | where the demo schema + tables are created |
| `num_background_rows` | non-target subjects per table (default `5000`) |
| `redaction_token` | value written into PII cells in OBFUSCATE mode (default `***REDACTED***`) |
| `deadline_days` | request deadline = `request_date + N days` (default `45`) |
| `tag_key` | UC column tag key used to mark PII columns (default `pii`) |

**`01`** — `catalog`, `schema`, `tag_key` (must match `00`).
**`02`** — `catalog`, `schema`, `redaction_token`, `dry_run` (`true` = count matches only, no writes).
**`03`** — `catalog`, `schema`, `redaction_token`, `do_purge` (`false` = REORG only, skip VACUUM).
**`04`** — `catalog`, `schema`, `redaction_token`, `do_purge`.

## Onboarding your own tables

The engine is config-driven — no code change per table:

1. **Tag the PII columns** on your table with `pii=<type>`, where `<type>` is one of `name`, `email`, or
   `pnr` (extend as needed):
   ```sql
   ALTER TABLE <catalog>.<schema>.<your_table> ALTER COLUMN <col> SET TAGS ('pii' = 'email');
   ```
2. If the table has **split or oddly-named name columns** (e.g. `first_nm`/`last_nm`/`full_name`) or a
   **JSON payload column**, add one line to the small role map in `01` so it knows which name part each
   column is (or which column is the JSON payload). Plain scalar `email` columns need nothing extra.
3. Re-run `01` — the registry now includes your table. `02`/`03`/`04` pick it up automatically.

## Scheduling the monthly job

Create a Databricks **Job** with `04_orchestrate_and_validate` as the task and a **monthly** schedule. It
reads PENDING requests from `dsar_request` and the config from `pii_column_registry`, so each run needs no
edits. In production, replace the seeded requests with an intake step that upserts new requests into
`dsar_request` with `status='PENDING'` (e.g. a REST/Lakeflow pull from your DSAR intake system).

## Notes

- **Zero-retention `VACUUM` is destructive** — it disables time-travel recovery, which is the point for CCPA
  erasure. Run `02` with `dry_run=true` first to confirm the match set. On serverless the notebooks set the
  table property `delta.deletedFileRetentionDuration = 'interval 0 hours'` and run a plain `VACUUM` (no
  `RETAIN` clause), because the `spark.databricks.delta.retentionDurationCheck.enabled` session conf is not
  settable there.
- Nothing here touches the blanket-masking notebooks in `../notebooks`, `../sql`, or `../performance`.
