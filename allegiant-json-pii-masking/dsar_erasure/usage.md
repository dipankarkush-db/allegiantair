# Usage — running the per-subject DSAR erasure notebooks

Step-by-step guide to run the CCPA/DSAR erasure demo. See [`README.md`](README.md) for the solution overview
(what it is, how it differs from blanket masking, the design decisions).

## Prerequisites

- A Unity Catalog-enabled workspace and permission to create objects in your target catalog/schema.
- **Serverless** compute (or a cluster) — the notebooks are Python + Spark SQL.
- Permission to set **UC column tags** (`ALTER TABLE … SET TAGS`) and to run `VACUUM`.

Everything is **self-contained** — the notebooks create their own schema, demo tables, data, tags, request
table, and registry. No pre-provisioned assets are required. All names are widget-driven, so nothing is
hard-coded to any particular workspace.

## Steps (demo)

1. **Clone the repo as a Git folder.** In Databricks: **Workspace → Create → Git folder**, paste the repo URL,
   and clone it under your Home.
2. **Open** `allegiant-json-pii-masking/dsar_erasure/` and attach serverless (or a cluster).
3. **Run `00_setup_and_generate`.** Sets the widgets (below), then creates the isolated schema, the demo
   tables + thousands of background subjects, **tags the PII columns** (`pii=<type>` — name/email/phone/address/pnr),
   tags the employee table (`subject_scope=employee`), and seeds `dsar_request` with sample requests (a mix of
   `DELETE` and `OBFUSCATE`, each with a 45-day deadline).
4. **Run `01_pii_column_registry`.** Reads the column tags from `information_schema.column_tags` (manual
   `pii=` tags **and** native `class.*` Data-Classification tags, manual winning on conflict) and
   **auto-seeds `pii_column_registry`** — enriching each tagged column with its match/erase metadata via a
   small role map, plus each table's `subject_scope`. This is the config that drives the engine.
5. **Then choose one path:**
   - **Step-by-step demo:** run `02_subject_erasure_engine` (erase — honours each request's `request_type`
     and the `subject_scope` filter), then `03_physical_purge` (physically remove the raw bytes + scrub the
     request table). Inspect the outputs at each step.
   - **One-shot / production driver:** run `04_orchestrate_and_validate` **instead of 02+03** — it does erase
     → purge → **no-trace validation** → report inline, the way the scheduled monthly job would, and can loop
     over multiple `catalog.schema` targets.

> **Run order:** always `00` → `01` first, then **either** `02` + `03` **or** `04`. Not both — `04`
> re-implements 02+03. To re-run from a clean state, re-run `00` + `01` to reset requests to `PENDING`.

## What you'll see

- **`02` / `04`** print a per-(request, table) audit showing, for each subject, how many rows were
  **obfuscated** vs **deleted**. Spot-checks show:
  - an **OBFUSCATE** subject → row still present, **all** PII cells (name, email, phone, address, pnr)
    replaced with the redaction token, non-PII (e.g. revenue) preserved; JSON payloads keep their structure
    with only `name` + URL params redacted;
  - a **DELETE** subject → the matched rows are gone (count 0);
  - a background subject → completely untouched;
  - the **employee-scoped** `employee_crew` table → **skipped** on a customer run: a planted subject's row
    there is left fully intact.
- **`04`** ends with `✅ COMBINED VALIDATION PASSED — no trace of any processed subject remains` (or a table
  of residual findings if anything was missed). This is your per-run compliance evidence.
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
**`02`** — `catalog`, `schema`, `redaction_token`, `dry_run` (`true` default = count matches only, no writes),
`subject_scope` (`customer` default \| `employee` \| `all`).
**`03`** — `catalog`, `schema`, `redaction_token`, `do_purge` (`false` = REORG only, skip VACUUM),
`subject_scope` (match the erase run).

**`04_orchestrate_and_validate`** (monthly driver, cross-catalog/schema)

| Widget | Meaning |
|---|---|
| `catalog` / `schema` | single-target fallback — used only when `targets` is blank |
| `targets` | list of `catalog.schema` entries to erase, comma- or newline-separated (e.g. `cat1.schema_a, cat1.schema_b, cat2.schema_c`). Blank = run the single `catalog`+`schema` pair |
| `redaction_token` | value written into PII cells in OBFUSCATE mode (default `***REDACTED***`) |
| `dry_run` | `true` (default) = count matches only, no writes; `false` = erase + purge |
| `do_purge` | `false` = REORG only, skip the zero-retention VACUUM |
| `subject_scope` | `customer` (default) \| `employee` \| `all` — which tables to erase |
| `request_source` | `per_schema` (default — each target reads/scrubs its own `dsar_request`) \| `central` (one master `dsar_request`, applied to every target) |
| `request_catalog` / `request_schema` | location of the master `dsar_request` when `request_source=central` (ignored otherwise) |

The `targets`, `subject_scope`, `redaction_token`, `dry_run`, and `do_purge` semantics apply **per target** —
each schema is loaded, erased, purged, and validated independently, then rolled up into a combined report.

**`05_intake_onetrust`** — `catalog`, `schema`, `onetrust_base_url`, `secret_scope`, `deadline_days`,
`use_mock` (`true` default = full MERGE path against a sample payload, no live API), `page_size`.

**`99_subject_erasure_all_match_reference`** — same widgets as `04`. Comparison-only: identical engine, but
the match rule is **all identifiers must match** instead of email-first. Run `00`→`01`, then this, to contrast
its audit counts with `04` on the same data. Not scheduled.

## Onboarding your own tables

The engine is config-driven — no code change per table:

1. **Tag the PII columns** on your table with `pii=<type>`, where `<type>` is one of `name`, `email`, `phone`,
   `address`, or `pnr` (extend as needed). **Only `email` and `name` are used to *match* a subject** (email
   first, name as fallback for email-less tables); `phone`/`address`/`pnr` are erased on a matched row but are
   not match keys:
   ```sql
   ALTER TABLE <catalog>.<schema>.<your_table> ALTER COLUMN <col> SET TAGS ('pii' = 'email');
   ```
   (If UC **Data Classification** is enabled on the catalog, its `class.*` tags are picked up automatically —
   manual `pii=` tags still win, and are required for JSON-payload and `pnr` columns.)
2. **Employee-only sources:** tag the **table** with `subject_scope=employee` so it is excluded from customer
   erasure:
   ```sql
   ALTER TABLE <catalog>.<schema>.<your_table> SET TAGS ('subject_scope' = 'employee');
   ```
3. If the table has **split or oddly-named name columns** (e.g. `first_nm`/`last_nm`/`full_name`) or a
   **JSON payload column**, add one line to the small role map in `01` so it knows which name part each
   column is (or which column is the JSON payload). Plain scalar `email` columns need nothing extra.
4. Re-run `01` — the registry now includes your table. `02`/`03`/`04` pick it up automatically.

## Production path (real requests) — `05` intake → `04` driver

For a real run, skip the demo scaffolding (`00`) and use:

1. **Seed each target schema's registry from tags** — run `01_pii_column_registry` against every real schema
   you want in scope. Tag your PII columns (`pii=<type>`) and, for any employee-only source, tag the
   **table** with `subject_scope=employee`. Each schema ends up with its own `pii_column_registry`.
2. **`05_intake_onetrust`** — pulls open requests from OneTrust and upserts them into `dsar_request`. Set
   `onetrust_base_url` + a `secret_scope` holding `client_id`/`client_secret`; run with `use_mock=true`
   first to validate the MERGE, then `use_mock=false` once creds are wired.
3. **`04_orchestrate_and_validate`** — list every schema in the `targets` widget (comma- or newline-separated
   `catalog.schema`; blank = the single `catalog`+`schema` pair). Pick `request_source`: `per_schema` (each
   target reads its own `dsar_request`) or `central` (one master `dsar_request` at
   `request_catalog.request_schema`, applied to all targets). Keep `subject_scope=customer` (default), run
   with **`dry_run=true`** and confirm the per-target audit counts, then re-run with **`dry_run=false`** to
   erase + purge + validate every target.

### Cross-catalog / cross-schema coverage

`04` loops the same engine (load config → erase → purge → validate → report) once per `catalog.schema` in
`targets`. It is a **loop per schema**, not one giant cross-catalog scan, for two reasons: a smaller blast
radius (a missing/empty registry or an out-of-scope schema is logged and skipped gracefully — one bad schema
never crashes the run or touches another), and a per-schema audit trail for compliance (the registry is
per-schema, scope is a per-table tag, so the report groups evidence by schema). The report prints a
per-(target, request, table) audit plus a rolled-up per-target summary, and the combined verdict is `PASS`
only if every processed target passes. The loop is plain Python inside the one notebook — no
`dbutils.notebook.run()` — so the monthly job stays **two tasks**: `05` intake → `04` erase-all-targets.

### Scheduling the monthly job

Create one Databricks **Job** with two tasks on a **monthly** schedule: task 1 = `05_intake_onetrust`,
task 2 = `04_orchestrate_and_validate` (depends on task 1, `dry_run=false`, with `targets` set to all
in-scope schemas). Both read their config from tables, so each run needs no edits.

## Notes

- **Zero-retention `VACUUM` is destructive** — it disables time-travel recovery, which is the point for CCPA
  erasure. Run `02`/`04` with `dry_run=true` first to confirm the match set. On serverless the notebooks set
  the table property `delta.deletedFileRetentionDuration = 'interval 0 hours'` and run a plain `VACUUM` (no
  `RETAIN` clause), because the `spark.databricks.delta.retentionDurationCheck.enabled` session conf is not
  settable there.
- Nothing here touches the blanket-masking notebooks in `../notebooks`, `../sql`, or `../performance`.
