# Per-Subject CCPA/DSAR Erasure

Targeted, in-place, physically-purged erasure of **one specific subject's** data across many tables — the
CCPA/DSAR "right to be forgotten" flow. This is a **separate layer** from the blanket column masking in the
parent repo, built entirely in **Databricks**. It is fully **config-driven** and **self-contained**: the
notebooks create their own isolated schema and demo tables, so you can run the whole thing end-to-end in any
workspace without pre-provisioning anything.

All catalog / schema names are **widget-driven** (defaults shown in the notebooks). Point the widgets at your
own catalog and schema; nothing below is hard-coded.

## Why this is separate from the masking demo

| | Blanket masking (`../notebooks/pii_masking_json_demo`) | Per-subject erasure (this folder) |
|---|---|---|
| **Shape** | Mask a whole key/column for **every** row | Erase **only the matched subject's** rows |
| **Who's affected** | Everyone's value in the column | One subject; everyone else untouched |
| **Enforcement** | Dynamic — governed tag + schema-level ABAC | Materialized `UPDATE`/`DELETE` in place + physical purge |
| **Persistence** | Original bytes remain (masked at read) | Raw bytes physically removed (CCPA "no trace") |
| **Trigger** | Standing policy | A per-subject DSAR/erasure request |

The masking layer is the right tool for day-to-day column masking. Erasure is a different requirement: don't
blank the column for all subjects — erase the one subject, everywhere, and remove the raw bytes.

## The flow

1. A privacy request names a subject (first name, last name, email) → lands in **`dsar_request`** (a native
   Delta request table) with a **45-day deadline** (`request_date + N days`, configurable) and
   `status=PENDING`. Each request is tied to **exactly one subject/email** and carries a **`request_type`**:
   `OBFUSCATE` or `DELETE`.
2. A **`pii_column_registry`** declares which columns are PII across which tables, how to **match** a subject,
   how to **erase** each column, and each table's **`subject_scope`** (customer/employee). It is
   **auto-seeded from UC column tags** (see below) and is the only thing the engine scans — never untagged
   or `system.*` tables.
3. The **erasure engine** builds a dynamic `WHERE` per (request, table) — **email-first, name fallback** (see
   Design decisions) — and, honouring `request_type`, either:
   - **OBFUSCATE** → in-place `UPDATE` on the **same table**, erasing **all** of the matched subject's PII
     cells (name, email, phone, address, pnr → redaction token; JSON → targeted `regexp_replace` redaction
     reusing the masking layer's approach). The row stays; co-located non-PII (revenue/metrics) is preserved.
   - **DELETE** → `DELETE FROM … WHERE <match>`, removing the whole matched row.

   Every other subject's rows are left intact either way.
4. **Physical purge** (`REORG … APPLY (PURGE)` + zero-retention `VACUUM`) removes the pre-erasure raw bytes so
   nothing is recoverable via time-travel, and **scrubs the request table** itself, marking `status=COMPLETE`.

## Design decisions

- **Two erasure modes, driven by `request_type`:** OBFUSCATE (redact PII cells, keep the row) and DELETE
  (remove the whole row). Both are physically purged. (Allegiant's word for a request is "obfuscation" — that
  is OBFUSCATE mode.)
- **Match = email-first, name fallback** (decided 2026-07-24 from the Kartik ↔ Kabeer transcript). The
  request's **email is the authoritative, unique identifier**: a table with an email column matches on
  **email alone** (a subject may carry a *different* name in some tables — we deliberately ignore name there,
  and this avoids missing a subject whose name is spelled differently across systems). Only tables with **no
  email column** fall back to name (first + last, or a single `full_name` = 'First Last'). For a JSON payload
  the string must contain the subject's email (primary), falling back to first + last only when the request
  itself carries no email. This supersedes the earlier "all identifiers must match" rule, which is preserved
  for comparison in `99_subject_erasure_all_match_reference` (identical engine, that rule only).
- **One request = one subject.** Each `dsar_request` row is tied to exactly one email/subject; multiple
  requests are processed independently in a run. Requests carry a **45-day SLA** (`deadline_date`).
- **All tagged PII on a matched row is erased** — not just the match key. `name`, `email`, `phone`,
  `address`, `pnr`, and in-JSON values are all redacted (OBFUSCATE) or removed with the row (DELETE).
  `phone`/`address`/`pnr` are erased PII but are **not** match keys.
- **Scalar erasure value = fixed redaction token** (default `***REDACTED***`, widget-configurable) in
  OBFUSCATE mode. Physical purge still removes the original raw bytes, so it is CCPA-grade; the token just
  marks the erased cell in the live version. (NULL or a deterministic hash are trivial config swaps.)
- **Employee vs. customer scope.** DSAR/CCPA is a **customer** right, so the engine **skips employee-only
  tables** (e.g. Merlot crew/internal tables) on a customer request. Scope is a **table-level** UC tag
  `subject_scope` = `customer` | `employee` (untagged → `customer`, safe for existing tables); `01` projects
  it onto the registry and the engine filters on the `subject_scope` widget (`customer` default, or
  `employee` / `all`). `02`/`03`/`04` all honour it consistently.
- **PII is declared once via UC column tags** (`pii=<type>` — key configurable; types: `name`, `email`,
  `phone`, `address`, `pnr`). `00` tags the columns; `01` reads the tags from
  `information_schema.column_tags` and **auto-seeds the registry**, enriching each with the match/erase
  metadata a tag can't hold (identifier role, is-identifier, strategy) plus the table's `subject_scope`. Tag
  a new column → it's in scope; no code change.
- **Native Data Classification is a second, automatic tag source.** If UC
  [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification)
  is enabled on the catalog, its agentic scanner auto-applies system `class.*` tags (`class.email_address`,
  `class.name`, `class.phone_number`, …) within ~24h of a table being created. `01` reads those too and maps
  them to our vocabulary, so the registry **auto-discovers** PII with no manual tagging. The manual `pii=`
  tag **wins on conflict** (lets you override the scanner and declare JSON-payload/`pnr` columns a
  column-level scan can't). Classifier-found types other than email/name are **erased but not used as match
  keys** — email stays the match primary, name the fallback; auto-discovery widens *what* is erased, not
  *who* is matched.
- **OneTrust REST intake** (`05_intake_onetrust`) pulls open privacy requests and **upserts** them into
  `dsar_request` (idempotent `MERGE`), replacing the seeded demo requests in production. Ships with a
  `use_mock` mode so the full path runs before Allegiant's OneTrust creds are wired.
- **Idempotent** — `00` rebuilds its demo schema cleanly on every run; `05`'s `MERGE` never duplicates or
  resets a `COMPLETE` request.

## Cross-catalog / cross-schema coverage

Kartik's requirement is that the job "look at all tables across all schemas and all catalogs" — tag-driven,
not a fixed single schema. `04_orchestrate_and_validate` (the monthly-job driver) handles this by looping the
same engine (load config → erase → purge → validate → report) over a **list of `catalog.schema` targets**
supplied by the `targets` widget (comma- or newline-separated, e.g. `cat1.schema_a, cat1.schema_b,
cat2.schema_c`). Blank `targets` falls back to the single `catalog`+`schema` widgets, so a one-schema run is
unchanged.

It is deliberately a **loop per schema**, not one giant cross-catalog scan:

- **Smaller blast radius.** Each target's erase/purge/validate is self-contained. A missing or empty
  registry, or an out-of-scope-only schema, is logged and skipped gracefully — one bad schema never crashes
  the whole run or half-erases another.
- **Per-schema audit trail for compliance.** The registry is per-schema (seeded by `01`), and scope
  (`customer`/`employee`) is a per-table tag, so the natural unit of evidence is the schema. The report
  prints a per-(target, request, table) audit **and** a rolled-up per-target summary; the combined
  validation verdict is `PASS` only if **every** processed target passes.
- **Still one notebook, one job.** The loop is plain Python inside `04` — no `dbutils.notebook.run()`. The
  monthly job is **two tasks**: `05_intake_onetrust` (pull requests) → `04_orchestrate_and_validate`
  (erase-all-targets).

The `dsar_request` table can live either **per schema** (`request_source=per_schema`, default — each target
reads and scrubs its own `dsar_request`) or **centrally** (`request_source=central` with
`request_catalog`/`request_schema` — one master `dsar_request` is read once and the same PENDING requests are
applied across every target, then scrubbed + completed once after the loop).

## Notebooks (run in order)

| Notebook | What it does |
|---|---|
| `00_setup_and_generate` | Creates an isolated demo schema + all demo tables (scalar with name/email/**phone**/**address**, email-only, single-name+PNR, split-name, nested-JSON, and an **employee-scoped** table) with thousands of background subjects, **tags the PII columns** (`pii=<type>`) and the employee table (`subject_scope=employee`), and seeds `dsar_request` with sample requests (mixed DELETE / OBFUSCATE, each with a 45-day deadline). |
| `01_pii_column_registry` | **Auto-seeds `pii_column_registry`** from the manual `pii=` tags **and** native `class.*` Data-Classification tags (manual wins), a small role map, and each table's `subject_scope`. The config that drives the engine. |
| `02_subject_erasure_engine` | Targeted, per-subject erase honouring `request_type` (OBFUSCATE → in-place redact; DELETE → row removal), **email-first/name-fallback** match, **scope filter**; before/after counts; spot-checks both modes + the skipped employee table. `dry_run` on by default. |
| `03_physical_purge` | `REORG` + zero-retention `VACUUM` the in-scope affected tables; scrub + complete the request table. |
| `04_orchestrate_and_validate` | **The monthly-job driver.** Folds erase → purge → **validate no trace remains** → report into one notebook: email-first/name-fallback match, customer/employee scope filter, `dry_run` guard, OBFUSCATE+DELETE, and a **cross-catalog/schema `targets` loop** with per-target audit + combined verdict. `02`/`03` are the same steps broken out for demo/inspection. |
| `05_intake_onetrust` | **DSAR intake (production).** Pulls open requests from the **OneTrust** REST API (OAuth2, paginated) and **upserts** them into `dsar_request` (idempotent `MERGE`). `use_mock=true` runs the full path with no live creds. Wire as job task 1, `04` as task 2. |
| `99_subject_erasure_all_match_reference` | **Comparison-only reference.** Identical to `04` in every way **except the match rule** — here a subject matches only if **all** registered identifiers match (email AND first AND last / full_name). Run against the same seeded data to contrast with the mainline **email-first** rule. Not part of the scheduled job. |

See **`usage.md`** for step-by-step run instructions, widget reference, and how to onboard your own tables.

**Run order (demo):** `00` → `01` first, then **either** `02` + `03` (step-by-step) **or** `04` (one-shot).
Not both — `04` re-implements 02+03. To re-run, re-run `00` + `01` for a fresh PENDING state.

**Run order (production):** `01` (seed the registry from tags — no demo data) → `05_intake_onetrust` (pull
real requests into `dsar_request`) → `04_orchestrate_and_validate` (`dry_run=true` to confirm, then
`dry_run=false`). Schedule `05` then `04` as two tasks of one monthly Job. Seed each target schema's registry
with `01` first.

## Demo tables (created by `00`, in the schema the widgets point at)

- `customer_profile` — scalar `first_name` / `last_name` / `email` / `phone` / `street_address` (+ non-PII
  `home_city`, `lifetime_revenue`) — matches on **email**, then erases all five PII cells
- `contact_email_only` — `email` identifier + PII `phone` (+ non-PII `opt_in`) — email-only match
- `booking` — single `full_name` column + `pnr` (+ non-PII `flight_no`, `fare_usd`) — no email → name match
- `loyalty_split` — split `first_nm` / `last_nm` (+ non-PII `tier`, `points`) — no email → name match
- `app_hits_json` — GA-style nested JSON: `appInfo.name` + URL `firstName`/`lastName`/`email` inside
  `hit_payload`; `appName` and `revenue` are non-PII and survive
- `employee_crew` — an **employee-scoped** table (table tag `subject_scope=employee`); a planted subject's
  row here must be **left intact** on a customer DSAR (proves the scope skip)
- `dsar_request` — the request table; `pii_column_registry` — the config

## Notes & caveats

- **Zero-retention `VACUUM` is destructive** and disables time-travel recovery for the table — that is the
  point for CCPA erasure. Use `02`/`04`'s `dry_run=true` to confirm the match set before purging. To purge on
  serverless, the notebooks set the table property `delta.deletedFileRetentionDuration = 'interval 0 hours'`
  then run a plain `VACUUM` (no `RETAIN` clause) — the session conf
  `spark.databricks.delta.retentionDurationCheck.enabled` is **not** settable on serverless / Spark Connect.
- Erasing raw *source files* on an object-storage lifecycle is a storage concern, separate from this
  table-level erasure; out of scope for these notebooks.
- Everything here is isolated from the masking demo — no shared schema, tables, tags, or policies. Nothing in
  `../notebooks`, `../sql`, or `../performance` is touched.
