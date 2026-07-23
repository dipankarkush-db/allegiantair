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
   Delta request table) with a **deadline** (`request_date + N days`, configurable) and `status=PENDING`.
   Each request carries a **`request_type`**: `OBFUSCATE` or `DELETE`.
2. A **`pii_column_registry`** declares which columns are PII across which tables, how to **match** a subject,
   and how to **erase** each column. It is **auto-seeded from UC column tags** (see below).
3. The **erasure engine** builds a dynamic `WHERE` per (request, table) and, honouring `request_type`, either:
   - **OBFUSCATE** → in-place `UPDATE` on the **same table**, erasing only the matched subject's PII cells
     (scalars → redaction token; JSON → targeted `regexp_replace` redaction reusing the masking layer's
     approach). The row stays; co-located non-PII (revenue/metrics) is preserved.
   - **DELETE** → `DELETE FROM … WHERE <match>`, removing the whole matched row.

   Every other subject's rows are left intact either way.
4. **Physical purge** (`REORG … APPLY (PURGE)` + zero-retention `VACUUM`) removes the pre-erasure raw bytes so
   nothing is recoverable via time-travel, and **scrubs the request table** itself, marking `status=COMPLETE`.

## Design decisions

- **Two erasure modes, driven by `request_type`:** OBFUSCATE (redact PII cells, keep the row) and DELETE
  (remove the whole row). Both are physically purged.
- **Scalar erasure value = fixed redaction token** (default `***REDACTED***`, widget-configurable) in
  OBFUSCATE mode. Physical purge still removes the original raw bytes, so it is CCPA-grade; the token just
  marks the erased cell in the live version. (NULL or a deterministic hash are trivial config swaps.)
- **Match = all registered identifiers on that table must match** (most conservative). A table with only
  `email` matches on email; a table with first + last + email requires all three; a single `full_name` column
  matches the full name string. This handles the "some tables only have an email" case without over-matching.
- **PII is declared once via UC column tags** (`pii=<type>` — key configurable). `00` tags the columns; `01`
  reads the tags from `information_schema.column_tags` and **auto-seeds the registry**, enriching each with
  the match/erase metadata a tag can't hold (identifier role, is-identifier, strategy). Tag a new column →
  it's in scope; no code change.
- **Native request table now; OneTrust/DSAR REST or Lakeflow intake documented as future** (see notebook `04`).
- **Idempotent** — `00` rebuilds its demo schema cleanly on every run.

## Notebooks (run in order)

| Notebook | What it does |
|---|---|
| `00_setup_and_generate` | Creates an isolated demo schema + all demo tables (scalar, email-only, single-name+PNR, split-name, nested-JSON) with thousands of background subjects, **tags the PII columns** (`pii=<type>`), and seeds `dsar_request` with sample requests (mixed DELETE / OBFUSCATE). |
| `01_pii_column_registry` | **Auto-seeds `pii_column_registry` from the column tags** + a small role map (the config that drives the engine). |
| `02_subject_erasure_engine` | Targeted, per-subject erase honouring `request_type` (OBFUSCATE → in-place redact; DELETE → row removal); before/after counts; spot-checks both modes. Supports `dry_run`. |
| `03_physical_purge` | `REORG` + zero-retention `VACUUM` the affected tables; scrub + complete the request table. |
| `04_orchestrate_and_validate` | Standalone monthly-job driver: erase (both modes) → purge → **validate no trace remains** → report. Deploy as a scheduled Databricks Job. |

See **`usage.md`** for step-by-step run instructions, widget reference, and how to onboard your own tables.

**Run order:** always `00` → `01` first, then **either** `02` + `03` (step-by-step demo) **or** `04`
(one-shot production path — it does erase + purge + validation inline). Not both — `04` re-implements 02+03.
To re-run, re-run `00` + `01` for a fresh PENDING state.

## Demo tables (created by `00`, in the schema the widgets point at)

- `customer_profile` — scalar `first_name` / `last_name` / `email` (+ non-PII `home_city`, `lifetime_revenue`)
- `contact_email_only` — only `email` (+ `phone`, `opt_in`)
- `booking` — single `full_name` column + `pnr` (+ non-PII `flight_no`, `fare_usd`)
- `loyalty_split` — split `first_nm` / `last_nm` (+ non-PII `tier`, `points`)
- `app_hits_json` — GA-style nested JSON: `appInfo.name` + URL `firstName`/`lastName`/`email` inside
  `hit_payload`; `appName` and `revenue` are non-PII and survive
- `dsar_request` — the request table; `pii_column_registry` — the config

## Notes & caveats

- **Zero-retention `VACUUM` is destructive** and disables time-travel recovery for the table — that is the
  point for CCPA erasure. Use `02`'s `dry_run=true` to confirm the match set before purging. To purge on
  serverless, the notebooks set the table property `delta.deletedFileRetentionDuration = 'interval 0 hours'`
  then run a plain `VACUUM` (no `RETAIN` clause) — the session conf
  `spark.databricks.delta.retentionDurationCheck.enabled` is **not** settable on serverless / Spark Connect.
- Erasing raw *source files* on an object-storage lifecycle is a storage concern, separate from this
  table-level erasure; out of scope for these notebooks.
- Everything here is isolated from the masking demo — no shared schema, tables, tags, or policies. Nothing in
  `../notebooks`, `../sql`, or `../performance` is touched.
