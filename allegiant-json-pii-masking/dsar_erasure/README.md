# Per-Subject CCPA/DSAR Erasure (Allegiant Air POC)

Targeted, in-place, physically-purged erasure of **one specific customer's** data across many tables — the
CCPA/DSAR "right to be forgotten" flow. This is a **separate layer** from the blanket column masking in the
parent repo, built entirely in **Databricks** on its own isolated schema and its own new tables.

## Why this is separate from the masking demo

| | Blanket masking (`../notebooks/pii_masking_json_demo`) | Per-subject erasure (this folder) |
|---|---|---|
| **Shape** | Mask a whole key/column for **every** row | Erase **only the matched subject's** rows |
| **Who's affected** | Everyone's value in the column | One customer; everyone else untouched |
| **Enforcement** | Dynamic — governed tag + schema-level ABAC | Materialized `UPDATE` in place + physical purge |
| **Persistence** | Original bytes remain (masked at read) | Raw bytes physically removed (CCPA "no trace") |
| **Trigger** | Standing policy | A OneTrust/DSAR request (~10/month) |

The masking repo is correct and unchanged — it's the right tool for day-to-day column masking. Erasure is a
different requirement (Kartik's clarification): don't blank the column for all customers, erase the one subject.

## The flow (mirrors Allegiant's current AWS/Glue job, rebuilt natively)

1. A privacy request names a customer (first name, last name, email) → lands in **`dsar_request`**
   (the native Delta replacement for their DynamoDB request table), with a **45-day deadline** and `status=PENDING`.
2. A **`pii_column_registry`** declares which columns are PII across which tables, how to **match** a subject,
   and how to **erase** each column (the equivalent of the DBA-provided PII column list).
3. The **erasure engine** builds a dynamic `WHERE` per (request, table) and runs an in-place `UPDATE` on the
   **same table**, erasing only the matched subject's PII — scalars → redaction token, JSON → targeted
   `regexp_replace` redaction (reusing the masking repo's approach). Every other customer's row is left intact.
4. **Physical purge** (`REORG … APPLY (PURGE)` + `VACUUM RETAIN 0`) removes the pre-erasure raw bytes so
   nothing is recoverable via time-travel, and **scrubs the request table** itself, marking `status=COMPLETE`.

## Design decisions (confirmed with the account team)

- **Scalar erasure value = fixed redaction token** `***REDACTED***`. Physical purge still removes the original
  raw bytes, so it is CCPA-grade; the token just marks the erased cell in the live version. (NULL or a
  deterministic hash are trivial config swaps.)
- **Match = all registered identifiers on that table must match** (most conservative). A table with only
  `email` matches on email; `customer_profile` requires first + last + email; `booking` matches the single
  `full_name` string; `loyalty_split` requires first_nm AND last_nm. This handles Kartik's "some tables only
  have an email" case without a silent over-match.
- **Native request table now; OneTrust REST/Lakeflow intake documented as future** (see notebook `04` §6).
- **Config-driven & idempotent** — onboard a new table by adding registry rows; no code change. `00` rebuilds
  the demo schema cleanly on every run.

## Notebooks (run in order)

| Notebook | What it does |
|---|---|
| `00_setup_and_generate` | Creates the isolated `allegiant_air_dsar` schema + all demo tables (scalar, email-only, single-name+PNR, split-name, nested-JSON) with thousands of background customers, and seeds `dsar_request` with 10 sample requests. |
| `01_pii_column_registry` | Creates + populates `pii_column_registry` (the config). |
| `02_subject_erasure_engine` | Targeted in-place `UPDATE` per subject; before/after counts; spot-checks. Supports `dry_run`. |
| `03_physical_purge` | `REORG` + `VACUUM` the erased tables; scrub + complete the request table. |
| `04_orchestrate_and_validate` | Standalone monthly-job driver: erase → purge → **validate no trace remains** → report. Deploy as a monthly Databricks Job. |

## Demo tables (all on `dkushari_uc.allegiant_air_dsar`, all created by `00`)

- `customer_profile` — scalar `first_name` / `last_name` / `email` (+ non-PII `home_city`, `lifetime_revenue`)
- `contact_email_only` — only `email` (+ `phone`, `opt_in`)
- `booking` — single `full_name` column + `pnr` (+ non-PII `flight_no`, `fare_usd`)
- `loyalty_split` — split `first_nm` / `last_nm` (+ non-PII `tier`, `points`)
- `app_hits_json` — GA-style nested JSON: `appInfo.name` + URL `firstName`/`lastName`/`email` inside
  `hit_payload`; `appName` and `revenue` are non-PII and survive
- `dsar_request` — the request table; `pii_column_registry` — the config

## Notes & caveats

- **`VACUUM RETAIN 0 HOURS`** is destructive and disables time-travel recovery for the table — that is the
  point for CCPA erasure. Use `02`'s `dry_run=true` to confirm the match set before purging. It disables the
  Delta retention-duration safety check per session (`spark.databricks.delta.retentionDurationCheck.enabled=false`).
- Erasing raw *source files* on a storage lifecycle (their 30-day bucket rule) is an object-storage concern,
  separate from this table-level erasure; out of scope for these notebooks.
- Everything here is isolated from the masking demo — no shared schema, tables, tags, or policies. Nothing in
  `../notebooks`, `../sql`, or `../performance` is touched.
