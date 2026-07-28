# Test runbook — DSAR erasure × Lakeflow Declarative Pipelines

Deploy the pipeline, run it end-to-end (bronze → silver → gold), then process a
DSAR erasure queue that deletes/masks every named subject at every layer — with
zero pipeline downtime. Serverless UC workspace assumed. ~20–30 min.

Uses the modern API: `from pyspark import pipelines as dp`.

---

## Step 1 — Import the notebooks

```
sdp_integration/
  00_setup_and_landing.ipynb
  01_sdp_pipeline.ipynb                 # clean-silver variant
  01b_sdp_pipeline_cdc_variant.ipynb    # SCD1-silver variant (optional)
  02_erasure.ipynb
```

---

## Step 2 — Build the landing source + DSAR queue (notebook 00)

1. Open **`00_setup_and_landing`**, set `catalog` / `schema` (defaults
   `dkushari_uc` / `allegiant_air_sdp_dsar`), `num_users`=2000, `events_per_user`=5.
2. **Run all.** Builds `raw_user` (~10k rows) + a `dsar_request` queue seeded with
   3 subjects (mix of DELETE / OBFUSCATE).

✅ `SELECT * FROM <cat>.<schema>.raw_user LIMIT 5` shows cleartext PII;
`SELECT * FROM <cat>.<schema>.dsar_request` shows 3 PENDING requests.

---

## Step 3 — Create the Declarative Pipeline (notebook 01)

> `01_sdp_pipeline` is **pipeline source code** — you do NOT "Run all" it like a
> normal notebook. You attach it to a Lakeflow pipeline and Databricks executes it
> as the DAG. Running it interactively will error on the `@dp.table` decorators.

### 3.1 Open the create-pipeline flow

**Jobs & Pipelines → Create → ETL pipeline** (a.k.a. Declarative / Lakeflow
pipeline). This opens the **Lakeflow pipeline editor**.

### 3.2 Point it at the EXISTING notebook (don't use the generated sample)

The editor offers to scaffold a **starter project** (a new folder with sample
`transformations/*.py` / `explorations/` files). **Do not build the demo from those
sample files** — you want *this* repo's notebook.

- If asked "Start with sample code / Add existing assets" → choose **add existing
  assets** (or create the project, then in the editor's file tree **remove the
  sample transformation** and **add existing source code**).
- **Add source code path:**
  `/Users/<you>/allegiantair/allegiant-json-pii-masking/dsar_erasure/sdp_integration/01_sdp_pipeline`
  (the notebook in your workspace Git folder).
- The pipeline's **root/source folder** can stay at its default; only the source
  file above matters. The graph is defined entirely by `01_sdp_pipeline`.

### 3.3 Settings (right-hand **Settings** / gear panel)

| Setting | Value |
|---|---|
| **Serverless** | ✅ on |
| **Default catalog** | your catalog (e.g. `dkushari_uc`) |
| **Default schema / target** | `allegiant_air_sdp_dsar` (same schema `00` created) |
| **Pipeline mode** | Triggered (fine for the demo) |

Then open **Advanced → Configuration** and add two key/value pairs (this is how the
notebook's `cfg("dsar.catalog", …)` / `cfg("dsar.schema", …)` resolve — they must
point at the schema holding `raw_user`):

| Key | Value |
|---|---|
| `dsar.catalog` | your catalog (e.g. `dkushari_uc`) |
| `dsar.schema` | `allegiant_air_sdp_dsar` |

> Default catalog/schema and the `dsar.*` config should all point to the **same**
> catalog + `allegiant_air_sdp_dsar` schema, so the pipeline reads `raw_user` and
> publishes `bronze/silver/gold_user` right next to it.

### 3.4 Run it

Click **Start** (top of the editor). First run does an initial load of all three
datasets.

✅ The graph shows a clean linear medallion `bronze_user → silver_user → gold_user`,
all green. Verify the output:

```sql
SELECT user_id, email, full_name, revenue FROM <cat>.<schema>.silver_user LIMIT 5;  -- PII redacted, user_id/revenue intact
SELECT user_id, lifetime_revenue, event_count FROM <cat>.<schema>.gold_user LIMIT 5;  -- per-customer rollup
```

**If the graph doesn't come up:**
- `dp` import error / "Run all" fails → you ran the notebook interactively; attach it
  to a pipeline instead.
- `TABLE_OR_VIEW_NOT_FOUND: …raw_user` → the `dsar.catalog`/`dsar.schema` config
  doesn't match where `00` wrote `raw_user`, or `00` wasn't run. Fix the config and
  re-run `00` if needed.
- Empty/half graph → confirm the source file is `01_sdp_pipeline` and the sample
  transformation was removed.

### (Optional) CDC variant

To demo the SCD1 silver that matches Allegiant's real `dbo_user_silver_cdc`:
create a **second** pipeline pointing at `01b_sdp_pipeline_cdc_variant`, with
`dsar.schema` = `allegiant_air_sdp_dsar_cdc` (its own schema). Re-run `00` against
that schema first. Everything else is identical.

---

## Step 3b — Running the pipeline: incremental vs full refresh

- **Incremental** (normal run): processes only new/changed commits. UI **Start**,
  or `databricks pipelines start-update <id>`.
- **Full refresh** (reset & rebuild): resets all tables, re-reads `raw_user` from
  scratch. UI **Start ▾ → Full refresh all**, or
  `databricks pipelines start-update <id> --full-refresh`.
- Wait for completion: `databricks pipelines get-update <id> <update-id>` →
  `COMPLETED`.

> On a billion-row source prefer **incremental** for routine erasures (the erased
> subject is already gone from the base tables). Reserve full refresh for forcing
> the gold MV to recompute or a clean rebuild.

---

## Step 4 — Dry-run the erasure (notebook 02)

1. Open **`02_erasure`**. Set `catalog` / `schema` to match; `dry_run` = **`true`**;
   `do_purge` = `true`; `pipeline_id` = your pipeline id (for the gold refresh).
2. **Run all.**

✅ Sections 1–3 list PENDING requests, resolve each email → `user_id`, and pre-count
matches at every layer. Section 4 prints the exact SQL it *would* run. No changes.

---

## Step 5 — Run the live erasure

1. Set `dry_run` = **`false`**. **Run all.**
2. What happens:
   - **Section 4** — per subject, DELETE or OBFUSCATE across `silver → bronze → raw`.
   - **Section 5** — VACUUM the modified base tables (serverless zero-retention).
   - **Section 6** — triggers a gold MV refresh (via `pipeline_id`) so gold
     re-derives from the erased silver.
   - **Section 7** — validates no cleartext trace, asserts DELETE subjects gone from
     every base table, marks requests **COMPLETE**.

✅ `PASS — all requested subjects erased from base tables with no cleartext trace.`
and `dsar_request` all `COMPLETE`.

> **Gold is validated on the base tables, then refreshed.** Section 7's PASS asserts
> the erasure on `raw/bronze/silver` (the source of truth). The section-6 gold MV
> refresh is **asynchronous** — `start_update` returns immediately — so at the instant
> section 7 prints, `gold_user` may still show the erased subject's aggregate row
> (section 7 prints it with a `<- refresh gold MV` note). That's expected: **wait for
> the pipeline update triggered in section 6 to reach `COMPLETED`**, then re-query
> `gold_user` — the DELETE subject's row is gone. Gold is derived from silver, so once
> silver is clean and gold recomputes, gold is clean.

> If you didn't set `pipeline_id`, refresh gold manually: UI **Start**, or
> `databricks pipelines start-update <id> --full-refresh`.

---

## Step 6 — Prove the pipeline never failed

Open the pipeline — the latest update is **healthy**, no `append-only source` /
`FAILED fatally` error. That behavior was impossible before `skipChangeCommits`.

---

## Step 7 — Idempotency (full refresh + repeated incrementals)

After the erasure, run: **incremental → full refresh → incremental**, checking
counts each time. **Use only the DELETE-requested user_ids here** (the default queue
= REQ-001 DELETE, REQ-002 **OBFUSCATE**, REQ-003 DELETE):

```sql
-- <deleted ids> = the user_ids of the DELETE requests ONLY (not the OBFUSCATE one)
SELECT 'raw' l,count(*) c, sum(case when user_id IN (<deleted ids>) then 1 else 0 end) subj FROM <cat>.<schema>.raw_user
UNION ALL SELECT 'bronze',count(*), sum(...) FROM <cat>.<schema>.bronze_user
UNION ALL SELECT 'silver',count(*), sum(...) FROM <cat>.<schema>.silver_user
UNION ALL SELECT 'gold',  count(*), sum(...) FROM <cat>.<schema>.gold_user;
```

✅ For **DELETE** subjects: `subj = 0` on every layer, counts identical across all
updates. A full refresh re-reads `raw_user` (subject already gone), so deleted
subjects never return.

> **Expected — OBFUSCATE keeps its rows.** An OBFUSCATE subject (REQ-002) is *not*
> removed — its rows stay at every layer with PII **redacted** in place. So its row
> count stays non-zero (e.g. 5 in the clean-silver variant); that's correct, not a
> failure. To verify OBFUSCATE, check the *content* is masked rather than the count:
> ```sql
> SELECT user_id, email, full_name FROM <cat>.<schema>.raw_user WHERE user_id = '<obfuscated id>';
> -- email/full_name = ***REDACTED***, row still present
> ```

---

## What this proves to Allegiant

| Question | Answer |
|----------|--------|
| Delete PII from all layers incl. bronze? | Yes — DELETE/OBFUSCATE at raw+bronze+silver + VACUUM, gold MV refreshed |
| Without breaking the append-only stream? | Yes — `skipChangeCommits` on the two streaming hops |
| Multiple subjects at once? | Yes — `02` processes the whole PENDING `dsar_request` queue |
| Does it add latency? | Negligible — skipping a commit just advances the offset |
| Future erasures need a pipeline stop? | No — once `skipChangeCommits` is set, erasures run live |

---

## Cleanup

```sql
DROP SCHEMA IF EXISTS <catalog>.<schema> CASCADE;
```

Then delete the pipeline(s) from Jobs & Pipelines.
