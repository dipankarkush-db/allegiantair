# Test runbook — DSAR erasure × SDP (step by step)

End-to-end guide to deploy the pipeline, run a DSAR erasure, and prove the
subject is erased at **every layer** with **zero pipeline downtime**.

Assumes a serverless-enabled Unity Catalog workspace. Total time ≈ 20–30 min.

---

## Step 1 — Import the folder

Import the three notebooks into your workspace (or use the Git folder that
already syncs this repo):

```
sdp_integration/
  00_setup_and_landing.ipynb
  01_sdp_pipeline.ipynb
  02_zero_downtime_erasure.ipynb
```

---

## Step 2 — Build the landing source (notebook 00)

1. Open **`00_setup_and_landing`**.
2. Set widgets (defaults are fine):
   - `catalog` = `dkushari_uc` (or your catalog)
   - `schema` = `allegiant_air_sdp_dsar`
   - `num_users` = `2000`, `events_per_user` = `5`
3. **Run all.** It drops/recreates the schema and builds `raw_user`
   (~10k rows).
4. **Copy the demo subject's email** printed in section 3, e.g.
   `alex.lucero0@example.com`. You'll paste it into notebook 02.

✅ Checkpoint: `SELECT * FROM <catalog>.<schema>.raw_user LIMIT 5` returns rows
with cleartext `email`/`full_name` and a `profile_json` blob.

---

## Step 3 — Create the Declarative Pipeline (notebook 01)

1. Go to **Jobs & Pipelines → Create → ETL/Declarative pipeline**.
2. **Source code:** add `01_sdp_pipeline`.
3. **Compute:** Serverless.
4. **Destination:** set **default catalog** = your catalog and **target schema**
   = `allegiant_air_sdp_dsar` (same schema as `raw_user`).
5. **Configuration** (Settings → Advanced → Configuration), add:
   | Key | Value |
   |-----|-------|
   | `dsar.catalog` | `dkushari_uc` |
   | `dsar.schema`  | `allegiant_air_sdp_dsar` |
6. **Start** the pipeline. Let the first update complete.

✅ Checkpoint: the pipeline graph shows `bronze_user → silver_user → gold_user`
all green. Verify:

```sql
SELECT * FROM <cat>.<schema>.bronze_user LIMIT 5;   -- email/full_name = ***REDACTED***, user_id intact
SELECT * FROM <cat>.<schema>.silver_user LIMIT 5;   -- one row per user_id
SELECT * FROM <cat>.<schema>.gold_user   LIMIT 5;   -- per-customer rollup
```

Note the current update status is **RUNNING** (continuous) or **COMPLETED**
(triggered) with **no errors**.

---

## Step 4 — Dry-run the erasure (notebook 02)

1. Open **`02_zero_downtime_erasure`**.
2. Set widgets:
   - `catalog` / `schema` — match steps 2–3
   - `subject_email` — the email you copied in step 2
   - `request_type` = `DELETE` (or `OBFUSCATE`)
   - `dry_run` = **`true`** (first pass)
   - `do_purge` = `true`
3. **Run all.**

✅ Checkpoint: sections 1–2 resolve the email to a `user_id` and print non-zero
match counts at every layer. Section 3 prints the exact SQL it *would* run. No
data is changed (`dry_run=true`).

---

## Step 5 — Run the live erasure

1. In notebook 02, set `dry_run` = **`false`**.
2. **Run all.**
3. Watch:
   - **Section 3** deletes/obfuscates gold → silver → bronze → raw.
   - **Section 4** physically purges (REORG + VACUUM) every layer.
   - **Section 5** re-counts all layers → **0** for DELETE, and asserts **no
     cleartext email survives** → prints `PASS`.

✅ Checkpoint: `PASS — subject erased with no cleartext trace.`

---

## Step 6 — Prove the pipeline never failed (the whole point)

1. Go back to the pipeline in the UI.
2. Confirm the latest/continuing update is **still healthy** — no
   `append-only source` / `FAILED fatally` error. This is the behavior that was
   impossible before `skipChangeCommits`.
3. **Liveness probe:** notebook 02 (section 6) appended a fresh raw event
   (`U999999`). Trigger the pipeline (or wait for the next micro-batch) and
   confirm it lands in gold:

```sql
SELECT * FROM <cat>.<schema>.gold_user WHERE user_id = 'U999999';
```

If the probe row appears, the stream is alive **after** the erasure — the
non-append commit was skipped, not fatal.

---

## Step 7 — (Optional) prove the "before" behavior

To demonstrate *why* the hardening matters, temporarily remove
`skipChangeCommits` from one streaming read in `01_sdp_pipeline`, redeploy, run an
erasure on that layer, and observe the fatal `append-only source` failure. Then
restore the option and **full-refresh the affected flow** to recover. (With the
option set from the start, you never need the full refresh.)

> ⚠️ Only do this in the demo schema — it intentionally breaks the pipeline.

---

## Step 8 — Idempotency: full refresh + repeated incrementals

DSAR erasure must survive any pipeline recompute. Because the erasure removes the
subject from the **base tables** (raw/bronze/silver), the pipeline is idempotent:

1. After the erasure (Step 5), run an **incremental** update — confirm counts are
   stable and the subject stays absent.
2. Run a **full refresh** (`--full-refresh`, or "Full refresh all" in the UI) —
   this resets every table and re-reads `raw_user` from scratch. The subject is
   gone from raw, so it cannot reappear; gold recomputes clean.
3. Run **another incremental** — still stable.

Verify with a per-layer count after each:

```sql
SELECT 'raw' l, count(*) c, sum(case when user_id='U000000' then 1 else 0 end) subj FROM <cat>.<schema>.raw_user
UNION ALL SELECT 'bronze', count(*), sum(case when user_id='U000000' then 1 else 0 end) FROM <cat>.<schema>.bronze_user
UNION ALL SELECT 'silver', count(*), sum(case when user_id='U000000' then 1 else 0 end) FROM <cat>.<schema>.silver_user
UNION ALL SELECT 'gold',   count(*), sum(case when user_id='U000000' then 1 else 0 end) FROM <cat>.<schema>.gold_user;
```

✅ Checkpoint: `subj = 0` on every layer, and total counts are identical across all
three updates. This was verified end-to-end on a live pipeline: incremental →
full refresh → incremental all converge to the same clean state, and the erased
`user_id` never returns.

> Note: `gold_user` is a materialized view, so **you cannot `DELETE` from it**
> (`EXPECT_TABLE_NOT_VIEW`). It is refreshed, not erased — an incremental update is
> enough for the base tables, and a **full refresh** guarantees gold also drops the
> subject. Notebook `02` section 5 triggers this refresh (set `PIPELINE_ID` to
> automate it).

---

## What this proves to Allegiant

| Question they asked | Answer this demo shows |
|---------------------|------------------------|
| Can we delete PII from all layers incl. bronze? | Yes — DELETE/OBFUSCATE at raw+bronze+silver+gold + VACUUM |
| Without breaking the append-only stream? | Yes — `skipChangeCommits` on every hop |
| Do we need it on both layers? | Yes — on the two streaming hops (raw→bronze, bronze→silver); gold is a materialized view, so no skipChangeCommits there |
| Does it add latency? | Negligible — skipping a commit just advances the offset |
| Do future erasures need a pipeline stop? | No — once `skipChangeCommits` is set, future erasures run live |

---

## Cleanup

```sql
DROP SCHEMA IF EXISTS <catalog>.<schema> CASCADE;
```

Then delete the pipeline from Jobs & Pipelines.
