# Test runbook — DSAR erasure × Lakeflow Declarative Pipelines (Auto Loader)

Ingest raw JSON files from a **Unity Catalog volume** with **Auto Loader** into a
streaming medallion pipeline (raw → bronze → silver → gold), then process a DSAR
erasure queue that deletes/masks every named subject **at every layer AND in the
source files** — with zero pipeline downtime, and in a way that **survives a full
refresh**.

Modern API: `from pyspark import pipelines as dp`. Serverless UC workspace assumed.

Two cycles, run in order:

- **Part A — Initial cycle:** generate landing files → pipeline initial load → first
  erasure wave.
- **Part B — Incremental cycle:** drop new files → pipeline **incremental** run →
  second erasure wave (mix of brand-new and older subjects).

> **Why the source files matter (CCPA).** Auto Loader ingests *files*. Erasing the
> tables alone leaves cleartext PII in those files, and a **full refresh re-reads
> them** — resurrecting the subject. So `02` scrubs the volume files too. This is
> verified below (Step A6).

---

## The two pipeline variants (choose one, or run both)

`01` and `01b` are **independent variants of the same pipeline** — run separately,
each in its own schema.

| | `01_sdp_pipeline` | `01b_sdp_pipeline_cdc_variant` |
|---|---|---|
| **Silver** | **Append event log** — every event kept (1 row/event). **Not SCD.** | **SCD type 1** — one row per `user_id`, latest wins (AUTO CDC). |
| **Mirrors** | Simplest medallion | Allegiant's real Merlot `dbo_user_silver_cdc` |
| **Schema** | `allegiant_air_sdp_dsar` | `allegiant_air_sdp_dsar_cdc` |
| **Needed?** | Yes — main demo | Optional — the SCD1 case |

> Neither is SCD type 2. **Part A and Part B below are the exact same steps for both
> variants** — the *only* differences are (1) which `01`/`01b` notebook you attach to the
> pipeline and (2) the schema you point everything at. Nothing else changes.

### How to follow the runbook for each variant

Pick your variant, use its settings **consistently** in every notebook widget and in the
pipeline config, then follow Part A → Part B verbatim:

| Setting (used in `00`, `00b`, `02` widgets **and** pipeline config) | Variant A — append silver | Variant B — SCD1 (CDC) |
|---|---|---|
| Pipeline source notebook (Step A2) | `01_sdp_pipeline` | `01b_sdp_pipeline_cdc_variant` |
| `schema` / `dsar.schema` | `allegiant_air_sdp_dsar` | `allegiant_air_sdp_dsar_cdc` |
| `catalog` / `dsar.catalog` | your catalog | your catalog (same) |
| `volume` / `dsar.volume` | `raw_user` | `raw_user` |
| `silver_user` row count after A2 | ~10,000 (1 row/event) | ~2,000 (1 row/user, latest) |

- **To run just one variant:** set the schema above in every notebook + the pipeline
  config, and go through Part A then Part B once.
- **To demo both:** run the whole runbook **twice** — once per row of the table, each in
  its **own schema**. The two are fully independent (separate volumes-subtrees are not
  even needed since each schema has its own volume); they never interact. Do them
  sequentially (finish A, then B) to keep it simple.

> The only step-level difference you'll observe is the **silver row count** (append keeps
> every event; SCD1 keeps one row per customer) — the erasure, file-scrub, gold-refresh,
> and full-refresh behavior are identical. Each per-step "Expected outcome" below notes
> the SCD1 number where it differs.

---

## Volume & landing layout

```
/Volumes/<catalog>/<schema>/raw_user/               ← UC volume (landing zone)
    landing/
        initial/      *.json    ← written by 00        (Part A)
        incremental/  *.json    ← written by 00b / uploaded manually (Part B)
```

The pipeline's Auto Loader reads the whole `landing/` folder recursively, so files in
either subfolder are ingested.

---

## Step 1 — Get the notebooks into your Databricks workspace

```
sdp_integration/
  00_setup_and_landing.ipynb            # initial: schema + volume + landing files + 1st DSAR wave
  00b_incremental_landing.ipynb         # incremental: new files + 2nd DSAR wave (Part B)
  01_sdp_pipeline.ipynb                 # append-silver variant (Auto Loader)
  01b_sdp_pipeline_cdc_variant.ipynb    # SCD1-silver variant (optional)
  02_erasure.ipynb                      # erasure driver (tables + volume files)
  03_validate.ipynb                     # click-to-run validation (widget-driven, one %sql check per cell)
  sample_data/incremental_batch_1/      # pre-made incremental files (for Part B option)
  README.md
  usage.md
```

- **Git folder (recommended).** Workspace → **Create → Git folder**, URL
  `https://github.com/dipankarkush-db/allegiantair`, branch `main`.
- **Manual import.** Import the `.ipynb` files.

**Expected outcome:** all six notebooks visible in your workspace. `00`, `00b`, `02`,
`03` are run interactively (Run all); `01`/`01b` are attached to a pipeline (Step A2).

> **Verifying results:** every `✅ Expected outcome` below can be checked at a click by
> running **`03_validate`** (set the same `catalog`/`schema`/`volume` widgets, Run all)
> — it renders each check as its own `%sql` result table (landing files, medallion
> counts, masking, DSAR queue, no-cleartext-trace). Or paste the SQL into the **SQL
> Editor**. Don't add scratch notebooks *inside* the Git folder — it dirties the repo;
> keep ad-hoc work in your home folder.

---

# Part A — Initial cycle

## Step A1 — Build the schema, volume & initial landing files (notebook 00)

**What this step does:** creates the isolated schema, the landing volume, writes the
initial batch of raw JSON files into `landing/initial/`, and seeds the 1st DSAR queue.

1. Open **`00_setup_and_landing`**. Set `catalog`/`schema` (defaults `dkushari_uc` /
   `allegiant_air_sdp_dsar`; CDC variant → `allegiant_air_sdp_dsar_cdc`), `volume`
   (`raw_user`), `num_users`=2000, `events_per_user`=5, `num_files`=8,
   `num_requests`=10 (DSAR requests to seed — matches the base layer's ~10/month).
2. **Run all.**

**✅ Expected outcome:**
- The volume exists: `/Volumes/<cat>/<schema>/raw_user/landing/initial/` contains
  **8 JSON part-files** (~10,000 records).
- `dsar_request` has **`num_requests` PENDING** rows (default **10**), alternating
  DELETE / OBFUSCATE (REQ-001 DELETE, REQ-002 OBFUSCATE, …).
- Sanity check:
  ```sql
  SELECT count(*) FROM read_files('/Volumes/<cat>/<schema>/raw_user/landing/initial', format=>'json');  -- 10000
  SELECT * FROM <cat>.<schema>.dsar_request;                                                             -- 10 PENDING (default)
  ```

## Step A2 — Create & build the pipeline (notebook 01 or 01b)

**What this step does:** stands up the Auto Loader medallion pipeline and runs the
**initial load**, ingesting the files from Step A1.

> `01`/`01b` are **pipeline source code** — do NOT "Run all" interactively (the
> `@dp.table` decorators only execute inside a pipeline).

**A2.1 — Open the editor.** Jobs & Pipelines → **Create → ETL pipeline**.

**A2.2 — Point it at the EXISTING notebook, not the sample.** The editor offers a
starter project with sample `transformations/*.py`. Instead, in **Pipeline settings →
Code assets** set the **Source code** to this repo's notebook (use **Configure
paths**):
- **Source code:** `01_sdp_pipeline` (or `01b_sdp_pipeline_cdc_variant`), i.e.
  `/Workspace/Users/<you>/allegiantair/allegiant-json-pii-masking/dsar_erasure/sdp_integration/01_sdp_pipeline`
- **Root folder:** the `sdp_integration` folder is fine (only the source file matters).

**A2.3 — Configure the settings.** Open **Pipeline settings** (gear / *Settings*). The
current Lakeflow UI groups these as follows:

- **Compute:** **Serverless**.
- **Pipeline mode:** Triggered.
- **Default location for data assets** → **Edit catalog and schema**:
  - **Default catalog** = your catalog (e.g. `dkushari_uc`)
  - **Default schema** = the variant's schema (`allegiant_air_sdp_dsar`, or
    `allegiant_air_sdp_dsar_cdc` for the CDC variant)
- **Parameters** *(Beta)* → **Edit parameters** — this is where the notebook's
  `dsar.*` keys go (the notebook's `cfg("dsar.catalog", …)` etc. read these). Add:

  | Parameter | Value |
  |---|---|
  | `dsar.catalog` | your catalog (e.g. `dkushari_uc`) |
  | `dsar.schema` | the variant's schema |
  | `dsar.volume` | `raw_user` |

> **UI note:** in the current editor these live under **Parameters** (a *Beta*
> section in **Pipeline settings**), not under a separate "Advanced → Configuration"
> panel. If your workspace shows a **Configuration** section (or you use the **JSON**/
> **YAML** settings tabs), put the same three `dsar.*` key/value pairs there — the
> pipeline reads them the same way. The **JSON** tab is the quickest way to paste all
> three at once.

**A2.4 — Start.** The first run is the initial/full load.

**✅ Expected outcome:**
- Graph shows **4 green nodes**: `raw_user → bronze_user → silver_user → gold_user`
  (`raw_user` is the Auto Loader ingest node).
- Row counts and content (**cleartext flows through — this DSAR demo does not mask at
  ingest**; every layer holds real PII until an erasure targets a subject):
  ```sql
  SELECT count(*) FROM <cat>.<schema>.raw_user;      -- 10000 (ingested from files)
  SELECT count(*) FROM <cat>.<schema>.silver_user;   -- 10000 (append variant) / 2000 (SCD1)
  SELECT count(*) FROM <cat>.<schema>.gold_user;     -- 2000 per-customer rollups
  SELECT user_id, email, full_name FROM <cat>.<schema>.silver_user LIMIT 3;   -- real email/full_name (cleartext) + user_id
  ```

**If the graph fails:**
- `dp` import / "Run all" error → you ran it interactively; attach to a pipeline.
- `path does not exist` / empty `raw_user` → `dsar.*` config doesn't match where `00`
  wrote the volume, or `00` wasn't run. Fix config, re-run `00`.

## Step A3 — First erasure, dry-run (notebook 02)

**What this step does:** previews exactly what would be erased — no changes.

1. Open **`02_erasure`**. Set `catalog`/`schema`/`volume` to match; `dry_run` =
   **`true`**; `do_purge` = `true`; `scrub_files` = `true`; `pipeline_id` = your
   pipeline id.
2. **Run all.**

**✅ Expected outcome:** sections 1–3 list all PENDING requests (10 by default), resolve each email
→ `user_id`, and pre-count matches at every layer; section 4 prints the SQL it *would*
run; section 5b lists the landing files it *would* scrub. **Nothing is modified.**

## Step A4 — First erasure, live (notebook 02)

**What this step does:** erases all wave-1 subjects (10 by default) across all layers **and** the source
files, then refreshes gold.

1. Set `dry_run` = **`false`**. **Run all.**

**✅ Expected outcome (per section):**
- **§4** — DELETE removes rows / OBFUSCATE redacts, across `silver → bronze → raw`.
- **§5** — VACUUM on the modified tables (zero-retention).
- **§5b** — landing files rewritten in place: DELETE subjects' records dropped,
  OBFUSCATE subjects' records redacted (`console shows "rewritten: N file(s)"`).
- **§6** — gold MV refresh triggered (async).
- **§7** — prints `PASS — DELETE subjects erased with no trace; OBFUSCATE subjects
  redacted in place.`, verifies **no cleartext survives in the volume files**, and
  flips all wave-1 requests to **COMPLETE**.

Confirm:
```sql
-- DELETE subjects gone from tables AND files
SELECT count(*) FROM <cat>.<schema>.raw_user WHERE user_id IN ('<deleted ids>');   -- 0
SELECT count(*) FROM read_files('/Volumes/<cat>/<schema>/raw_user/landing', format=>'json', recursiveFileLookup=>'true')
  WHERE user_id IN ('<deleted ids>');                                              -- 0
```

> **Gold timing:** §6's refresh is async, so §7 may print gold's stale row with a
> `<- refresh gold MV` note. Wait for the pipeline update from §6 to reach
> `COMPLETED`, then re-query `gold_user` — the DELETE subject's row is gone.

## Step A5 — Prove the pipeline never failed

Open the pipeline — the update after the erasure is **healthy**, no `append-only
source` / `FAILED fatally` error.

**✅ Expected outcome:** latest update `COMPLETED`; no error events. (This was
impossible before `skipChangeCommits`.)

## Step A6 — Prove erasure survives a FULL REFRESH (the CCPA acid test)

**What this step does:** re-reads every landing file from scratch and confirms the
deleted subjects do **not** come back — because the files were scrubbed.

1. Full-refresh the pipeline: UI **Start ▾ → Full refresh all**, or
   `databricks pipelines start-update <id> --full-refresh`. Wait for `COMPLETED`.

**✅ Expected outcome:**
```sql
SELECT count(*) FROM <cat>.<schema>.raw_user WHERE user_id IN ('<deleted ids>');  -- 0 (stayed gone!)
SELECT count(*) FROM <cat>.<schema>.raw_user;                                     -- 9990 (10000 - 2 DELETE subjects x5)
```
The OBFUSCATE subject's rows are still present but ingest as `***REDACTED***` (the
file was redacted). *This is the whole point of scrubbing the source files.*

---

# Part B — Incremental cycle

New files keep arriving, the pipeline processes them **incrementally**, and new
erasure requests (some new subjects, some old) are handled live.

## Step B1 — Land new files + enqueue the 2nd wave (notebook 00b)

**What this step does:** drops a new batch of files into `landing/incremental/` and
enqueues 4 new DSAR requests mixing new & old subjects.

1. Open **`00b_incremental_landing`**. Set `catalog`/`schema`/`volume` = the same as
   Part A. **Run all.**
   *(Alternatively, upload the committed `sample_data/incremental_batch_1/*.json` to
   `landing/incremental/` by hand — see that folder's README — and only run 00b's
   wave-2 cell.)*

**✅ Expected outcome:**
- `landing/incremental/` gains a file with **24 records** (8 new customers
  `U900001`–`U900008`).
- `dsar_request` gains **4 new PENDING** rows: 2 target NEW subjects
  (`U9000xx`), 2 target OLD pre-existing subjects. Verify:
  ```sql
  SELECT count(*) FROM read_files('/Volumes/<cat>/<schema>/raw_user/landing/incremental', format=>'json');  -- 24
  SELECT request_id, subject_email, request_type, status FROM <cat>.<schema>.dsar_request ORDER BY request_id;
  ```

## Step B2 — Run the pipeline INCREMENTALLY

**What this step does:** ingests only the new files (no full refresh).

- UI: open the pipeline → **Start** (normal triggered run), or
  `databricks pipelines start-update <id>` (no `--full-refresh`).

**✅ Expected outcome:** only the new records are ingested; graph stays green.
```sql
SELECT count(*) FROM <cat>.<schema>.raw_user;                          -- grew by 24
SELECT count(*) FROM <cat>.<schema>.raw_user  WHERE user_id >= 'U900000';  -- 24
SELECT count(*) FROM <cat>.<schema>.gold_user WHERE user_id >= 'U900000';  -- 8 new customers
```

> **Incremental vs full refresh:** incremental (normal Start) processes only new
> commits — the steady-state path. Full refresh (Start ▾ → *Full refresh all*) resets
> and re-reads all files; because erased subjects were scrubbed from the files, a full
> refresh never resurrects them (Step A6).

## Step B3 — Second erasure wave (notebook 02, again)

**What this step does:** same driver, now processing the 2nd wave (new + old
subjects).

1. Run **`02_erasure`** exactly as A3/A4 (dry-run then live) — nothing to change.

**✅ Expected outcome:** both new and old subjects erased across every layer (DELETE
removes rows, OBFUSCATE redacts), the volume files scrubbed, gold refreshed, and
`REQ-004…007` flipped to **COMPLETE** — **while the pipeline keeps running**.

> **Repeat the loop:** re-run `00b` (new files + new wave) → incremental pipeline run
> → `02`, as many times as you like. `00b` continues the `U9000xx` / `REQ-xxx`
> numbering.

---

## Idempotency check (any point after an erasure)

For **DELETE** subjects, run **incremental → full refresh → incremental** and check
counts each time (DELETE user_ids only — OBFUSCATE keeps its rows):

```sql
SELECT 'raw' l,count(*) c, sum(case when user_id IN ('<deleted ids>') then 1 else 0 end) subj FROM <cat>.<schema>.raw_user
UNION ALL SELECT 'silver',count(*), sum(...) FROM <cat>.<schema>.silver_user
UNION ALL SELECT 'gold',  count(*), sum(...) FROM <cat>.<schema>.gold_user;
```

**✅ Expected outcome:** `subj = 0` on every layer, stable across all updates.

> OBFUSCATE keeps its rows (PII redacted in place) — verify by content, not count:
> `SELECT email FROM <cat>.<schema>.raw_user WHERE user_id='<obf id>';  -- ***REDACTED***`

---

## What this proves to Allegiant

| Question | Answer |
|----------|--------|
| Ingest via Auto Loader from files? | Yes — `raw_user` is a `cloudFiles` streaming table over a UC volume |
| Delete PII from all layers incl. bronze? | Yes — DELETE/OBFUSCATE at raw+bronze+silver + VACUUM, gold MV refreshed |
| Delete PII from the **source files**? | Yes — `02` rewrites the landing files in place (§5b) |
| Survives a full refresh? | Yes — files are scrubbed, so re-ingest can't resurrect a subject (Step A6) |
| Without breaking the append-only stream? | Yes — `skipChangeCommits` on the streaming hops |
| Works on continuously-arriving (incremental) data? | Yes — Part B: new files stream in, erasures for new + old subjects run live |
| Future erasures need a pipeline stop? | No — erasures run live |

---

## Cleanup

```sql
DROP SCHEMA IF EXISTS <catalog>.<schema> CASCADE;   -- drops tables; volume too unless referenced elsewhere
```
Then delete the pipeline(s) from Jobs & Pipelines. (To keep the volume, drop only the
tables and clear `landing/`.)
