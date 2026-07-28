# Incremental batch 1 — sample landing files

Pre-made JSON files for the **Part B (incremental) cycle** of the runbook. They
contain **8 brand-new customers** (`U900001`–`U900008`, 24 events total) whose
`user_id`s are deliberately far above the initial load's range (`U000000`–`U001999`),
so "new" is unmistakable.

## How to use

Upload both `.json` files to the pipeline's Auto Loader landing zone, then run the
pipeline **incrementally** (no full refresh). Auto Loader picks up only these new
files.

**Option A — Catalog Explorer UI:** open the volume
`/Volumes/<catalog>/<schema>/raw_user`, navigate to `landing/incremental/`
(create the folder if needed), **Upload** both files.

**Option B — notebook `00b_incremental_landing`:** run it — it copies these committed
files into `landing/incremental/` for you and enqueues the 2nd DSAR wave.

**Option C — CLI:**
```bash
databricks fs cp incremental_batch_1_part0.json \
  dbfs:/Volumes/<catalog>/<schema>/raw_user/landing/incremental/ --profile <profile>
databricks fs cp incremental_batch_1_part1.json \
  dbfs:/Volumes/<catalog>/<schema>/raw_user/landing/incremental/ --profile <profile>
```

## Schema (one JSON object per line)

`event_id, user_id, email, full_name, profile_json, revenue, event_ts, _ingest_ts`
— identical to the initial load, so the same pipeline ingests them unchanged.
