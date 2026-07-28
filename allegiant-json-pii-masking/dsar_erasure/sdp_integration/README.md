# DSAR erasure × Lakeflow Declarative Pipelines (SDP)

Integrates per-subject DSAR/CCPA erasure with a **streaming** Lakeflow Declarative
Pipeline so an erasure deletes/masks a subject's records at **every layer
(bronze → silver → gold)** **without stopping the pipeline and without a full
refresh**.

Self-contained sub-solution of the [`dsar_erasure/`](../) layer. Uses the **modern**
Lakeflow Declarative Pipelines Python API: `from pyspark import pipelines as dp`
(`@dp.table`, `@dp.materialized_view`, `dp.create_auto_cdc_flow`). *(Not the legacy
`import dlt` module.)*

## The problem it solves

Allegiant's Merlot ingest is an SDP: `autoloader → obfuscation view → bronze
STREAMING TABLE → silver (AUTO CDC/SCD1) → gold`. An in-place DSAR erasure
`UPDATE`/`DELETE` on the append-only bronze streaming source is a **non-append
commit** → the downstream flow fails fatally:

> *Streaming tables may only use append-only streaming sources... we detected an
> update or delete to one or more rows in the source table.*

## The fix

`.option("skipChangeCommits", "true")` on **every streaming read** that consumes a
table an erasure mutates. The stream **skips** the erasure's non-append commit
instead of failing — the row is still gone from the source; the stream just
doesn't crash. Skipping a commit only advances the offset (no data read) →
negligible latency.

## Clean linear medallion

```
raw_user ──stream──▶ bronze_user ──stream──▶ silver_user ──batch MV──▶ gold_user
           mask PII            clean/validate           per-customer aggregate
          (skipChangeCommits)  (skipChangeCommits)      (recomputed each refresh)
```

- **`raw_user`** — landing table (cleartext PII + stable non-PII `user_id`).
- **`bronze_user`** — streaming; masks PII inline (scalar + in-JSON), preserves
  `user_id`/`revenue`.
- **`silver_user`** — streaming; cleaned/validated events (or SCD1 dimension in the
  CDC variant). Reads **bronze**.
- **`gold_user`** — **materialized view**; per-customer rollup. Reads **silver**
  (linear). Batch recompute → reflects erasures on refresh, no checkpoint state to
  resurrect a subject. You cannot `DELETE` from a view, so erasure deletes the base
  tables and refreshes this MV.

`skipChangeCommits` is on the two streaming hops (raw→bronze, bronze→silver). Gold
is an MV, so it needs no skipChangeCommits — it just recomputes.

### Why `user_id` is the match key

Intake gives an **email**, but bronze/silver mask it — so we resolve email →
`user_id` once from raw, then erase by `user_id` everywhere. `user_id` is non-PII
and survives masking.

## Files

| File | What | How to run |
|------|------|-----------|
| `00_setup_and_landing.ipynb` | Isolated schema + `raw_user` + multi-subject `dsar_request` queue | **Batch** — run once |
| `01_sdp_pipeline.ipynb` | Pipeline: bronze → silver (clean) → gold MV | **Attach as a Lakeflow pipeline source** |
| `01b_sdp_pipeline_cdc_variant.ipynb` | Alt pipeline: silver as **SCD1 via AUTO CDC** (matches real Merlot) | Attach as a *separate* pipeline (own schema) |
| `02_erasure.ipynb` | Process the DSAR queue: erase **every** subject at **every** layer + purge + refresh gold | **Batch** — run per DSAR batch |
| `usage.md` | Step-by-step runbook (incremental & full refresh) | — |

## Two variants of silver

- **Clean silver (`01`)** — `silver_user` is a plain streaming table (validated
  events). Simple linear graph; directly `DELETE`-able.
- **SCD1 silver (`01b`)** — `silver_user` is a type-1 dimension via
  `dp.create_auto_cdc_flow` keyed on `user_id`. Faithfully matches Allegiant's real
  `dbo_user_silver_cdc` flow. Deploy as a separate pipeline with its own target
  schema (e.g. `allegiant_air_sdp_dsar_cdc`).

## Two erasure modes (per `dsar_request` row)

- **DELETE** — remove the subject's rows entirely.
- **OBFUSCATE** — keep rows, redact PII cells (only `raw_user` still holds cleartext
  PII; downstream already masked).

`02_erasure` processes **all PENDING requests** in one run — every named subject,
every layer — exactly like the base `dsar_erasure/` engine.

## Idempotency

Erasure removes subjects from the **base tables**, so the pipeline is idempotent
under any mix of incremental and full-refresh updates: a full refresh re-reads
`raw_user` (subject already gone), so erased subjects never reappear.

## Status

**Verified end-to-end on a live pipeline** (2026-07-27, `e2-demo-field-eng`,
pipeline `allegiant_dsar_sdp_demo`, schema `dkushari_uc.allegiant_air_sdp_dsar`):
modern-API pipeline full-refresh COMPLETED (linear medallion, masking + aggregate
correct), multi-subject `02_erasure` erased all 3 PENDING requests (2× DELETE +
1× OBFUSCATE) across raw/bronze/silver, refreshed gold, marked all COMPLETE.

## Requirements

Serverless Lakeflow Declarative Pipelines (AUTO CDC needs it). Unity Catalog. All
names widget/config-driven.

See `usage.md` for the full runbook.
