# DSAR erasure × Spark Declarative Pipelines (SDP)

Integrates per-subject DSAR/CCPA erasure with a **streaming** Spark Declarative
Pipeline so an erasure can delete PII at **every layer (bronze → silver → gold)**
**without stopping the pipeline and without a full refresh**.

This is a self-contained sub-solution of the [`dsar_erasure/`](../) layer. It
targets the exact failure Allegiant hit on the Merlot pipeline:

> `Flow ... has FAILED fatally... we detected an update or delete to one or more
> rows in the source table. Streaming tables may only use append-only streaming
> sources.`

## The problem in one paragraph

Allegiant's Merlot ingest is an SDP:
`autoloader → obfuscation view → bronze STREAMING TABLE → silver (AUTO CDC / SCD1) → gold`.
Bronze is an **append-only streaming source**. When an in-place DSAR erasure
`UPDATE`/`DELETE` ran directly on bronze, it created a **non-append commit**. The
downstream streaming flow had already consumed those files, so it could not
represent "a file I handed downstream just changed" — it **failed fatally and
would not auto-restart**. Every retry hit the same commit and died again.

## The fix

Set **`skipChangeCommits = "true"` on every streaming read** that consumes a
table an erasure will mutate:

```python
spark.readStream.option("skipChangeCommits", "true").table("...")
```

This tells the stream: *when you meet a commit containing an UPDATE/DELETE, skip
it instead of failing.* It does **not** undo the delete — the row is still gone
from the source. The stream just doesn't crash when it notices the source
changed. Skipping a commit only advances the offset (no data read, no files
scanned), so the latency cost is negligible.

**Which layers need it** (answering *"we'd need it on both layers, right?"*): yes —
on the **two append-only streaming hops**, `raw → bronze` and `bronze → silver`.
The erasure deletes at every layer, so each of those streaming reads meets a
non-append commit and needs the option.

The aggregating **gold** layer is deliberately **not** a `skipChangeCommits`
stream — it's a **materialized view** (batch recompute over bronze). A
`skipChangeCommits` stream over the SCD1 silver would silently drop *legitimate*
customer updates (SCD1 mutates on every change, not just erasures), and a
streaming aggregation keeps checkpoint state that could **resurrect an erased
subject** on restart. An MV fully recomputes from current bronze, so it reflects
every erasure automatically and holds no state to resurrect. So the rule is:
**`skipChangeCommits` on the two streams; materialized view for gold.**

## Architecture

```
raw_user ──stream(skipChangeCommits)──▶ bronze_user ──stream(skipChangeCommits)──▶ silver_user
  (landing)      obfuscate PII inline        (append-only)      AUTO CDC / SCD1         (dimension)

bronze_user ──batch recompute (materialized view)──▶ gold_user
                                                     (per-customer lifetime rollup)
```

- **`raw_user`** — the landing table (what autoloader writes). Holds cleartext
  PII **and** a stable non-PII business key `user_id`.
- **`bronze_user`** — streaming table that reads raw and applies native-SQL PII
  masking inline (scalar + in-JSON), preserving `user_id`, `revenue`, `event_ts`.
- **`silver_user`** — SCD type 1 dimension via **AUTO CDC**, keyed on `user_id`
  (the flow that was failing in the incident).
- **`gold_user`** — **materialized view**: per-customer lifetime rollup
  (lifetime revenue over all bronze events, event count), recomputed each refresh.
  You cannot `DELETE` from it — erasure removes the subject from the base tables and
  the MV recomputes clean on refresh.

### Idempotency

Erasure removes the subject from the **base tables**, so the pipeline is idempotent
under any mix of incremental and full-refresh updates: the erased subject never
reappears, and repeated updates converge to the same state (subject absent, everyone
else intact). A full refresh re-reads `raw_user` from scratch — and the subject is
already gone from raw, so it stays gone. Verified live (see `usage.md` Step 8).

### Why `user_id` is the match key

DSAR intake gives an **email**, but bronze/silver/gold have **redacted the
email**, so you can't match on it downstream. We resolve email → `user_id` once
from the raw layer, then erase by `user_id` everywhere. `user_id` is a non-PII
business key, so it survives obfuscation. (This mirrors the base layer's
"email-first" rule: email is the authoritative intake key; here it resolves to
the stable join key.)

## Files

| File | What it is | How to run |
|------|-----------|-----------|
| `00_setup_and_landing.ipynb` | Builds the isolated demo schema + `raw_user` landing table + surfaces a demo subject | **Batch** — run once, top to bottom |
| `01_sdp_pipeline.ipynb` | The pipeline definition (bronze/silver/gold, hardened) | **Attach as a Declarative Pipeline source** — do NOT run interactively |
| `02_zero_downtime_erasure.ipynb` | Erase one subject across all layers + physical purge, while the pipeline streams | **Batch** — run per DSAR request |
| `usage.md` | Step-by-step test runbook | — |

## Two erasure modes

- **DELETE** — remove the subject's rows entirely (right-to-delete).
- **OBFUSCATE** — keep the row, redact PII cells (opt-out / do-not-sell). Only
  `raw_user` still holds cleartext PII, so obfuscate redacts there; downstream
  layers are already redacted.

## Relationship to the base `dsar_erasure/` layer

This reuses the same building blocks — email-first matching, native `regexp_replace`
in-JSON masking, DELETE/OBFUSCATE modes, and the serverless VACUUM technique
(table property `delta.deletedFileRetentionDuration = 'interval 0 hours'` + plain
`VACUUM`). What's new is the **streaming integration**: the pipeline is hardened
with `skipChangeCommits` so erasure runs live. The base layer (`00`–`05`) remains
the engine for non-streaming / cross-catalog erasure; this folder is the pattern
when the tables are SDP streaming targets.

## Requirements

- **Serverless Lakeflow Declarative Pipelines** (or Pro/Advanced edition) — AUTO
  CDC requires it.
- Unity Catalog. All names are widget/config-driven; nothing is hard-coded to
  Allegiant.

See `usage.md` for the full test runbook.
