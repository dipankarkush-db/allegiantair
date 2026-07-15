# Performance framework — masking overhead at scale

Measures the **read-path cost of masking** on the in-JSON PII solution at **1M / 10M / 100M** rows,
so Allegiant can see how it behaves at production volume. Everything here is **isolated** — it builds its
own schema (`dkushari_uc.allegiant_air_perf`) and never touches the demo table or objects.

## What it measures

For each scale it stands up **three side-by-side tables** from the same generated data, and runs the same
query patterns against each so the difference **is** the masking overhead:

| Variant | Table | What the query sees | Purpose |
|---|---|---|---|
| **baseline** | `events_<N>_baseline` | raw data (untagged) | the no-mask control |
| **dynamic** | `events_<N>_dynamic` | masked **at read time** by the schema **ABAC** policy | the recommended enforcement path |
| **materialized** | `events_<N>_materialized` | physically **pre-masked** copy | zero read-time mask cost (CCPA path) |

`events_<N>_dynamic` is a **shallow clone** of the baseline (no data duplication) whose columns are tagged
`pii_aa=name` / `pii_aa=email`, so the schema-level ABAC policy masks it automatically. `events_<N>_materialized`
is a one-time masked rewrite.

**Overhead = (variant median − baseline median) / baseline median**, per query pattern and scale.

## Query patterns

Realistic patterns for GA-style event blobs — designed to expose *when* masking costs and when it doesn't:

| Pattern | What it does | Why |
|---|---|---|
| `full_scan_mask` | full scan aggregating both payload columns | heaviest signal — masks every row of both PII columns |
| `point_lookup` | fetch payloads for a single `hit_id` | typical row retrieval |
| `filter_nonpii` | filter on a **non-PII** field extracted from the payload | shows mask cost even when the result isn't PII |
| `groupby_nonpii` | GROUP BY a non-PII field (loyalty tier) | typical BI aggregation over a masked column |
| `extract_pii` | extract a field that **is** masked | shows masking actually working (`***MASKED***`) |
| `no_masked_col` | touches only `hit_id` (no masked column) | proves **dynamic ABAC adds ~0 overhead when the masked column isn't read** |

## Example results & findings

Best-of-5 samples on a serverless SQL warehouse (`execution_duration_ms`, result cache off). Your numbers
will differ with warehouse size — treat these as directional.

**At 100M rows:**

| Query pattern | Baseline | Dynamic (ABAC) | Materialized |
|---|--:|--:|--:|
| `no_masked_col` (masked column not read) | 12 ms | **12 ms (~0%)** | 12 ms |
| `point_lookup` (one row) | 66 ms | **66 ms (~0%)** | 76 ms |
| `extract_pii` (read a masked field, 1k rows) | 60 ms | 81 ms (+35%) | 60 ms |
| `groupby_nonpii` (GROUP BY over masked col) | 436 ms | 1,085 ms (+149%) | 463 ms |
| `filter_nonpii` (filter on field in masked col) | 602 ms | 3,637 ms (+504%) | 735 ms |
| `full_scan_mask` (scan **both** PII columns) | 416 ms | **4,024 ms (+867%)** | 390 ms |

**What it shows:**
1. **Dynamic ABAC masking is effectively free when a query doesn't read a masked column** — `no_masked_col`
   and `point_lookup` show ~0% overhead at every scale. UC only applies the mask to columns actually projected.
2. **When a query scans/reads the masked JSON columns, dynamic masking adds real latency** — the
   `regexp_replace` runs on every row. A full scan of both PII columns was ~10× baseline at 100M.
3. **Materializing a pre-masked copy removes the read-time cost** — `materialized` ≈ baseline (and it's the
   CCPA permanent-removal path).
4. **Overhead is per-row CPU**, so wall-clock scales with *rows masked* and *inversely with warehouse
   parallelism* — a bigger/faster warehouse cuts it. (This is why a small 1M table, with few files and low
   parallelism, can show *higher* wall-clock than 10M; read each scale on its own, not as a cross-scale trend.)

**Takeaway for Allegiant:** for BI/analytics that repeatedly scan the masked columns at volume, prefer the
**materialized** path; use **dynamic ABAC** where some users need raw access and masked scans are occasional
or don't touch the masked column. Either way, tag the columns and the ABAC policy does the rest.

**Live dashboard:** `Allegiant PII Masking — Performance & Overhead` (in this folder in the workspace) —
or import `perf_dashboard.lvdash.json`.

## How to run

1. **`01_setup_and_generate.sql`** — Run All. Builds the perf schema, native SQL mask functions, ABAC
   policies, and the 3 tables × 3 scales. (100M generation is the longest step.)
2. **`02_run_benchmarks.sql`** — Run All **4–5 times**. Each run is one timed sample of every
   pattern × scale × variant. `use_cached_result = false` guarantees real execution each time, and the
   **warm-up cell (§0)** scales the serverless warehouse first so the first samples aren't cold-start outliers.
3. **`03_collect_results.sql`** — Run All. Creates `perf_results` and pulls each query's **server-side
   execution time** from `system.query.history` (short ingestion delay — re-run if rows are missing). Ends
   with a summary of median time and overhead % vs baseline.
4. **Dashboard** — import `perf_dashboard.lvdash.json` (AI/BI) for the visual side-by-side: latency by
   scale × variant × pattern and the masking-overhead %.

Runs on any SQL warehouse (built and validated on the demo's serverless warehouse). Timing comes from
`system.query.history` — no stopwatch code, just the query engine's own measurements.

## Notes

- Reuses the existing governed tag **`pii_aa`** (values `name`, `email`) and the same native
  `regexp_replace` mask functions as the main solution — so the numbers reflect the real masking logic.
- Re-runnable and isolated: `01` uses `CREATE OR REPLACE`, `03` de-dupes on `statement_id`.
- To reset everything: `DROP SCHEMA dkushari_uc.allegiant_air_perf CASCADE;` (leaves the demo untouched).
