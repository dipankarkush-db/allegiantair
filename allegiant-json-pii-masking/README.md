# Masking PII inside nested JSON — Databricks

Mask only the PII **inside** a nested JSON column (Google-Analytics-style event blobs) while keeping the
**full JSON structure and every non-PII value** — revenue, transactions, behavioral metrics — intact.

This replaces masking the **whole row or column**, which throws away all the usable business data that
sits right next to the PII.

## The problem this solves (Allegiant Air)

Allegiant's PII lives *inside* a single JSON column — for example a `name` field, or a first/last name and
email embedded in a URL. Today the whole column (or row) is masked, so genuine business data is lost along
with the PII.

Databricks' native Auto Data Classification and column masks work at the **whole-column** level — they can
tag or mask an entire column, but they can't reach a single value *inside* a JSON blob. This solution fills
that gap: it masks the PII value **in place** and leaves the rest of the JSON untouched, while staying
entirely inside native Unity Catalog governance (**ABAC**, which Allegiant prefers over third-party tools).

## How it works

Three pieces, all native to Unity Catalog:

1. **Config says what is PII.** A single `pii_policies` map — `PII type → { keys, url_params, regex,
   strategy }` — defines what to mask. Retargeting is a config edit, not a code change.
2. **A native SQL mask function per PII type.** Each config entry becomes a `regexp_replace`-based SQL
   function (`mask_pii_<type>`) that rewrites only the matched PII inside the JSON string and runs in
   **Photon** — no row-by-row overhead. Patterns are quote-anchored, so `name` never matches `appName`.
3. **A tag-driven ABAC policy per PII type.** Tag any column `<tag_key>=<type>` and a schema-level ABAC
   policy automatically applies the matching mask function. It is **column-agnostic** — tag a new column
   (in this table or a future one) and it's masked with no extra setup.

### What it can target inside the JSON

| Config field | Masks | Example |
|---|---|---|
| `keys` | the value of a key at **any depth** | `name` → `appInfo.name` |
| `url_params` | a query-param value **embedded in a URL string** | `firstName` / `lastName` / `email` in a `manage-travel` URL |
| `regex` | matches of a pattern inside a string | `ssn=[0-9-]+` |
| `strategy` | how to mask | `REDACT` \| `NULLIFY` \| `PARTIAL` |

Example config (`config/pii_policies.json`):

```json
{
  "name":  {"strategy": "REDACT", "keys": ["name"],
            "url_params": ["firstName", "lastName", "first_name", "last_name", "email"], "regex": []},
  "email": {"strategy": "REDACT", "keys": ["email"], "url_params": [], "regex": []}
}
```

## Enforcement options

- **Dynamic masking via ABAC (recommended — Allegiant's preference).** The mask is applied at read time;
  raw bytes are unchanged. `TO account users [EXCEPT <group>]` lets a privileged group see raw — omit
  `EXCEPT` to mask everyone.
- **Permanent / materialized rewrite (for CCPA).** Rewrite the table once with the mask functions so the
  raw PII bytes are physically gone — "even we can't see it." This is also the fastest to read (zero
  per-query cost).

## Scaling

- **More columns or tables:** just tag them — the matching policy masks them, no policy change.
- **More PII types:** add an entry to `pii_policies`; a new mask function + ABAC policy is generated. The
  demo masks **two** columns (`hit_payload` → name/URL PII, `user_payload` → email PII).

## What's in this repo

| Path | Purpose |
|---|---|
| **`notebooks/pii_masking_json_demo.ipynb`** | **Start here.** Self-contained, idempotent notebook — saved **with the outputs from a full run**. Builds the demo table, mask functions, governed tag, and ABAC policies, then validates. |
| `notebooks/02_pii_path_discovery.py` | *Optional* AI-assisted discovery: `ai_query` classifies which JSON paths hold PII (confidence score → human review → tag → ABAC auto-masks). A discovery aid, not enforcement. |
| `sql/01_json_pii_masking.sql` | SQL-only reference of the same objects. |
| `config/pii_policies.json` | The single config file (`PII type → config`). The notebook loads its default from here. |
| `sample/` | Allegiant Air's GA sample and a URL-embedded example, each with its masked output. |

## Run it in Databricks

1. **Workspace → Create → Git folder**, paste `https://github.com/dipankarkush-db/allegiantair`, clone it.
2. Open `allegiant-json-pii-masking/notebooks/pii_masking_json_demo`.
3. Set the widgets: `catalog` / `schema` / `table`, `num_records`, `tag_key` (default `pii_aa`),
   `full_access_group` (blank = mask everyone), `pii_policies`, `tag_columns`.
4. **Run all.** The saved notebook already shows the outputs from a full run: raw vs. masked JSON side by
   side, and a validation that **100% of rows are masked** (name / URL / email REDACTed) while `appName`,
   `phone`, `loyalty`, and every other non-PII value are preserved.

Requires serverless SQL or a Photon-enabled cluster. Provisioning the governed tag needs the account-level
`CREATE` privilege (admin); if the tag already exists the notebook reuses it.

See [`usage.md`](usage.md) for the full step-by-step run guide.

> **Confirm the PII field list before production.** The default config masks the `name`/`email` keys and
> common URL name params. Confirm the authoritative list of PII keys/params for Allegiant's data — the
> config is built to be edited as the JSON evolves.
