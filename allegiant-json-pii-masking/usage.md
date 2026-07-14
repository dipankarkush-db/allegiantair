# Usage — running the notebook

Step-by-step guide to run the JSON PII masking demo. See [`README.md`](README.md) for the solution
overview (the problem, how it works, enforcement options).

## Prerequisites

- A Unity Catalog-enabled workspace and permission to create objects in your target catalog/schema.
- **Serverless SQL** or a **Photon-enabled cluster**.
- Provisioning the governed tag needs the **account-level `CREATE` privilege** (admin). If the tag already
  exists the notebook reuses it; if you lack the privilege it prints the statement for an admin and
  continues.

The notebook is **self-contained** — it creates the catalog & schema (`IF NOT EXISTS`), the mask functions,
the demo table + data, the governed tag, and the ABAC policies. The only thing it relies on is the built-in
`account users` group.

## Steps

1. **Clone the repo as a Git folder.** In Databricks: **Workspace → Create → Git folder**, paste
   `https://github.com/dipankarkush-db/allegiantair`, and clone it under your Home.
2. **Open** `allegiant-json-pii-masking/notebooks/pii_masking_json_demo`.
3. **Attach** serverless or a Photon-enabled cluster.
4. **Set the widgets** (top of the notebook):

   | Widget | Meaning |
   |---|---|
   | `catalog` / `schema` / `table` | where the demo table is created |
   | `num_records` | rows to generate (default `10000`) |
   | `tag_key` | the governed tag key (default `pii_aa`) |
   | `full_access_group` | account group that sees **raw** PII; blank = mask everyone (CCPA posture) |
   | `pii_policies` | map of `PII type → config`; **one ABAC policy is created per entry** |
   | `tag_columns` | map of `column → PII type` — which columns to tag |

5. **Run all.** In order, the notebook:
   1. creates the catalog/schema;
   2. builds the native SQL mask functions from the config;
   3. creates + populates the demo table with **two** JSON columns — `hit_payload` (name in `appInfo.name`
      + first/last/email in a `documentLocation` URL) and `user_payload` (`contact.email`, with
      `phone`/`loyalty` kept as non-PII);
   4. previews the **raw** data (PII visible);
   5. shows each policy's **before/after** and the full JSON document original-vs-masked, so you can see
      only the PII fields change (structure preserved);
   6. **self-provisions the governed tag** and tags the columns per `tag_columns`;
   7. creates **one schema-level ABAC policy per PII type**;
   8. shows the **masked** view and validates.

6. **Confirm.** Expect e.g. `total=10000, name_masked=10000, url_masked=10000, email_masked=10000,
   phone_preserved=10000, appname_preserved=10000` — every tagged column masked per its policy, all non-PII
   intact.

> The saved notebook already contains these outputs, so you can review the results without re-running it.

## Common follow-ups

- **Give a group raw access:** set `full_access_group` to an **account-level** group and re-run — each
  policy is created `TO account users EXCEPT <group>`, so its members bypass the mask while everyone else
  stays masked. (Workspace-local groups are not visible to the account-scoped policy engine.)
- **Add a PII type:** add an entry to `pii_policies` (e.g.
  `"ssn": {"strategy":"REDACT","regex":["ssn=[0-9-]+"]}`) and re-run — a new mask function + policy
  `pii_json_mask_ssn` is created; tag any column with that type to mask it.
- **Retarget:** edit an entry in `pii_policies` and re-run — the mask function and policy are recreated.
- **Permanent removal (CCPA):** materialize a masked copy once with the mask functions so the raw bytes are
  gone (also the fastest to read).
