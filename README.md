# databricks-crash-course

A practice repo for Databricks study, set up so code is deployed and tested by
CI/CD using **Databricks Asset Bundles** and **GitHub Actions**.

## Layout

```
databricks.yml                  Bundle config + per-environment targets (local/dev/test/prod)
resources/
  demo_etl_job.job.yml          The job: apply_ddl -> seed_landing -> bronze -> silver -> gold -> data_quality_checks
SQL/
  apply_ddl.py                  Applies versioned DDL to an environment (runs as the job's first task)
  ddl/V#####__<name>.sql        Table definitions - the single source of truth for schemas
ETL/                            Medallion architecture, one script per source per layer
  landing/  seed_landing.py       Dev/test only: drops the samples into the landing volumes
  bronze/   ingest_<source>.py    Auto Loader: new landing files, appended unmodified
  silver/   clean_<source>.py     Cleaned, typed, de-duplicated
  gold/     <model>.py            Common data model (CDM) - business-facing tables
tests/
  unit/                         pytest, no Spark - runs on every PR
  integration/                  Data quality checks - run inside the job in each environment
exploration/                    Scratch notebooks (not deployed)
.github/workflows/              CI/CD
```

## Sources and tables

All sources run in parallel through one job, `demo_etl_job`. Each delivery is a
**complete extract** of its source, landed as CSV in its own sortable folder, e.g.
`/Volumes/<catalog>/<prefix>landing/customers/2026-11-26/*.csv`. In dev and test,
`seed_landing` copies the `samples.bakehouse` tables into a folder named after the
current date the first time the job runs.

| Source       | Bronze                | Silver                 | Gold (CDM)                                 |
|--------------|-----------------------|------------------------|--------------------------------------------|
| suppliers    | `bronze.suppliers`    | `silver.suppliers`     | `gold.asian_suppliers`                     |
| franchises   | `bronze.franchises`   | `silver.franchises`    | `gold.dim_franchises`                      |
| customers    | `bronze.customers`    | `silver.customers`     | `gold.dim_customers` (no contact details)  |
| transactions | `bronze.transactions` | `silver.transactions`  | `gold.fact_transactions` (no card details) |

`fact_transactions` waits for both dimensions and has informational foreign keys to them.
Full card numbers stay in bronze; silver keeps only the last four digits.

To add a source: a landing volume in `V000`, a bronze/silver/gold DDL file each
in `SQL/ddl` (enable the change data feed on the silver table), a script per layer
in `ETL/`, tasks in `resources/demo_etl_job.job.yml`, and an entry in
`tests/integration/data_quality_checks.py` (plus `seed_landing.py` for dev/test data).

## Incremental loading

Each run processes only what's new since the last run:

| Layer  | Reads                                               | Writes                                                        |
|--------|-----------------------------------------------------|---------------------------------------------------------------|
| Bronze | New CSV files in the landing volume (Auto Loader)   | Appends clean files, with `_ingested_at` and `_source_file`; quarantines bad ones |
| Silver | Bronze rows appended since the last run (stream)    | Upserts the latest row per key (unchanged rows untouched), then deletes keys missing from the latest delivery |
| Gold   | Silver's change data feed since the last run        | Applies inserts, updates and deletes (SCD1: latest values only) |

- **Progress is tracked in checkpoints** under the `<prefix>landing._checkpoints` volume,
  one folder per task (e.g. `bronze_customers`, `silver_customers`, `gold_dim_customers`).
  Bronze and silver checkpoints sit in a sub-folder named after the bronze schema's
  fingerprint (`schema_<hash>`, `bronze_schema_<hash>`).
- **To load new data**, drop a new delivery folder with sortable name (e.g.
  `2026-11-27`) into `/Volumes/<catalog>/<prefix>landing/<source>/`, holding CSV files
  with a header row and the bronze column names (e.g. `customerID`, `first_name`).
  The next run picks it up.
- **Updates** come through as a newer row for the same key. **Deletes** are worked
  out by comparing with the latest delivery (see below).

### Deletes

After its upsert, each silver task compares silver with the **latest delivery
folder** (the greatest folder name) once per new delivery:

- Any key in silver that isn't in that delivery is **hard-deleted** from silver.
  Gold applies the same delete from silver's change data feed.
- Only the delivery's key column is read, straight from landing, so this stays
  cheap however much history bronze holds. Bronze keeps every row, so a deleted
  key is still in its history.
- The last delivery checked is recorded in the silver checkpoint
  (`last_reconciled_delivery`), so a delivery is only compared once.

Safety guards:
- **Quarantined file in the latest delivery:** no deletes until it's fixed and back
  in landing, because the snapshot is incomplete.
- **Snapshot under 90% of silver's keys:** the silver task fails instead of deleting,
  in case the delivery is partial or still being written. If a big drop is genuine,
  rerun that task with `--min-snapshot-ratio=0` (or set it in the job parameters).

Things to know:
- Files directly in `landing/<source>/`, not in a delivery folder, are loaded but
  ignored when working out deletes.
- Deleting a customer or franchise whose transactions are still in the latest
  transactions delivery fails the fact table's orphan checks, because gold deletes
  are hard deletes.
- A key that comes back in a later delivery is inserted again.
- **To reprocess a layer from scratch**, empty its table and delete its checkpoint
  folder, plus the checkpoints of every layer downstream. For bronze, just delete
  `_checkpoints/bronze_<source>`: the next run empties bronze and reloads all of landing.

### Full reload when the bronze schema changes

Landing keeps every file, so bronze can always be rebuilt. Each bronze run works out
a fingerprint of the bronze table's column names and types (comments don't count):

- **Same fingerprint as last run:** only new files are loaded, as normal.
- **New fingerprint** (a DDL migration added, renamed, retyped or dropped a column,
  or replaced the table): bronze is emptied and every file in landing is reloaded
  under the new schema. Historic rows get the new column filled in.
- **Silver** follows the same fingerprint, so it re-reads the reloaded bronze from a
  fresh checkpoint. Its merge skips rows that haven't changed, so gold only sees
  real changes.

Things to know:
- A reload re-reads all of that source's landing files, so it costs a full load.
- `_ingested_at` becomes the reload time; use `_source_file` for where a row came from.
- A file that loaded under the old schema can be quarantined under the new one.
- During a reload, silver briefly re-inserts deleted keys from bronze's history, then
  its delete step removes them again in the same run. Gold applies only the latest
  change per key, so they never reach gold.

### Quarantine

A landing file is loaded whole or not at all. If any row doesn't fit the bronze
schema (an extra, renamed or wrongly typed column, or a malformed CSV line), bronze:

1. loads none of that file's rows (other files in the same run still load);
2. records why in `<prefix>landing.quarantine_log`: the source, the file's old and
   new paths, a reason, bad and total row counts, and up to 5 examples of the bad data;
3. moves the file, unchanged and still CSV, to
   `/Volumes/<catalog>/<prefix>landing/quarantine/<source>/batch_<id>/<original name>`.

While a file is unresolved, the data quality checks fail. To reprocess it:

1. See why it was rejected:
   `SELECT * FROM <prefix>landing.quarantine_log WHERE resolved_at IS NULL`.
2. Either fix the file, or add a DDL migration if the new column is wanted.
3. Put the file back in `landing/<source>/` under a **new name**. Auto Loader
   remembers paths it has seen and won't load the same name again.
4. Mark it resolved:
   `UPDATE <prefix>landing.quarantine_log SET resolved_at = current_timestamp() WHERE source_file = '...'`.

Limits: a column that's **missing** from a file's header isn't caught (it loads as
`NULL`), and a file that isn't readable CSV at all fails the bronze task rather than
being quarantined.

## Branching and environments

| Event                    | What happens                                                                 |
|--------------------------|------------------------------------------------------------------------------|
| PR into `dev` or `main`  | Unit tests (`ci.yml`)                                                         |
| Merge to `dev`           | Unit tests -> deploy to **dev** -> run job incl. data quality checks          |
| Merge to `main`          | Unit tests -> deploy + run in **test** -> approval -> deploy to **prod**      |

Workflow: feature branch -> PR into `dev` -> PR `dev` into `main`.

Each environment gets its own schemas per layer (`dev_bronze`, `dev_silver`, `dev_gold`, `prod_gold`, ...).
For separate catalogs or workspaces per environment, set `catalog` /
`workspace.host` per target in `databricks.yml` and `schema_prefix: ""`.

## Where table definitions live

In **`SQL/ddl/`**, as versioned, environment-agnostic DDL, not in the ETL code.

- The ETL scripts only move data (`INSERT` / `MERGE`); they never create tables.
- `apply_ddl.py` runs first in every job run. It applies each `V#####__*.sql` file
  once per environment and records it in `<prefix>ops.schema_migrations`.
- Files are grouped by layer: `V000`–`V00099` prerequisites, `V001xx` bronze, `V002xx`
  silver, `V003xx` gold.
- Tables are created with `CREATE OR REPLACE TABLE`, which **drops existing data**.
  That's safe because each file runs only once per environment. Schemas and
  volumes keep `IF NOT EXISTS`, since Databricks has no `OR REPLACE` for them.
  Bronze is safe to replace too, because it reloads from landing when its schema
  changes (see "Full reload when the bronze schema changes").
- To change a table, **add a new file** (e.g. `V00204__silver_suppliers_add_size.sql`
  containing `ALTER TABLE ... ADD COLUMN ...`). Editing an already-applied file
  fails the deploy, so every environment goes through the same history.
- `${catalog}`, `${bronze_schema}`, `${silver_schema}`, `${gold_schema}`,
  `${landing_schema}` and `${ops_schema}` are filled in per environment.

### Changing a schema safely

1. **One non-repeatable statement per file.** `ADD COLUMN`, `RENAME`, `DROP` (without
   `IF EXISTS`), `ADD CONSTRAINT` and data changes fail if a half-applied file is
   retried, so the runner rejects files that mix them with other statements.
2. **Change the ETL in the same PR.** Bronze reads its schema from the bronze table,
   so a new bronze column needs no code change. Silver and gold scripts list their
   columns in a `SELECT`, so a new silver/gold column is only filled once that
   `SELECT` produces it.
3. **Renames and drops are allowed.** `V00400__enable_column_mapping.sql` turns on
   Delta column mapping, so `ALTER TABLE ... RENAME COLUMN` / `DROP COLUMN` work
   without rewriting data. New tables must set
   `TBLPROPERTIES ('delta.columnMapping.mode' = 'name')` (a unit test enforces it).
   A rename or drop on an existing table must be numbered **after** `V00400`
   (e.g. `V00401__silver_customers_rename_gender.sql`); a unit test checks this too.
4. **Bronze changes reload automatically.** Change bronze with `ALTER` or
   `CREATE OR REPLACE` as you like; the next run rebuilds it from landing. If you
   replace a **silver or gold** table, delete its checkpoint folder and those
   downstream of it.
5. **Files with new source columns are quarantined.** The data quality checks then
   fail, which is your prompt to add a migration and reprocess the file.
6. **Streams track schema changes.** Silver and gold streams keep a
   `schemaTrackingLocation` in their checkpoint. Silver also restarts from scratch
   whenever bronze's schema changes, so it never has to cross one.

Why not in the ETL code? Schemas are reviewed separately from logic, constraints,
clustering and comments are explicit, a typo in a dataframe can't silently
change a production schema, and the same definitions apply identically to
dev, test and prod.

The exception: if you move to **Lakeflow Declarative Pipelines** (DLT), the
pipeline owns its tables and the schema is declared alongside the pipeline
code. That's a different model, so don't mix it with this one for the same tables.

## One-off setup

1. Create a `dev` branch from `main` and push it.
2. In GitHub **Settings > Environments**, create `dev`, `test` and `prod`, each
   with secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. Add required
   reviewers to `prod` for the approval gate.
3. (Recommended) Protect `dev` and `main` so changes arrive by PR with `ci` passing.

## Working locally

```powershell
pip install -r requirements-dev.txt
pytest
databricks bundle deploy             # "local" target, deployed under your user with local_* schemas
databricks bundle run demo_etl_job
```
