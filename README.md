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

All sources run in parallel through one job, `demo_etl_job`. Files arrive in the
landing volumes (`<prefix>landing.<source>`); in dev and test, `seed_landing` copies
the `samples.bakehouse` tables there the first time the job runs.

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
| Bronze | New files in the landing volume (Auto Loader)       | Appends them, with `_ingested_at`, `_source_file`, `_rescued_data` |
| Silver | Bronze rows appended since the last run (stream)    | Upserts the latest row per key; unchanged rows untouched      |
| Gold   | Silver's change data feed since the last run        | Applies inserts, updates and deletes (SCD1: latest values only) |

- **Progress is tracked in checkpoints** under the `<prefix>landing._checkpoints` volume,
  one folder per task (e.g. `bronze_customers`, `silver_customers`, `gold_dim_customers`).
- **To load new data**, drop Parquet files into `/Volumes/<catalog>/<prefix>landing/<source>/`
  with the bronze column names (e.g. `customerID`, `first_name`). The next run picks them up.
- **Updates** come through as a newer row for the same key. **Deletes** in the source
  aren't detected: a missing row is just a row with no new file. Deleting from silver
  directly does flow through to gold.
- **To reprocess a layer from scratch**, empty its table and delete its checkpoint
  folder, plus the checkpoints of every layer downstream.
- **Unexpected columns** in landing files go to `_rescued_data`, and the data quality
  checks fail until you add a DDL migration for them.

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
- Files are grouped by layer: `V000` prerequisites, `V001xx` bronze, `V002xx`
  silver, `V003xx` gold.
- Tables are created with `CREATE OR REPLACE TABLE`, which **drops existing data**.
  That's safe because each file runs only once per environment. Schemas and
  volumes keep `IF NOT EXISTS`, since Databricks has no `OR REPLACE` for them.
  Because bronze is now loaded incrementally, any new bronze table should use
  `IF NOT EXISTS` (see point 4 below).
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
4. **Don't `CREATE OR REPLACE` a table that has data you can't reload.** Bronze is
   append-only history, so use `IF NOT EXISTS` + `ALTER` for bronze tables. If you do
   replace any table, delete its checkpoint folder and those downstream of it.
5. **New source columns land in `_rescued_data`.** The data quality checks then fail,
   which is your prompt to add a migration for them.
6. **Streams track schema changes.** Silver and gold streams keep a
   `schemaTrackingLocation` in their checkpoint, so they can carry on after a column
   is renamed or dropped upstream.

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
