# databricks-crash-course

A practice repo for Databricks study, set up so code is deployed and tested by
CI/CD using **Databricks Asset Bundles** and **GitHub Actions**.

## Layout

```
databricks.yml                  Bundle config + per-environment targets (local/dev/test/prod)
resources/
  suppliers_etl.job.yml         The job: apply_ddl -> bronze -> silver -> gold -> data_quality_checks
SQL/
  apply_ddl.py                  Applies versioned DDL to an environment (runs as the job's first task)
  ddl/V###__<name>.sql          Table definitions - the single source of truth for schemas
ETL/                            Medallion architecture
  bronze/   ingest_suppliers.py   Source data as landed, unmodified
  silver/   clean_suppliers.py    Cleaned, typed, de-duplicated
  gold/     asian_suppliers.py    Common data model (CDM) - business-facing tables
tests/
  unit/                         pytest, no Spark - runs on every PR
  integration/                  Data quality checks - run inside the job in each environment
exploration/                    Scratch notebooks (not deployed)
.github/workflows/              CI/CD
```

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
- `apply_ddl.py` runs first in every job run. It applies each `V###__*.sql` file
  once per environment and records it in `<prefix>ops.schema_migrations`.
- To change a table, **add a new file** (e.g. `V005__add_supplier_size.sql`
  containing `ALTER TABLE ... ADD COLUMN ...`). Editing an already-applied file
  fails the deploy, so every environment goes through the same history.
- `${catalog}`, `${bronze_schema}`, `${silver_schema}`, `${gold_schema}` and
  `${ops_schema}` are filled in per environment.

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
databricks bundle run suppliers_etl
```
