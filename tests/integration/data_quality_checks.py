"""Post-load checks, run as the last task of demo_etl_job in every environment.

Any failed check raises, which fails the job run, which fails the CI deploy.
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()


def table(layer: str, name: str) -> str:
    return f"`{args.catalog}`.`{args.schema_prefix}{layer}`.{name}"


def scalar(sql: str):
    return spark.sql(sql).first()[0]


def is_not_empty(tbl: str) -> bool:
    return scalar(f"SELECT count(*) FROM {tbl}") > 0


def is_unique(tbl: str, key: str) -> bool:
    return scalar(f"SELECT count(*) - count(DISTINCT {key}) FROM {tbl}") == 0


def has_no_orphans(fact: str, key: str, dim: str) -> bool:
    return scalar(
        f"""SELECT count(*) FROM {fact} f
            LEFT ANTI JOIN {dim} d ON f.{key} = d.{key}
            WHERE f.{key} IS NOT NULL"""
    ) == 0


# (source, primary key in silver/gold)
SOURCES = {
    "suppliers": "supplier_id",
    "franchises": "franchise_id",
    "customers": "customer_id",
    "transactions": "transaction_id",
}

# gold table -> primary key
GOLD = {
    "dim_franchises": "franchise_id",
    "dim_customers": "customer_id",
    "fact_transactions": "transaction_id",
}

checks = {}
for source, key in SOURCES.items():
    checks[f"bronze.{source} is not empty"] = is_not_empty(table("bronze", source))
    # Rescued data means the landing files no longer match the bronze schema
    # (new, renamed or retyped source columns): add a DDL migration for it.
    checks[f"bronze.{source} has no rescued data"] = scalar(
        f"SELECT count(*) FROM {table('bronze', source)} WHERE _rescued_data IS NOT NULL"
    ) == 0
    checks[f"silver.{source} is not empty"] = is_not_empty(table("silver", source))
    checks[f"silver.{source}.{key} is unique"] = is_unique(table("silver", source), key)

for name, key in GOLD.items():
    checks[f"gold.{name}.{key} is unique"] = is_unique(table("gold", name), key)

silver_suppliers = table("silver", "suppliers")
fact = table("gold", "fact_transactions")

checks["gold.fact_transactions has every silver transaction"] = scalar(
    f"SELECT count(*) FROM {fact}"
) == scalar(f"SELECT count(*) FROM {table('silver', 'transactions')}")

checks["gold.fact_transactions.customer_id all in dim_customers"] = has_no_orphans(
    fact, "customer_id", table("gold", "dim_customers")
)
checks["gold.fact_transactions.franchise_id all in dim_franchises"] = has_no_orphans(
    fact, "franchise_id", table("gold", "dim_franchises")
)

failed = [name for name, passed in checks.items() if not passed]
if failed:
    raise AssertionError(f"Data quality checks failed: {failed}")
