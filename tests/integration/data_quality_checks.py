"""Post-load checks, run as the last task of the job in every environment.

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


bronze = table("bronze", "suppliers")
silver = table("silver", "suppliers")
gold = table("gold", "asian_suppliers")

checks = {
    "bronze is not empty": scalar(f"SELECT count(*) FROM {bronze}") > 0,
    "silver is not empty": scalar(f"SELECT count(*) FROM {silver}") > 0,
    "silver supplier_id is unique": scalar(
        f"SELECT count(*) - count(DISTINCT supplier_id) FROM {silver}"
    ) == 0,
    "gold supplier_id is unique": scalar(
        f"SELECT count(*) - count(DISTINCT supplier_id) FROM {gold}"
    ) == 0,
    "gold matches silver Asian suppliers": scalar(f"SELECT count(*) FROM {gold}")
    == scalar(f"SELECT count(*) FROM {silver} WHERE continent = 'Asia'"),
}

for name, passed in checks.items():
    print(f"[{'PASS' if passed else 'FAIL'}] {name}")

failed = [name for name, passed in checks.items() if not passed]
if failed:
    raise AssertionError(f"Data quality checks failed: {failed}")
