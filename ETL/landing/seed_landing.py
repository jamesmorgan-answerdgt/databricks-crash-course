"""Landing: seed the landing volumes with the bakehouse samples (dev/test only).

In a real environment an upstream system drops files into
/Volumes/<catalog>/<prefix>landing/<source>/ and Auto Loader picks them up.
Here, this task plays that system: the first time it runs in an environment it
writes each samples.bakehouse table into its volume as Parquet, typed to match
the bronze table. After that it does nothing, so bronze has nothing new to load
until you drop another file in.

Controlled by the seed_landing bundle variable; it's off in prod.
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
parser.add_argument("--enabled", required=True)
args = parser.parse_args()

# landing volume -> source table
SOURCES = {
    "suppliers": "samples.bakehouse.sales_suppliers",
    "franchises": "samples.bakehouse.sales_franchises",
    "customers": "samples.bakehouse.sales_customers",
    "transactions": "samples.bakehouse.sales_transactions",
}
BRONZE_METADATA = {"_ingested_at", "_source_file", "_rescued_data"}


def has_files(path: str) -> bool:
    return any(not name.startswith((".", "_")) for name in os.listdir(path))


if args.enabled.lower() == "true":
    spark = SparkSession.builder.getOrCreate()
    for source, source_table in SOURCES.items():
        landing = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/{source}"
        if has_files(landing):
            continue
        bronze = f"`{args.catalog}`.`{args.schema_prefix}bronze`.{source}"
        fields = [f for f in spark.table(bronze).schema if f.name not in BRONZE_METADATA]
        (
            spark.table(source_table)
            .select([col(f.name).cast(f.dataType).alias(f.name) for f in fields])
            .write.parquet(f"{landing}/seed")
        )
