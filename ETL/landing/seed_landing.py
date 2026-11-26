"""Landing: seed the landing volumes with the bakehouse samples (dev/test only).

In a real environment an upstream system drops each delivery, a complete extract,
into its own sortable folder, e.g. /Volumes/<catalog>/<prefix>landing/<source>/2026-11-26/,
and Auto Loader picks it up. Here, this task plays that system: the first time it
runs in an environment it writes each samples.bakehouse table into a folder named
after today's date, as CSV with a header row, using the bronze column names. After
that it does nothing, so bronze has nothing new to load until you drop another
delivery in.

Controlled by the seed_landing bundle variable; it's off in prod.
"""

import argparse
import datetime
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
BRONZE_METADATA = {"_ingested_at", "_source_file"}


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
            .write.option("header", "true")
            .csv(f"{landing}/{datetime.date.today().isoformat()}")
        )
