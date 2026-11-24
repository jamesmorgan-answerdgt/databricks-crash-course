"""Bronze layer: incrementally load new customers files from the landing volume.

Auto Loader picks up only files it hasn't seen before (tracked in the
_checkpoints volume) and appends them, so bronze keeps every version of every
row it has received. The schema comes from the bronze table itself, so a column
added by a DDL migration is loaded with no change here. Anything in a file that
doesn't fit the schema lands in _rescued_data instead of being dropped.

Target table: SQL/ddl/V00102__bronze_customers.sql (+ V00106__bronze_customers_add_ingest_metadata.sql)
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
target = f"`{args.catalog}`.`{args.schema_prefix}bronze`.customers"
landing = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/customers"
checkpoint = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/_checkpoints/bronze_customers"

METADATA = {"_ingested_at", "_source_file", "_rescued_data"}
file_schema = StructType([f for f in spark.table(target).schema if f.name not in METADATA])

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(file_schema)
    .load(landing)
    .select(
        "*",
        current_timestamp().alias("_ingested_at"),
        col("_metadata.file_path").alias("_source_file"),
    )
    .writeStream.option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(target)
    .awaitTermination()
)
