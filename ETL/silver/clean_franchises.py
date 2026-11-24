"""Silver layer: clean new bronze franchises and upsert one row per franchise.

Incremental: streams only the bronze rows appended since the last run (bronze is
append-only), keeps the latest row per franchise_id in each batch, and merges it in.
Rows whose values haven't changed are left untouched, which keeps the change
data feed that gold reads small.

Target table: SQL/ddl/V00201__silver_franchises.sql
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
source = f"`{args.catalog}`.`{args.schema_prefix}bronze`.franchises"
target = f"`{args.catalog}`.`{args.schema_prefix}silver`.franchises"
checkpoint = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/_checkpoints/silver_franchises"

KEY = "franchise_id"
CLEANED = """
SELECT
  franchiseID      AS franchise_id,
  trim(name)       AS franchise_name,
  trim(city)       AS city,
  trim(district)   AS district,
  trim(zipcode)    AS zip_code,
  trim(country)    AS country,
  trim(size)       AS size,
  longitude,
  latitude,
  supplierID       AS supplier_id,
  _ingested_at
FROM updates
WHERE franchiseID IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY franchiseID ORDER BY _ingested_at DESC) = 1
"""
changed = " OR ".join(
    f"NOT (t.`{c}` <=> s.`{c}`)"
    for c in spark.table(target).columns
    if c not in (KEY, "_ingested_at")
)
MERGE = f"""
MERGE INTO {target} AS t
USING ({CLEANED}) AS s
ON t.{KEY} = s.{KEY}
WHEN MATCHED AND s._ingested_at >= t._ingested_at AND ({changed}) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


def upsert(batch, _batch_id):
    batch.createOrReplaceTempView("updates")
    batch.sparkSession.sql(MERGE)


(
    spark.readStream.option("schemaTrackingLocation", f"{checkpoint}/_schema")
    .table(source)
    .writeStream.foreachBatch(upsert)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
