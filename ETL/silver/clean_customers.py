"""Silver layer: clean new bronze customers and upsert one row per customer.

Incremental: streams only the bronze rows appended since the last run (bronze is
append-only), keeps the latest row per customer_id in each batch, and merges it in.
Rows whose values haven't changed are left untouched, which keeps the change
data feed that gold reads small.

Target table: SQL/ddl/V00202__silver_customers.sql
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
source = f"`{args.catalog}`.`{args.schema_prefix}bronze`.customers"
target = f"`{args.catalog}`.`{args.schema_prefix}silver`.customers"
checkpoint = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/_checkpoints/silver_customers"

KEY = "customer_id"
CLEANED = """
SELECT
  customerID                  AS customer_id,
  trim(first_name)            AS first_name,
  trim(last_name)             AS last_name,
  lower(trim(email_address))  AS email_address,
  trim(phone_number)          AS phone_number,
  trim(address)               AS address,
  trim(city)                  AS city,
  trim(state)                 AS state,
  trim(country)               AS country,
  trim(continent)             AS continent,
  trim(postal_zip_code)       AS postal_code,
  trim(gender)                AS gender,
  _ingested_at
FROM updates
WHERE customerID IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY customerID ORDER BY _ingested_at DESC) = 1
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
