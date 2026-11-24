"""Silver layer: type new bronze transactions and upsert one row per transaction.

Incremental: streams only the bronze rows appended since the last run (bronze is
append-only), keeps the latest row per transaction_id in each batch, and merges it in.
Rows whose values haven't changed are left untouched, which keeps the change
data feed that gold reads small.

Card numbers are reduced to their last four digits here; the full number
never leaves bronze.

Target table: SQL/ddl/V00203__silver_transactions.sql
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
source = f"`{args.catalog}`.`{args.schema_prefix}bronze`.transactions"
target = f"`{args.catalog}`.`{args.schema_prefix}silver`.transactions"
checkpoint = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/_checkpoints/silver_transactions"

KEY = "transaction_id"
CLEANED = """
SELECT
  transactionID                       AS transaction_id,
  customerID                          AS customer_id,
  franchiseID                         AS franchise_id,
  dateTime                            AS transaction_ts,
  trim(product)                       AS product,
  CAST(quantity AS INT)               AS quantity,
  CAST(unitPrice AS DECIMAL(18, 2))   AS unit_price,
  CAST(totalPrice AS DECIMAL(18, 2))  AS total_price,
  trim(paymentMethod)                 AS payment_method,
  right(trim(cardNumber), 4)          AS card_last4,
  _ingested_at
FROM updates
WHERE transactionID IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY transactionID ORDER BY _ingested_at DESC) = 1
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
