"""Silver layer: type new bronze transactions and upsert one row per transaction.

Incremental: streams only the bronze rows appended since the last run (bronze is
append-only), keeps the latest row per transaction_id in each batch, and merges it in.
Rows whose values haven't changed are left untouched, which keeps the change
data feed that gold reads small.

The checkpoint follows bronze's schema fingerprint. When a bronze schema change
makes bronze reload everything, silver also starts a fresh checkpoint and re-reads
bronze in full; the merge skips rows that haven't changed, so gold only sees
real changes.

Deletes: after the upsert, silver is compared with the latest full delivery in
landing (hard delete; see the end of this file).

Card numbers are reduced to their last four digits here; the full number
never leaves bronze.

Target table: SQL/ddl/V00203__silver_transactions.sql
"""

import argparse
import hashlib
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
# Refuse to delete if the latest snapshot holds fewer keys than this share of silver.
parser.add_argument("--min-snapshot-ratio", type=float, default=0.9)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
source = f"`{args.catalog}`.`{args.schema_prefix}bronze`.transactions"
target = f"`{args.catalog}`.`{args.schema_prefix}silver`.transactions"

# Must match the fingerprint in ETL/bronze/ingest_transactions.py.
bronze_fingerprint = hashlib.sha256(
    ",".join(f"{f.name}:{f.dataType.simpleString()}" for f in spark.table(source).schema).encode()
).hexdigest()[:12]
checkpoint_root = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/_checkpoints/silver_transactions"
checkpoint = f"{checkpoint_root}/bronze_schema_{bronze_fingerprint}"
if not os.path.exists(checkpoint) and os.path.isdir(checkpoint_root):
    for old in os.listdir(checkpoint_root):
        shutil.rmtree(f"{checkpoint_root}/{old}")

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
    # Ignore bronze's TRUNCATE before a reload; the reloaded rows arrive as appends.
    .option("skipChangeCommits", "true")
    .table(source)
    .writeStream.foreachBatch(upsert)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)


# --- Deletes: compare silver with the latest full snapshot -----------------------
# Each delivery is a complete extract in its own sortable folder, for example
# landing/transactions/2026-11-26/*.csv. Once per new delivery, any transaction_id missing
# from the latest one is deleted from silver; gold picks the delete up from
# silver's change data feed. Only the snapshot's key column is read, from landing.
RAW_KEY = "transactionID"
landing = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/transactions"
quarantine_log = f"`{args.catalog}`.`{args.schema_prefix}landing`.quarantine_log"
reconciled_marker = f"{checkpoint}/last_reconciled_delivery"

deliveries = sorted(
    name
    for name in os.listdir(landing)
    if not name.startswith((".", "_")) and os.path.isdir(f"{landing}/{name}")
)
last_reconciled = None
if os.path.exists(reconciled_marker):
    with open(reconciled_marker) as fh:
        last_reconciled = fh.read().strip()

if deliveries and deliveries[-1] != last_reconciled:
    latest = deliveries[-1]
    # A quarantined file means the snapshot is incomplete: don't delete until it's
    # fixed and back in landing (the data quality checks flag it meanwhile).
    quarantined = spark.sql(
        f"""SELECT count(*) FROM {quarantine_log}
            WHERE source = 'transactions' AND resolved_at IS NULL AND source_file LIKE :folder""",
        args={"folder": f"%/transactions/{latest}/%"},
    ).first()[0]
    if not quarantined:
        snapshot = (
            spark.read.option("header", "true")
            .option("recursiveFileLookup", "true")
            .csv(f"{landing}/{latest}")
            .select(col(RAW_KEY).cast("bigint").alias(KEY))
            .where(col(KEY).isNotNull())
            .distinct()
        )
        snapshot_keys = snapshot.count()
        silver_keys = spark.table(target).count()
        if silver_keys and snapshot_keys < args.min_snapshot_ratio * silver_keys:
            raise RuntimeError(
                f"Delivery {latest} has {snapshot_keys} {KEY}s but silver has {silver_keys}. "
                "It looks partial, so nothing was deleted. If it's genuine, rerun this "
                "task with --min-snapshot-ratio=0."
            )
        snapshot.createOrReplaceTempView("snapshot_keys")
        spark.sql(
            f"""MERGE INTO {target} AS t
                USING snapshot_keys AS s
                ON t.{KEY} = s.{KEY}
                WHEN NOT MATCHED BY SOURCE THEN DELETE"""
        )
        with open(reconciled_marker, "w") as fh:
            fh.write(latest)
