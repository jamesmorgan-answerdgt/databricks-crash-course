"""Bronze layer: incrementally load new suppliers CSV files from the landing volume.

Auto Loader picks up only files it hasn't seen before (tracked in the
_checkpoints volume). Each file is loaded whole or not at all:

  * Clean files are appended to bronze, so bronze keeps every version of every
    row it has received.
  * A file with any row that doesn't fit the bronze schema (extra, renamed or
    wrongly typed columns, malformed lines) is not loaded. It is moved unchanged
    to the quarantine volume, and the reason is recorded in
    <prefix>landing.quarantine_log.

The schema comes from the bronze table itself, so a column added by a DDL
migration is loaded with no change here. CSV headers are matched to it by name.

Full reload on schema change: the checkpoint is keyed to a fingerprint of the
bronze columns and types. When a DDL migration changes them, this run finds no
checkpoint for the new fingerprint, so it empties bronze and reloads every file
in landing under the new schema (landing keeps every file). Otherwise it only
loads new files.

Target table: SQL/ddl/V00100__bronze_suppliers.sql
Quarantine:   SQL/ddl/V00001__landing_quarantine.sql
"""

import argparse
import hashlib
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, current_timestamp, slice
from pyspark.sql.types import StructType

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

SOURCE = "suppliers"
spark = SparkSession.builder.getOrCreate()
target = f"`{args.catalog}`.`{args.schema_prefix}bronze`.{SOURCE}"
quarantine_log = f"`{args.catalog}`.`{args.schema_prefix}landing`.quarantine_log"
landing_root = f"/Volumes/{args.catalog}/{args.schema_prefix}landing"
landing = f"{landing_root}/{SOURCE}"
quarantine = f"{landing_root}/quarantine/{SOURCE}"

# Auto Loader puts anything that doesn't fit the schema in this column. It is
# only used to spot bad files and never written to bronze.
BAD_DATA = "_bad_data"
METADATA = {"_ingested_at", "_source_file"}
bronze_schema = spark.table(target).schema
file_schema = StructType([f for f in bronze_schema if f.name not in METADATA])

# Column names and types only, so changing a comment doesn't trigger a reload.
fingerprint = hashlib.sha256(
    ",".join(f"{f.name}:{f.dataType.simpleString()}" for f in bronze_schema).encode()
).hexdigest()[:12]
checkpoint_root = f"{landing_root}/_checkpoints/bronze_{SOURCE}"
checkpoint = f"{checkpoint_root}/schema_{fingerprint}"

if not os.path.exists(checkpoint):
    # First run, or the bronze schema changed: start again from every landing file.
    spark.sql(f"TRUNCATE TABLE {target}")
    if os.path.isdir(checkpoint_root):
        for old in os.listdir(checkpoint_root):
            shutil.rmtree(f"{checkpoint_root}/{old}")

REASON = (
    "Rows don't match the bronze schema: extra, renamed or wrongly typed columns, "
    "or malformed CSV lines (see sample_bad_data)"
)
LOG_SCHEMA = (
    "source STRING, source_file STRING, quarantine_file STRING, reason STRING, "
    "bad_row_count BIGINT, total_row_count BIGINT, sample_bad_data ARRAY<STRING>"
)


def load_batch(batch, batch_id):
    session = batch.sparkSession

    # 1. Which files in this batch have rows that don't fit the schema?
    bad_files = (
        batch.groupBy("_source_file")
        .agg(
            count("*").alias("total_row_count"),
            count(BAD_DATA).alias("bad_row_count"),
            slice(collect_list(BAD_DATA), 1, 5).alias("sample_bad_data"),
        )
        .where("bad_row_count > 0")
        .collect()
    )
    bad_paths = [r["_source_file"] for r in bad_files]

    # 2. Append only the clean files. txnAppId/txnVersion make a retried batch a
    #    no-op. The fingerprint is part of the ID because a new checkpoint
    #    restarts batch numbers at 0, which Delta would otherwise skip as done.
    (
        batch.where(~col("_source_file").isin(bad_paths))
        .drop(BAD_DATA)
        .write.mode("append")
        .option("txnAppId", f"bronze_{SOURCE}_{fingerprint}")
        .option("txnVersion", batch_id)
        .saveAsTable(target)
    )
    if not bad_files:
        return

    # 3. Record why each file was rejected (also idempotent), then move it out of
    #    landing unchanged, keeping its name and relative path.
    moves, log_rows = [], []
    for r in bad_files:
        src = r["_source_file"].removeprefix("dbfs:")
        dest = f"{quarantine}/schema_{fingerprint}_batch_{batch_id}/{os.path.relpath(src, landing)}"
        moves.append((src, dest))
        log_rows.append(
            (SOURCE, src, dest, REASON, r["bad_row_count"], r["total_row_count"], r["sample_bad_data"])
        )
    (
        session.createDataFrame(log_rows, LOG_SCHEMA)
        .withColumn("quarantined_at", current_timestamp())
        .write.mode("append")
        .option("txnAppId", f"quarantine_{SOURCE}_{fingerprint}")
        .option("txnVersion", batch_id)
        .saveAsTable(quarantine_log)
    )
    for src, dest in moves:
        if os.path.exists(src):  # already moved if this batch is a retry
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.move(src, dest)


(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", BAD_DATA)
    # A retried batch may find its bad files already moved to quarantine.
    .option("ignoreMissingFiles", "true")
    .schema(file_schema)
    .load(landing)
    .select(
        "*",
        current_timestamp().alias("_ingested_at"),
        col("_metadata.file_path").alias("_source_file"),
    )
    .writeStream.foreachBatch(load_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
