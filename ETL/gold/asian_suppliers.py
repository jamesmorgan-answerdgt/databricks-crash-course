"""Gold layer (common data model): suppliers based in Asia (from exploration/practice.ipynb).

Incremental (SCD1): reads only what changed in silver.suppliers since the last run
(Delta change data feed), keeps the latest change per supplier_id, and applies it.
New rows are inserted, changed rows overwritten, deleted rows removed;
unchanged rows aren't touched.

A supplier that moves out of Asia is removed.

Target table: SQL/ddl/V00300__gold_asian_suppliers.sql
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
source = f"`{args.catalog}`.`{args.schema_prefix}silver`.suppliers"
target = f"`{args.catalog}`.`{args.schema_prefix}gold`.asian_suppliers"
checkpoint = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/_checkpoints/gold_asian_suppliers"

KEY = "supplier_id"
LATEST_CHANGES = """
SELECT
  supplier_id,
  supplier_name,
  continent,
  _change_type = 'delete' AS _is_delete
FROM changes
WHERE _change_type IN ('insert', 'update_postimage', 'delete')
QUALIFY row_number() OVER (PARTITION BY supplier_id ORDER BY _commit_version DESC) = 1
"""
columns = spark.table(target).columns
changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in columns if c != KEY)
assignments = ", ".join(f"t.`{c}` = s.`{c}`" for c in columns)
column_list = ", ".join(f"`{c}`" for c in columns)
values = ", ".join(f"s.`{c}`" for c in columns)
MERGE = f"""
MERGE INTO {target} AS t
USING ({LATEST_CHANGES}) AS s
ON t.{KEY} = s.{KEY}
WHEN MATCHED AND (s._is_delete OR NOT s.continent <=> 'Asia') THEN DELETE
WHEN MATCHED AND ({changed}) THEN UPDATE SET {assignments}
WHEN NOT MATCHED AND NOT s._is_delete AND s.continent = 'Asia' THEN INSERT ({column_list}) VALUES ({values})
"""


def apply_changes(batch, _batch_id):
    batch.createOrReplaceTempView("changes")
    batch.sparkSession.sql(MERGE)


(
    spark.readStream.option("readChangeFeed", "true")
    .option("schemaTrackingLocation", f"{checkpoint}/_schema")
    .table(source)
    .writeStream.foreachBatch(apply_changes)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
