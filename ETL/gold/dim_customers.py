"""Gold layer (common data model): customer dimension.

Incremental (SCD1): reads only what changed in silver.customers since the last run
(Delta change data feed), keeps the latest change per customer_id, and applies it.
New rows are inserted, changed rows overwritten, deleted rows removed;
unchanged rows aren't touched.

Contact details (email, phone, street address) are deliberately left in silver.

Target table: SQL/ddl/V00302__gold_dim_customers.sql
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
source = f"`{args.catalog}`.`{args.schema_prefix}silver`.customers"
target = f"`{args.catalog}`.`{args.schema_prefix}gold`.dim_customers"
checkpoint = f"/Volumes/{args.catalog}/{args.schema_prefix}landing/_checkpoints/gold_dim_customers"

KEY = "customer_id"
LATEST_CHANGES = """
SELECT
  customer_id, first_name, last_name, city, state,
  country, continent, postal_code, gender,
  _change_type = 'delete' AS _is_delete
FROM changes
WHERE _change_type IN ('insert', 'update_postimage', 'delete')
QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY _commit_version DESC) = 1
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
WHEN MATCHED AND s._is_delete THEN DELETE
WHEN MATCHED AND ({changed}) THEN UPDATE SET {assignments}
WHEN NOT MATCHED AND NOT s._is_delete THEN INSERT ({column_list}) VALUES ({values})
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
