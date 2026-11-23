"""Silver layer: clean bronze suppliers and upsert one row per supplier.

Target table: SQL/ddl/V003__silver_suppliers.sql
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
source = f"`{args.catalog}`.`{args.schema_prefix}bronze`.suppliers"
target = f"`{args.catalog}`.`{args.schema_prefix}silver`.suppliers"

spark.sql(
    f"""
    MERGE INTO {target} AS t
    USING (
      SELECT
        supplierID         AS supplier_id,
        trim(name)         AS supplier_name,
        trim(ingredient)   AS ingredient,
        trim(continent)    AS continent,
        trim(city)         AS city,
        trim(district)     AS district,
        longitude,
        latitude,
        _ingested_at
      FROM {source}
      WHERE supplierID IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY supplierID ORDER BY _ingested_at DESC) = 1
    ) AS s
    ON t.supplier_id = s.supplier_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
)
print(f"{target} now has {spark.table(target).count()} rows")
