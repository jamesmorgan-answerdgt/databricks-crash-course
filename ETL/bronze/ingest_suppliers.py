"""Bronze layer: land samples.bakehouse.sales_suppliers as-is.

The target table is defined in SQL/ddl/V002__bronze_suppliers.sql; this script only
moves data. Full reload each run (the source is small).
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema-prefix", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
target = f"`{args.catalog}`.`{args.schema_prefix}bronze`.suppliers"

spark.sql(
    f"""
    INSERT OVERWRITE {target}
    SELECT
      CAST(supplierID AS BIGINT),
      name, ingredient, continent, city, district, size,
      CAST(longitude AS DOUBLE),
      CAST(latitude AS DOUBLE),
      CAST(approved AS STRING),
      current_timestamp()
    FROM samples.bakehouse.sales_suppliers
    """
)
print(f"Loaded {spark.table(target).count()} rows into {target}")
