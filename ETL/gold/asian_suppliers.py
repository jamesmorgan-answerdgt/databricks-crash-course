"""Gold layer (common data model): suppliers based in Asia (from exploration/practice.ipynb).

Target table: SQL/ddl/V004__gold_asian_suppliers.sql
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

spark.sql(
    f"""
    MERGE INTO {target} AS t
    USING (
      SELECT supplier_id, supplier_name
      FROM {source}
      WHERE continent = 'Asia'
    ) AS s
    ON t.supplier_id = s.supplier_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
)
print(f"{target} now has {spark.table(target).count()} rows")
