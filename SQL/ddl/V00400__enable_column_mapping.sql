-- Turn on Delta column mapping for every table so columns can later be
-- renamed (ALTER TABLE ... RENAME COLUMN) or dropped (ALTER TABLE ... DROP COLUMN)
-- without rewriting the table. This upgrades each table's Delta protocol
-- (reader v2 / writer v5), which current Databricks runtimes all support.
--
-- Every statement here is safe to re-run, so a partly applied run can retry.
-- Any table created after this file must set the property itself, e.g.
--   CREATE OR REPLACE TABLE ... TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
-- (enforced by tests/unit/test_apply_ddl.py).

ALTER TABLE `${catalog}`.`${bronze_schema}`.suppliers
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE `${catalog}`.`${bronze_schema}`.franchises
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE `${catalog}`.`${bronze_schema}`.customers
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE `${catalog}`.`${bronze_schema}`.transactions
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE `${catalog}`.`${silver_schema}`.suppliers
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE `${catalog}`.`${silver_schema}`.franchises
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE `${catalog}`.`${silver_schema}`.customers
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE `${catalog}`.`${silver_schema}`.transactions
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE `${catalog}`.`${gold_schema}`.asian_suppliers
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE `${catalog}`.`${gold_schema}`.dim_franchises
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE `${catalog}`.`${gold_schema}`.dim_customers
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE `${catalog}`.`${gold_schema}`.fact_transactions
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
