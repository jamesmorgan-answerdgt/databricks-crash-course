-- Productionised version of the asian_suppliers table from exploration/practice.ipynb.
-- Keyed on the source supplier ID rather than uuid() so reloads are idempotent.
CREATE OR REPLACE TABLE `${catalog}`.`${gold_schema}`.asian_suppliers (
  supplier_id   BIGINT NOT NULL,
  supplier_name STRING,
  CONSTRAINT gold_asian_suppliers_pk PRIMARY KEY (supplier_id)
)
CLUSTER BY (supplier_name)  -- liquid clustering
COMMENT 'Suppliers based in Asia';
