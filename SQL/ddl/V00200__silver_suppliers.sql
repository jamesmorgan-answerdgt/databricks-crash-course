CREATE OR REPLACE TABLE `${catalog}`.`${silver_schema}`.suppliers (
  supplier_id   BIGINT NOT NULL,
  supplier_name STRING,
  ingredient    STRING,
  continent     STRING,
  city          STRING,
  district      STRING,
  longitude     DOUBLE,
  latitude      DOUBLE,
  _ingested_at  TIMESTAMP,
  CONSTRAINT silver_suppliers_pk PRIMARY KEY (supplier_id)
)
COMMENT 'One row per supplier, trimmed and de-duplicated';
