-- Landed copy of samples.bakehouse.sales_franchises.
CREATE OR REPLACE TABLE `${catalog}`.`${bronze_schema}`.franchises (
  franchiseID  BIGINT,
  name         STRING,
  city         STRING,
  district     STRING,
  zipcode      STRING,
  country      STRING,
  size         STRING,
  longitude    DOUBLE,
  latitude     DOUBLE,
  supplierID   BIGINT,
  _ingested_at TIMESTAMP COMMENT 'When this row was loaded'
)
COMMENT 'Bronze: franchises as landed from samples.bakehouse.sales_franchises';
