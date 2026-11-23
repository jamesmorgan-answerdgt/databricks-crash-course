-- Landed copy of samples.bakehouse.sales_suppliers.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${bronze_schema}`.suppliers (
  supplierID   BIGINT,
  name         STRING,
  ingredient   STRING,
  continent    STRING,
  city         STRING,
  district     STRING,
  size         STRING,
  longitude    DOUBLE,
  latitude     DOUBLE,
  approved     STRING,
  _ingested_at TIMESTAMP COMMENT 'When this row was loaded'
)
COMMENT 'Bronze: suppliers as landed from samples.bakehouse.sales_suppliers';
