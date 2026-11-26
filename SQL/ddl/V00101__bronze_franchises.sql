-- Append-only history of every franchises CSV file loaded from the landing volume
-- (seeded from samples.bakehouse in dev/test). Landing keeps every file, so bronze
-- can be rebuilt: changing its columns makes the next run reload it in full.
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
  _ingested_at TIMESTAMP COMMENT 'When this row was loaded',
  _source_file STRING    COMMENT 'Landing file this row came from'
)
COMMENT 'Bronze: append-only history of franchises CSV files from the landing volume';
