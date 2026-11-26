-- Append-only history of every suppliers CSV file loaded from the landing volume
-- (seeded from samples.bakehouse in dev/test). Landing keeps every file, so bronze
-- can be rebuilt: changing its columns makes the next run reload it in full.
CREATE OR REPLACE TABLE `${catalog}`.`${bronze_schema}`.suppliers (
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
  _ingested_at TIMESTAMP COMMENT 'When this row was loaded',
  _source_file STRING    COMMENT 'Landing file this row came from'
)
COMMENT 'Bronze: append-only history of suppliers CSV files from the landing volume';
