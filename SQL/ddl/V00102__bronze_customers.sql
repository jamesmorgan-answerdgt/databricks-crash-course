-- Append-only history of every customers CSV file loaded from the landing volume
-- (seeded from samples.bakehouse in dev/test). Landing keeps every file, so bronze
-- can be rebuilt: changing its columns makes the next run reload it in full.
CREATE OR REPLACE TABLE `${catalog}`.`${bronze_schema}`.customers (
  customerID      BIGINT,
  first_name      STRING,
  last_name       STRING,
  email_address   STRING,
  phone_number    STRING,
  address         STRING,
  city            STRING,
  state           STRING,
  country         STRING,
  continent       STRING,
  postal_zip_code STRING,
  gender          STRING,
  _ingested_at    TIMESTAMP COMMENT 'When this row was loaded',
  _source_file    STRING    COMMENT 'Landing file this row came from'
)
COMMENT 'Bronze: append-only history of customers CSV files from the landing volume';
