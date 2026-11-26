-- Append-only history of every transactions CSV file loaded from the landing volume
-- (seeded from samples.bakehouse in dev/test). Landing keeps every file, so bronze
-- can be rebuilt: changing its columns makes the next run reload it in full.
CREATE OR REPLACE TABLE `${catalog}`.`${bronze_schema}`.transactions (
  transactionID BIGINT,
  customerID    BIGINT,
  franchiseID   BIGINT,
  dateTime      TIMESTAMP,
  product       STRING,
  quantity      BIGINT,
  unitPrice     BIGINT,
  totalPrice    BIGINT,
  paymentMethod STRING,
  cardNumber    STRING,
  _ingested_at  TIMESTAMP COMMENT 'When this row was loaded',
  _source_file  STRING    COMMENT 'Landing file this row came from'
)
COMMENT 'Bronze: append-only history of transactions CSV files from the landing volume (contains card numbers)';
