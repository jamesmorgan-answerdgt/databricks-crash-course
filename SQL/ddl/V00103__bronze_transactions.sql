-- Landed copy of samples.bakehouse.sales_transactions.
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
  _ingested_at  TIMESTAMP COMMENT 'When this row was loaded'
)
COMMENT 'Bronze: transactions as landed from samples.bakehouse.sales_transactions (contains card numbers)';
