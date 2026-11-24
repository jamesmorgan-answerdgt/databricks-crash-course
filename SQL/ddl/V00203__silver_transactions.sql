CREATE OR REPLACE TABLE `${catalog}`.`${silver_schema}`.transactions (
  transaction_id BIGINT NOT NULL,
  customer_id    BIGINT,
  franchise_id   BIGINT,
  transaction_ts TIMESTAMP,
  product        STRING,
  quantity       INT,
  unit_price     DECIMAL(18, 2),
  total_price    DECIMAL(18, 2),
  payment_method STRING,
  card_last4     STRING COMMENT 'Last four digits only; full number stays in bronze',
  _ingested_at   TIMESTAMP,
  CONSTRAINT silver_transactions_pk PRIMARY KEY (transaction_id)
)
COMMENT 'One row per transaction, typed and de-duplicated';
