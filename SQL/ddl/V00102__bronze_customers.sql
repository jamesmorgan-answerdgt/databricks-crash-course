-- Landed copy of samples.bakehouse.sales_customers.
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
  _ingested_at    TIMESTAMP COMMENT 'When this row was loaded'
)
COMMENT 'Bronze: customers as landed from samples.bakehouse.sales_customers';
