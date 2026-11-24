CREATE OR REPLACE TABLE `${catalog}`.`${silver_schema}`.customers (
  customer_id   BIGINT NOT NULL,
  first_name    STRING,
  last_name     STRING,
  email_address STRING,
  phone_number  STRING,
  address       STRING,
  city          STRING,
  state         STRING,
  country       STRING,
  continent     STRING,
  postal_code   STRING,
  gender        STRING,
  _ingested_at  TIMESTAMP,
  CONSTRAINT silver_customers_pk PRIMARY KEY (customer_id)
)
COMMENT 'One row per customer, trimmed and de-duplicated (contains PII)';
