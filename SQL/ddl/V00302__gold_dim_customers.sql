-- CDM customer dimension. Contact details (email, phone, street address)
-- stay in silver so the business-facing layer holds as little PII as possible.
CREATE OR REPLACE TABLE `${catalog}`.`${gold_schema}`.dim_customers (
  customer_id BIGINT NOT NULL,
  first_name  STRING,
  last_name   STRING,
  city        STRING,
  state       STRING,
  country     STRING,
  continent   STRING,
  postal_code STRING,
  gender      STRING,
  CONSTRAINT gold_dim_customers_pk PRIMARY KEY (customer_id)
)
CLUSTER BY (country)
COMMENT 'Customer dimension (no contact details)';
