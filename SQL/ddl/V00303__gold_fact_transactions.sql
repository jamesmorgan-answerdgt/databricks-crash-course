-- CDM sales fact. Foreign keys are informational (not enforced) but document
-- the star schema for BI tools and anyone reading the catalog.
CREATE OR REPLACE TABLE `${catalog}`.`${gold_schema}`.fact_transactions (
  transaction_id   BIGINT NOT NULL,
  transaction_ts   TIMESTAMP,
  transaction_date DATE,
  customer_id      BIGINT,
  franchise_id     BIGINT,
  product          STRING,
  quantity         INT,
  unit_price       DECIMAL(18, 2),
  total_price      DECIMAL(18, 2),
  payment_method   STRING,
  CONSTRAINT gold_fact_transactions_pk PRIMARY KEY (transaction_id),
  CONSTRAINT gold_fact_transactions_customer_fk FOREIGN KEY (customer_id)
    REFERENCES `${catalog}`.`${gold_schema}`.dim_customers,
  CONSTRAINT gold_fact_transactions_franchise_fk FOREIGN KEY (franchise_id)
    REFERENCES `${catalog}`.`${gold_schema}`.dim_franchises
)
CLUSTER BY (transaction_date)
COMMENT 'Sales transactions fact';
