-- CDM franchise dimension.
CREATE OR REPLACE TABLE `${catalog}`.`${gold_schema}`.dim_franchises (
  franchise_id   BIGINT NOT NULL,
  franchise_name STRING,
  city           STRING,
  district       STRING,
  zip_code       STRING,
  country        STRING,
  size           STRING,
  longitude      DOUBLE,
  latitude       DOUBLE,
  supplier_id    BIGINT,
  CONSTRAINT gold_dim_franchises_pk PRIMARY KEY (franchise_id)
)
CLUSTER BY (country)
COMMENT 'Franchise dimension';
