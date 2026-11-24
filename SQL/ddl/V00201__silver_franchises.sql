CREATE OR REPLACE TABLE `${catalog}`.`${silver_schema}`.franchises (
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
  _ingested_at   TIMESTAMP,
  CONSTRAINT silver_franchises_pk PRIMARY KEY (franchise_id)
)
COMMENT 'One row per franchise, trimmed and de-duplicated';
