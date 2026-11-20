CREATE TABLE dev.bronze.sales_franchises_raw (
  franchiseID BIGINT,
  name STRING,
  city STRING,
  district STRING,
  zipcode STRING,
  country STRING,
  size STRING,
  longitude DOUBLE,
  latitude DOUBLE,
  supplierID BIGINT)
USING DELTA
COMMENT 'The table contains data related to franchises, including their locations and characteristics. It can be used to analyze franchise distribution, understand regional performance, and assess market presence. Key information includes franchise identifiers, names, and geographical details such as city, district, zipcode, and coordinates.'
CLUSTER BY AUTO;