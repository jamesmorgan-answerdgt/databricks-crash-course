-- One schema per medallion layer, per environment (e.g. dev_bronze, prod_gold).
CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${landing_schema}`
  COMMENT 'Landing: raw data ingestion area';

CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${bronze_schema}`
  COMMENT 'Bronze: source data as landed, unmodified';

CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${silver_schema}`
  COMMENT 'Silver: cleaned, typed and de-duplicated data';

CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${gold_schema}`
  COMMENT 'Gold: common data model (CDM) - business-facing tables';
