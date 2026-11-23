-- Landing zone: managed UC volumes for raw source files picked up by Auto Loader.
CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${landing_schema}`
  COMMENT 'Raw source files landed for ingestion into bronze';

CREATE VOLUME IF NOT EXISTS `${catalog}`.`${landing_schema}`.suppliers
  COMMENT 'Raw supplier files';

CREATE VOLUME IF NOT EXISTS `${catalog}`.`${landing_schema}`.customers
  COMMENT 'Raw customer files';

CREATE VOLUME IF NOT EXISTS `${catalog}`.`${landing_schema}`.franchises
  COMMENT 'Raw franchise files';

CREATE VOLUME IF NOT EXISTS `${catalog}`.`${landing_schema}`.transactions
  COMMENT 'Raw transaction files';

-- Auto Loader checkpoints and schema tracking (kept apart from the data)
CREATE VOLUME IF NOT EXISTS `${catalog}`.`${landing_schema}`._checkpoints
  COMMENT 'Auto Loader checkpoint and schema locations';

-- One schema per medallion layer, per environment (e.g. dev_bronze, prod_gold).
CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${landing_schema}`
  COMMENT 'Landing: raw data ingestion area';

CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${bronze_schema}`
  COMMENT 'Bronze: source data as landed, unmodified';

CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${silver_schema}`
  COMMENT 'Silver: cleaned, typed and de-duplicated data';

CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${gold_schema}`
  COMMENT 'Gold: common data model (CDM) - business-facing tables';