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