-- Quarantine: landing files that bronze rejected are moved here unchanged
-- (still CSV), under quarantine/<source>/<batch id>/<original path>, to be fixed
-- and dropped back into landing. quarantine_log records why each was rejected.
CREATE VOLUME IF NOT EXISTS `${catalog}`.`${landing_schema}`.quarantine
  COMMENT 'Landing files rejected by bronze, kept as-is for fixing and reprocessing';

CREATE TABLE IF NOT EXISTS `${catalog}`.`${landing_schema}`.quarantine_log (
  source              STRING    COMMENT 'suppliers, franchises, customers or transactions',
  source_file         STRING    COMMENT 'Where the file was in landing',
  quarantine_file     STRING    COMMENT 'Where it is now',
  reason              STRING,
  bad_row_count       BIGINT,
  total_row_count     BIGINT,
  sample_bad_data     ARRAY<STRING> COMMENT 'Up to 5 examples of what did not fit the bronze schema',
  quarantined_at      TIMESTAMP,
  resolved_at         TIMESTAMP COMMENT 'Set once the fixed file is back in landing'
)
COMMENT 'Why each quarantined landing file was rejected'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
