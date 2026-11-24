-- Bronze is now append-only, loaded by Auto Loader from the landing volume.
-- _source_file: the landing file each row came from.
-- _rescued_data: any source columns or values that don't fit the table schema,
--   so new or changed source columns are kept rather than silently dropped.
ALTER TABLE `${catalog}`.`${bronze_schema}`.franchises
  ADD COLUMNS (_source_file STRING, _rescued_data STRING);
