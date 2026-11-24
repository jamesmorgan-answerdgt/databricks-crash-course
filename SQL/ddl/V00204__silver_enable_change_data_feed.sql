-- Gold reads only what changed in silver (inserts, updates, deletes) via the
-- Delta change data feed, so it must be switched on for every silver table.
-- Safe to re-run.

ALTER TABLE `${catalog}`.`${silver_schema}`.suppliers
  SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
ALTER TABLE `${catalog}`.`${silver_schema}`.franchises
  SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
ALTER TABLE `${catalog}`.`${silver_schema}`.customers
  SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
ALTER TABLE `${catalog}`.`${silver_schema}`.transactions
  SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
