


DROP TABLE IF EXISTS {$Catalog}.presented_master.tb_dim_Employer;

CREATE TABLE {$Catalog}.presented_master.tb_dim_Employer
USING DELTA
LOCATION 'abfss://{lake_root}@stajobtrendanalytics.dfs.core.windows.net/Presented/dim_Employer/Schema=1';