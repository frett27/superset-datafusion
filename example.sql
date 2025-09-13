
CREATE EXTERNAL TABLE users STORED AS PARQUET LOCATION 'users.parquet';

-- Set configuration
SET datafusion.execution.batch_size = 4096;