# DataFusion connector for setupset (still incubating)

This project add Datafusion support in superset.
Leveraging the usage of parquet file, csv,  for graphing


## 10 mins setup 

    docker compose up --build

## How to use the datafusion source 

Once superset is up, one can add a datafusion database, using the following connection string : 

    datafusion://app/setup.sql

`setup.sql` is a sql configuration to setup the datasources used by datafusion, this can be : 

```

CREATE EXTERNAL TABLE users STORED AS PARQUET LOCATION 'users.parquet';

-- Set configuration
SET datafusion.execution.batch_size = 4096;

```
to configure a parquet access



## Building from source


    git clone --recursive https://github.com/frett27/superset-datafusion


see more information for development deployment here : 

    ./setup_superset_datafusion.sh
    


