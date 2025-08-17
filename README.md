# airflow_csv_to_postgres
# *Overview*
Project repo to demonstrate ETL orchestration using Apache Airflow, extracting data from source, transforming data (removing duplicates, remove missing value, correcting data type), load data into postgresql. All process is done automatically orchestrate by Airflow by scheduling. This repo demonstrate how utilize Airflow to conduct ETL in simple way.
# *Prerequisites*
To follow along this learning need have below requirements on system:
- python3 install
  ```bash
  sudo apt install python3
  ```
- Apache Airflow running
  ```bash
  # start the web server, default port is 8080
  airflow webserver --port 8080

  # start the scheduler
  airflow scheduler
  ```
- postgresql running
  ```bash
  sudo systemctl status postgresql
  ```
# *Project Flow*
1. Import necessary airflow library; -- airflow.decorators, airflow.providers, sqlalchemy, pandas
   ```python3
   from airflow.decorators import dag, task
   from datetime import datetime, timedelta
   from airflow.providers.postgres.hooks.postgres import PostgresHook

   import pandas as pd
   ```
3. Initialization; -- file path, database config, create connection
   ```python3
   FILE_PATH = "/home/mulyo/Learning/ETL_CSV_To_PostrgeSQL/CO2 Emission Country.csv"

   db_config = {
    'host':'localhost',
    'database':'csvpostgres',
    'user':'*****',
    'password':'*****'
   }

   POSTGRES_CONN_ID = 'postgres_conn'
   hook = PostgresHook(postgres_conn_id)
   ```
5. ETL flow; data extraction -> data transformation -> load data
   ```python3
   extracted_df = dataExtraction(FILE_PATH)
   transformed_df = dataTransformation(extracted_df)
   loadData(transformed_df, table_name, POSTGRES_CONN_ID)
   ```
7. close connetion -- auto with context
   ```python3
   ...
   hook = PostgresHook(postgres_conn_id)
     with hook.get_conn() as conn:
   ...
   ``` 
