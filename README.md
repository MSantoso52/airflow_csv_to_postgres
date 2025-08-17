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
- postgreql running
# *Project Flow*
Automate ETL csv to PostgreSQL with Airflow:
1. Import necessary airflow library; -- airflow.decorators, airflow.providers, sqlalchemy, pandas
2. Initialization; -- file path, database config, create connection
3. ETL flow; data extraction -> data transformation -> load data
4. close connetion -- auto with context 
