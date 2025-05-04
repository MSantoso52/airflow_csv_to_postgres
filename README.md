# airflow_csv_to_postgres
Automate ETL csv to PostgreSQL with Airflow:
1. Import necessary airflow library; -- airflow.decorators, airflow.providers, sqlalchemy, pandas
2. Initialization; -- file path, database config, create connection
3. ETL flow; data extraction -> data transformation -> load data
4. close connetion -- auto with context 
