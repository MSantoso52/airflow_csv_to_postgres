from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from sqlalchemy.types import Float, Text 

import pandas as pd

FILE_PATH = "/home/mulyo/Learning/ETL_CSV_To_PostrgeSQL/CO2 Emission Country.csv"

db_config = {
    'host':'localhost',
    'database':'csvpostgres',
    'user':'postgres',
    'password':'postgres'
}

table_name = 'co2_emission'

POSTGRES_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'mulyo',
    'tries': 5,
    'try_delay': timedelta(minutes=2)
}

@dag(
    dag_id = 'csv_to_postgres_dag',
    description = 'ETL csv to postgresql',
    default_args = default_args,
    start_date = datetime(2025, 5, 3),
    schedule_interval = '@daily',
    catchup = False,
    tags=["postgres", "csv"],
)

def csv_to_postgres():

    @task()
    def dataExtraction(file_path):
        df = pd.read_csv(file_path)
        return df

    @task()
    def dataTransformation(df:pd.DataFrame)->pd.DataFrame:

        # remove duplicates
        deduplication_df = df.drop_duplicates()

        # remove n/a
        removena_df = deduplication_df.dropna()

        # data type correction
        # --Remove '%' from string data
        removena_df['% of global total'] = removena_df['% of global total'].str.replace('%', '', regex=False)

        # --Convert string to numeric on '% of global total' column
        removena_df['% of global total'] = pd.to_numeric(removena_df['% of global total'], errors='raise')

        # --Remove ',' string from value from 'Fossil emissions 2023' column
        removena_df['Fossil emissions 2023'] = removena_df['Fossil emissions 2023'].str.replace(',', '', regex=False)

        # --Convert to numeric on column 'Fossil emissions 2023'
        removena_df['Fossil emissions 2023'] = pd.to_numeric(removena_df['Fossil emissions 2023'], errors='raise')

        # --Remove ',' 'no' string from value from 'Fossil emissions 2000' column
        removena_df['Fossil emissions 2000'] = removena_df['Fossil emissions 2000'].str.replace(',', '', regex=False)
        removena_df['Fossil emissions 2000'] = removena_df['Fossil emissions 2000'].str.replace('no', '', regex=False)

        # --Convert to numeric on column 'Fossil emissions 2000'
        removena_df['Fossil emissions 2000'] = pd.to_numeric(removena_df['Fossil emissions 2000'], errors='raise')

        # --Remove '%' '+' string from value from '% change 2000' column
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace('%', '', regex=False)
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace('+', '', regex=False)
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace("−", "-")

        # --Remove ',' 'change' string from value from '% change 2000' column
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace(',', '', regex=False)
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace('change', '', regex=False)

        # --Convert to numeric on column '% change 2000'
        removena_df['% change from 2000'] = pd.to_numeric(removena_df['% change from 2000'], errors='raise')

        return removena_df

    @task()
    def loadData(df:pd.DataFrame, table_name:str, postgres_conn_id:str):
        try:
            hook = PostgresHook(postgres_conn_id)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    create_table_sql=f'''
                        CREATE TABLE IF NOT EXISTS {table_name}(
                        location VARCHAR(100), 
                        global_total DECIMAL(10,2),
                        emission_2023 DECIMAL(10,2),
                        emission_2000 DECIMAL(10,2),
                        change_from_2000 DECIMAL(10,2)
                        );
                    '''
                    cur.execute(create_table_sql)
                    conn.commit()

                    for index, row in df.iterrows():
                        sql = f'''
                            INSERT INTO {table_name} (location, global_total, emission_2023, emission_2000, change_from_2000)
                            VALUES (%s, %s, %s, %s, %s)
                        '''
                        cur.execute(sql, row.tolist())
                    conn.commit()
        except Exception as e:
            print(e)

    extracted_df = dataExtraction(FILE_PATH)
    transformed_df = dataTransformation(extracted_df)
    loadData(transformed_df, table_name, POSTGRES_CONN_ID)

etl = csv_to_postgres() 

