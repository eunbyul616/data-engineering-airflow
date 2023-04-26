from airflow imoprt DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta

import requests
import logging
import psycopg2

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgre_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract(**context):
    link = context["params"]["url"]
    task_instance = context["task_instance"]
    execution_date = context["execution_date"]

    logging.info(execution_date)
    f = requests.get(link)
    return f.text

def transform(**context):
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    # header 처리
    lines = text.split("\n")[1:]
    return lines

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")

    sql = f"BEGIN; DELETE FROM {schema}.{table};"
    for line in lines:
        if line != "":
            (name, gender) = line.split(",")
            print(f"{name}-{gender}")
            sql += f"INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)

dag_name_gender_etl = DAG(
    dag_id="name_gender_etl",
    start_date=datetime(2023, 4, 6),
    schedule="0 2 * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
    }
)

extract = PythonOperator(
    task_id="extract",
    python_callable=extract,
    params={
        'url': Variable.get("csv_url")
    },
    dag=dag_name_gender_etl
)
transform = PythonOperator(
    task_id="transform",
    python_callable=transform,
    params={},
    dag=dag_name_gender_etl
)
load = PythonOperator(
    task_id="load",
    python_callable=load,
    params={
        'schema': 'star1996416',
        'table': 'name_gender'
    },
    dag=dag_name_gender_etl
)

extract >> transform >> load