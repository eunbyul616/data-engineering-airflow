from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta
from plugins import slack
from plugins.redshift import get_redshift_connection

import requests
import logging
import psycopg2


def extract(**context):
    link = context["params"]["url"]
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    f = requests.get(link)
    return (f.text)


def transform(**context):
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.split("\n")[1:]
    return lines


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    sql = "BEGIN; DELETE FROM {schema}.{table};".format(schema=schema, table=table)
    for line in lines:
        if line != "":
            (name, gender) = line.split(",")
            print(name, "-", gender)
            sql += f"""INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"""
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)


dag = DAG(
    dag_id='name_gender_csv_to_redshift',
    start_date=datetime(2023, 4, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 2 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack.on_failure_callback,
    }
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'url': Variable.get("csv_url")
    },
    dag=dag)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    params={
    },
    dag=dag)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'schema': 'star1996416',
        'table': 'name_gender'
    },
    dag=dag)

trigger = TriggerDagRunOperator(
        task_id='trigger-next-job',
        trigger_dag_id="build-gender-summary",
        dag=dag)

extract >> transform >> load >> trigger
