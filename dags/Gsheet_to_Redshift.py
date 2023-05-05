from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
from plugins import gsheet
from plugins import s3

import requests
import logging
import psycopg2
import json


def download_tab_in_gsheet(**context):
    url = context["params"]["url"]
    tab = context["params"]["tab"]
    table = context["params"]["table"]
    data_dir = Variable.get("local_data_dir")

    gsheet.get_google_sheet_to_csv(
        url,
        tab,
        data_dir + '{}.csv'.format(table)
    )


def copy_to_s3(**context):
    table = context["params"]["table"]
    s3_conn_id = "aws_conn_id"
    s3_bucket = "grepp-data-engineering"
    s3_key = table
    data_dir = Variable.get("local_data_dir")
    local_files_to_upload = [data_dir + '{}.csv'.format(table)]
    replace = True

    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace)


dag = DAG(
    dag_id='Gsheet_to_Redshift',
    start_date=datetime(2021, 11, 27),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 9 * * *',  # 적당히 조절
    max_active_runs=1,
    max_active_tasks=2,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

sheets = [
    {
        "url": "https://docs.google.com/spreadsheets/d/12oQYdqc1RPU6PZ0TBK1yyFdocYFNLbes/edit?usp=share_link&ouid=114463140943808436104&rtpof=true&sd=true",
        "tab": "Test",
        "schema": "star1996416",
        "table": "spreadsheet_copy_testing"
    }
]

for sheet in sheets:
    download_tab_in_gsheet = PythonOperator(
        task_id='download_{}_in_gsheet'.format(sheet["table"]),
        python_callable=download_tab_in_gsheet,
        params=sheet,
        dag=dag)

    copy_to_s3 = PythonOperator(
        task_id='copy_{}_to_s3'.format(sheet["table"]),
        python_callable=copy_to_s3,
        params={
            "table": sheet["table"]
        },
        dag=dag)

    run_copy_sql = S3ToRedshiftOperator(
        task_id='run_copy_sql_{}'.format(sheet["table"]),
        s3_bucket="grepp-data-engineering",
        s3_key=sheet["table"],
        schema=sheet["schema"],
        table=sheet["table"],
        copy_options=['csv', 'IGNOREHEADER 1'],
        method='REPLACE',
        redshift_conn_id="redshift_dev_db",
        dag=dag
    )

    download_tab_in_gsheet >> copy_to_s3 >> run_copy_sql