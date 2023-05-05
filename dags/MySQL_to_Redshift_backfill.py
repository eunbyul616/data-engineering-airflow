from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json

dag = DAG(
    dag_id="MySQL_to_Redshift_backfill",
    start_date=datetime(2023, 4, 28),
    schedule="0 9 * * *",
    max_active_runs=1,
    catchup=True,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

schema = "star1996416"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table

mysql_to_s3_nps = SqlToS3Operator(
    task_id='mysql_to_s3_nps',
    query="SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ execution_date }}')",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    sql_conn_id="mysql_conn_id",
    aws_conn_id="aws_conn_id",
    verify=False,
    replace=True,
    pd_kwargs={"index": False, "header": False},
    dag=dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id='s3_to_redshift_nps',
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    schema=schema,
    table=table,
    copy_options=['csv'],
    redshift_conn_id="redshift_dev_db",
    method="UPSERT",
    # upsert_keys=["id", "created_at"],
    upsert_keys=["id"],
    dag=dag
)

mysql_to_s3_nps >> s3_to_redshift_nps