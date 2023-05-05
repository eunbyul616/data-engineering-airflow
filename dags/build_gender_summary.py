from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
import logging
from plugins.redshift import get_redshift_connection


def exec_sql(**context):
    schema = context['params']['schema']
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_redshift_connection()

    # temp table 생성
    sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """
    sql += select_sql
    cur.execute(sql)

    cur.execute(f"""SELECT COUNT(1) FROM {schema}.temp_{table}""")
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError(f"{schema}.{table} didn't have any record")
    else:
        pass

    # 기존 테이블 삭제 및 temp table을 table로 바꿈
    try:
        sql = f"""DROP TABLE IF EXISTS {schema}.{table};ALTER TABLE {schema}.temp_{table} RENAME to {table};"""
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)

    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")


dag = DAG(
    dag_id="build-gender-summary",
    start_date=datetime(2021, 12, 10),
    schedule='@once',
    catchup=False
)

# 집계 쿼리
execsql = PythonOperator(
    task_id='exec_summary_sql',
    python_callable=exec_sql,
    params={
        'schema': 'star1996416',
        'table': 'gender_summary',
        'sql': """
        SELECT gender, count(*) as cnt
        FROM star1996416.name_gender
        GROUP BY gender
        ORDER BY cnt;
        """
    },
    dag=dag
)
