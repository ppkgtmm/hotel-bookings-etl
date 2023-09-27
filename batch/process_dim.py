from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pytz
from os import getenv


default_args = dict(owner="airflow", depends_on_past=False)

dag = DAG(
    "process_dims",
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
    catchup=False,
    start_date=datetime(2023, 9, 22, 0, 0, 0, 0, tzinfo=pytz.timezone("Asia/Bangkok")),
    schedule="0 7 * * *",
)
