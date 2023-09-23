from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pytz
from scripts.facts import *

default_args = dict(
    owner="airflow",
    start_date=datetime(2023, 9, 22, tzinfo=pytz.timezone("Asia/Bangkok")),
    depends_on_past=False,
    schedule_interval="0 0 * * *",
    catchup=False,
)

dag = DAG(
    "process_facts",
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
    catchup=False,
)

process_fct_booking = PythonOperator(
    python_callable=process_bookings,
    op_args=("{{ ts }}",),
    task_id="process_fct_booking",
    dag=dag,
)
process_fct_amenities = PythonOperator(
    python_callable=process_amenities,
    op_args=("{{ ts }}",),
    task_id="process_fct_amenities",
    dag=dag,
)
