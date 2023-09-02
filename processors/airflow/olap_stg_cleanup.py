from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = dict(
    owner="airflow",
    start_date=datetime(2023, 9, 1),
    depends_on_past=False,
    schedule_interval="@daily",
    catchup=False,
)

dag = DAG(
    "olap_stg_cleanup",
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
)


def pprint():
    print(1)


cleanup_booking_addons = PythonOperator(
    python_callable=pprint, task_id="cleanup_booking_addons", dag=dag
)
cleanup_booking_rooms = PythonOperator(
    python_callable=pprint, task_id="cleanup_booking_rooms", dag=dag
)
cleanup_bookings = PythonOperator(
    python_callable=pprint, task_id="cleanup_bookings", dag=dag
)
cleanup_guests = PythonOperator(
    python_callable=pprint, task_id="cleanup_guests", dag=dag
)
cleanup_rooms = PythonOperator(python_callable=pprint, task_id="cleanup_rooms", dag=dag)
