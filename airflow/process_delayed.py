from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from utilities.db_writer import DatabaseWriter

db_writer = DatabaseWriter()

default_args = dict(
    owner="airflow",
    start_date=datetime(2023, 9, 1),
    depends_on_past=False,
    schedule_interval="@hourly",
    catchup=False,
)

dag = DAG(
    "process_delayed_data",
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
)

remove_fct_booking = PythonOperator(
    python_callable=db_writer.remove_fct_bookings,
    task_id="remove_fct_booking",
    dag=dag,
)
remove_fct_purchase = PythonOperator(
    python_callable=db_writer.remove_fct_purchases,
    task_id="remove_fct_purchase",
    dag=dag,
)
write_fct_booking = PythonOperator(
    python_callable=db_writer.write_fct_bookings,
    task_id="write_fct_booking",
    dag=dag,
)
write_fct_purchase = PythonOperator(
    python_callable=db_writer.write_fct_purchases,
    task_id="write_fct_purchase",
    dag=dag,
)

tear_down_db_writer = PythonOperator(
    python_callable=db_writer.tear_down,
    task_id="tear_down_db_writer",
    dag=dag,
)

remove_fct_booking >> write_fct_booking
remove_fct_purchase >> write_fct_purchase
[write_fct_booking, write_fct_purchase] >> tear_down_db_writer
