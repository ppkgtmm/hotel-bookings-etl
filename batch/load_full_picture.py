from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import pytz
from os import getenv


mysql_conn_id = getenv("AIRFLOW_DWH_CONN_ID")
dim_date_table = getenv("DIM_DATE_TABLE")
dim_roomtype_table = getenv("DIM_ROOMTYPE_TABLE")
dim_guest_table = getenv("DIM_GUEST_TABLE")
dim_location_table = getenv("DIM_LOCATION_TABLE")
fct_booking_table = getenv("FCT_BOOKING_TABLE")
dim_addon_table = getenv("DIM_ADDON_TABLE")
fct_amenities_table = getenv("FCT_AMENITIES_TABLE")
full_picture_table = getenv("FULL_PICTURE_TABLE")
load_dag_name = getenv("FACT_LOAD_DAG_NAME")
dag_name = getenv("FULL_PICTURE_DAG_NAME")

default_args = dict(owner="airflow", depends_on_past=False)


dag = DAG(
    dag_name,
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
    catchup=False,
    start_date=datetime(2023, 9, 22, 0, 0, 0, 0, tzinfo=pytz.timezone("Asia/Bangkok")),
    schedule="0 7 * * *",
)

check_facts_processed = ExternalTaskSensor(
    external_dag_id=load_dag_name,
    poke_interval=10,
    timeout=300,
    task_id="check_facts_processed",
    dag=dag,
)

process_full_picture = MySqlOperator(
    sql="scripts/full_picture.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        full_picture=full_picture_table,
        dim_date=dim_date_table,
        dim_guest=dim_guest_table,
        dim_location=dim_location_table,
        dim_roomtype=dim_roomtype_table,
        dim_addon=dim_addon_table,
        fct_amenities=fct_amenities_table,
        fct_bookings=fct_booking_table,
    ),
    task_id="process_full_picture",
    dag=dag,
)

check_facts_processed >> process_full_picture
