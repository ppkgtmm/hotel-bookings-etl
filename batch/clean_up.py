from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from os import getenv
import pytz

mysql_conn_id = getenv("AIRFLOW_DWH_CONN_ID")
raw_room_table = getenv("RAW_ROOM_TABLE")
raw_guest_table = getenv("RAW_GUEST_TABLE")
raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
raw_booking_table = getenv("RAW_BOOKING_TABLE")
raw_booking_addon_table = getenv("RAW_BOOKING_ADDON_TABLE")
dag_name = getenv("CLEAN_UP_DAG_NAME")
load_dag_name = getenv("FACT_LOAD_DAG_NAME")

default_args = dict(owner="airflow", depends_on_past=False)

dag = DAG(
    dag_name,
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
    start_date=datetime(2023, 9, 22, 0, 0, 0, 0, tzinfo=pytz.timezone("Asia/Bangkok")),
    schedule="0 7 * * *",
    catchup=False,
)

check_facts_processed = ExternalTaskSensor(
    external_dag_id=load_dag_name,
    poke_interval=10,
    timeout=300,
    task_id="check_facts_processed",
    dag=dag,
)

cleanup_booking_addons = MySqlOperator(
    sql="scripts/stg_booking_addons.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(booking_addons=raw_booking_addon_table),
    task_id="cleanup_booking_addons",
    dag=dag,
)

cleanup_booking_rooms = MySqlOperator(
    sql="scripts/stg_booking_rooms.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        booking_addons=raw_booking_addon_table,
        booking_rooms=raw_booking_room_table,
    ),
    task_id="cleanup_booking_rooms",
    dag=dag,
)

cleanup_bookings = MySqlOperator(
    sql="scripts/stg_bookings.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        bookings=raw_booking_table,
        booking_rooms=raw_booking_room_table,
    ),
    task_id="cleanup_bookings",
    dag=dag,
)

cleanup_guests = MySqlOperator(
    sql="scripts/stg_guests.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(guests=raw_guest_table),
    task_id="cleanup_guests",
    dag=dag,
)

cleanup_rooms = MySqlOperator(
    sql="scripts/stg_rooms.sql",
    task_id="cleanup_rooms",
    mysql_conn_id=mysql_conn_id,
    params=dict(rooms=raw_room_table),
    dag=dag,
)

check_facts_processed >> [cleanup_bookings, cleanup_guests, cleanup_rooms]
cleanup_bookings >> cleanup_booking_rooms >> cleanup_booking_addons
