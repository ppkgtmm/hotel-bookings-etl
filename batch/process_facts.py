from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime
import pytz
from os import getenv

mysql_conn_id = getenv("AIRFLOW_OLAP_CONN_ID")
raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
raw_booking_table = getenv("RAW_BOOKING_TABLE")
raw_room_table = getenv("RAW_ROOM_TABLE")
raw_guest_table = getenv("RAW_GUEST_TABLE")
dim_roomtype_table = getenv("DIM_ROOMTYPE_TABLE")
dim_guest_table = getenv("DIM_GUEST_TABLE")
dim_location_table = getenv("DIM_LOCATION_TABLE")
fct_booking_table = getenv("FCT_BOOKING_TABLE")
raw_booking_addon_table = getenv("RAW_BOOKING_ADDON_TABLE")
dim_addon_table = getenv("DIM_ADDON_TABLE")
fct_amenities_table = getenv("FCT_AMENITIES_TABLE")

default_args = dict(
    owner="airflow",
    depends_on_past=False,
)

dag = DAG(
    "process_facts",
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
    catchup=False,
    start_date=datetime(2023, 9, 22, 0, 0, 0, 0, tzinfo=pytz.timezone("Asia/Bangkok")),
    schedule="0 0 * * *",
)

process_fct_booking = MySqlOperator(
    sql="scripts/fct_bookings.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        bookings=raw_booking_table,
        booking_rooms=raw_booking_room_table,
        dim_guest=dim_guest_table,
        rooms=raw_room_table,
        guests=raw_guest_table,
        dim_location=dim_location_table,
        dim_roomtype=dim_roomtype_table,
        fct_bookings=fct_booking_table,
    ),
    task_id="process_fct_booking",
    dag=dag,
)

process_fct_amenities = MySqlOperator(
    sql="scripts/fct_amenities.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        booking_addons=raw_booking_addon_table,
        booking_rooms=raw_booking_room_table,
        dim_guest=dim_guest_table,
        rooms=raw_room_table,
        guests=raw_guest_table,
        dim_location=dim_location_table,
        dim_roomtype=dim_roomtype_table,
        dim_addon=dim_addon_table,
        fct_amenities=fct_amenities_table,
    ),
    task_id="process_fct_amenities",
    dag=dag,
)
