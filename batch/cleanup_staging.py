from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime
from os import getenv
import pytz

mysql_conn_id = getenv("AIRFLOW_OLAP_CONN_ID")
raw_room_table = getenv("RAW_ROOM_TABLE")
raw_guest_table = getenv("RAW_GUEST_TABLE")
raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
raw_booking_table = getenv("RAW_BOOKING_TABLE")
raw_booking_addon_table = getenv("RAW_BOOKING_ADDON_TABLE")

default_args = dict(owner="airflow", depends_on_past=False)

dag = DAG(
    "cleanup_staging",
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
    start_date=datetime(2023, 9, 22, 0, 0, 0, 0, tzinfo=pytz.timezone("Asia/Bangkok")),
    schedule="30 0 * * *",
    catchup=False,
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

cleanup_stg_bookings = MySqlOperator(
    sql=delete_stg_bookings.format(
        stg_booking_table=stg_booking_table,
        stg_booking_room_table=stg_booking_room_table,
        del_booking_room_table=del_booking_room_table,
        date="{{ ds }}",
    ),
    task_id="cleanup_stg_bookings",
    mysql_conn_id=mysql_conn_id,
    dag=dag,
)

cleanup_del_bookings = MySqlOperator(
    sql=delete_del_bookings.format(
        del_booking_table=del_booking_table,
        stg_booking_room_table=stg_booking_room_table,
        del_booking_room_table=del_booking_room_table,
        date="{{ ds }}",
    ),
    task_id="cleanup_del_bookings",
    mysql_conn_id=mysql_conn_id,
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
