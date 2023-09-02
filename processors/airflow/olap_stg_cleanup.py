from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from os import getenv
from utilities.constants import *
from utilities.db_writer import DatabaseWriter

db_writer = DatabaseWriter()

mysql_conn_id = getenv["AIRFLOW_OLAP_CONN_ID"]

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

cleanup_stg_booking_addons = create_table = MySqlOperator(
    sql=delete_stg_booking_addons.format(
        stg_booking_addon_table=stg_booking_addon_table, datetime="{{ ts }}"
    ),
    task_id="cleanup_stg_booking_addons",
    mysql_conn_id=mysql_conn_id,
    dag=dag,
)

cleanup_del_booking_addons = create_table = MySqlOperator(
    sql=delete_del_booking_addons.format(
        del_booking_addon_table=del_booking_addon_table, datetime="{{ ts }}"
    ),
    task_id="cleanup_del_booking_addons",
    mysql_conn_id=mysql_conn_id,
    dag=dag,
)

cleanup_stg_booking_rooms = MySqlOperator(
    sql=delete_stg_booking_rooms.format(
        stg_booking_room_table=stg_booking_room_table,
        stg_booking_table=stg_booking_table,
        del_booking_table=del_booking_table,
        date="{{ ds }}",
    ),
    task_id="cleanup_stg_booking_rooms",
    mysql_conn_id=mysql_conn_id,
    dag=dag,
)

cleanup_del_booking_rooms = MySqlOperator(
    sql=delete_del_booking_rooms.format(
        del_booking_room_table=del_booking_room_table,
        stg_booking_table=stg_booking_table,
        del_booking_table=del_booking_table,
        date="{{ ds }}",
    ),
    task_id="cleanup_del_booking_rooms",
    mysql_conn_id=mysql_conn_id,
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
    sql=delete_guests_query.format(stg_guest_table=stg_guest_table),
    task_id="cleanup_guests",
    mysql_conn_id=mysql_conn_id,
    dag=dag,
)

cleanup_rooms = MySqlOperator(
    sql=delete_rooms_query.format(stg_room_table=stg_room_table),
    task_id="cleanup_rooms",
    mysql_conn_id=mysql_conn_id,
    dag=dag,
)

remove_fct_booking >> write_fct_booking
remove_fct_purchase >> write_fct_purchase
[write_fct_booking, write_fct_purchase] >> tear_down_db_writer

tear_down_db_writer >> [
    cleanup_stg_booking_addons,
    cleanup_del_booking_addons,
    cleanup_stg_booking_rooms,
    cleanup_del_booking_rooms,
    cleanup_guests,
    cleanup_rooms,
]
[cleanup_stg_booking_rooms, cleanup_del_booking_rooms] >> cleanup_stg_bookings
[cleanup_stg_booking_rooms, cleanup_del_booking_rooms] >> cleanup_del_bookings
