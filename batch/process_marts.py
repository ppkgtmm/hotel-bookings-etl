from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime
import pytz
from os import getenv

mysql_conn_id = getenv("AIRFLOW_OLAP_CONN_ID")
dim_date_table = getenv("DIM_DATE_TABLE")
dim_roomtype_table = getenv("DIM_ROOMTYPE_TABLE")
dim_guest_table = getenv("DIM_GUEST_TABLE")
dim_location_table = getenv("DIM_LOCATION_TABLE")
dim_addon_table = getenv("DIM_ADDON_TABLE")
fct_booking_table = getenv("FCT_BOOKING_TABLE")
fct_amenities_table = getenv("FCT_AMENITIES_TABLE")
mrt_age_table = getenv("MRT_AGE")
mrt_gender_table = getenv("MRT_GENDER")
mrt_location_table = getenv("MRT_LOCATION")
mrt_roomtype_table = getenv("MRT_ROOMTYPE")
mrt_addon_table = getenv("MRT_ADDON")

dag_name = getenv("MART_LOAD_DAG_NAME")

default_args = dict(owner="airflow", depends_on_past=False)


dag = DAG(
    dag_name,
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
    catchup=False,
    start_date=datetime(2023, 9, 22, 0, 0, 0, 0, tzinfo=pytz.timezone("Asia/Bangkok")),
    schedule="0 7 * * *",
)


process_mrt_age = MySqlOperator(
    sql="scripts/mrt_age.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        mrt_age=mrt_age_table,
        fct_bookings=fct_booking_table,
        fct_amenities=fct_amenities_table,
        dim_addon=dim_addon_table,
        dim_date=dim_date_table,
        dim_guest=dim_guest_table,
        dim_roomtype=dim_roomtype_table,
    ),
    task_id="process_mrt_age",
    dag=dag,
)

process_mrt_gender = MySqlOperator(
    sql="scripts/mrt_gender.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        mrt_gender=mrt_gender_table,
        fct_bookings=fct_booking_table,
        fct_amenities=fct_amenities_table,
        dim_addon=dim_addon_table,
        dim_date=dim_date_table,
        dim_guest=dim_guest_table,
        dim_roomtype=dim_roomtype_table,
    ),
    task_id="process_mrt_gender",
    dag=dag,
)

process_mrt_location = MySqlOperator(
    sql="scripts/mrt_location.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        mrt_location=mrt_location_table,
        fct_bookings=fct_booking_table,
        fct_amenities=fct_amenities_table,
        dim_addon=dim_addon_table,
        dim_date=dim_date_table,
        dim_location=dim_location_table,
        dim_roomtype=dim_roomtype_table,
    ),
    task_id="process_mrt_location",
    dag=dag,
)

process_mrt_roomtype = MySqlOperator(
    sql="scripts/mrt_roomtype.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        mrt_roomtype=mrt_roomtype_table,
        fct_bookings=fct_booking_table,
        dim_date=dim_date_table,
        dim_roomtype=dim_roomtype_table,
    ),
    task_id="process_mrt_roomtype",
    dag=dag,
)

process_mrt_addon = MySqlOperator(
    sql="scripts/mrt_addon.sql",
    mysql_conn_id=mysql_conn_id,
    params=dict(
        mrt_addon=mrt_addon_table,
        fct_amenities=fct_amenities_table,
        dim_date=dim_date_table,
        dim_addon=dim_addon_table,
    ),
    task_id="process_mrt_addon",
    dag=dag,
)

(
    process_mrt_age
    >> process_mrt_gender
    >> process_mrt_location
    >> process_mrt_roomtype
    >> process_mrt_addon
)
