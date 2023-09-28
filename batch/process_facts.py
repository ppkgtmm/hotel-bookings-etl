from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pytz
from os import getenv
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text
from math import ceil

db_host = getenv("DB_HOST_INTERNAL")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")
dim_date_table = getenv("DIM_DATE_TABLE")
fmt = getenv("DT_FORMAT")

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
dag_name = getenv("FACT_LOAD_DAG_NAME")

default_args = dict(owner="airflow", depends_on_past=False)


def generate_datetime(base_date: datetime, hour_diff: int):
    for hour in range(hour_diff):
        date_time = base_date + timedelta(hours=hour + 1)
        yield dict(
            id=int(date_time.strftime(fmt)),
            datetime=date_time,
            date=date_time.date(),
            month=date_time.replace(day=1).date(),
            quarter=date_time.replace(
                month=((date_time.month - 1) // 3) * 3 + 1, day=1
            ).date(),
            year=date_time.replace(month=1, day=1).date(),
        )


def insert_dim_date(ts: str):
    engine = create_engine(
        url=f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
        poolclass=NullPool,
    )
    with engine.connect() as conn:
        max_date = conn.execute(f"SELECT MAX(datetime) FROM {dim_date_table}").all()
        if not max_date or len(max_date) == 0:
            return
        max_date = max_date[0][0]
        seconds_in_day = 24 * 3600
        end_date = datetime.fromisoformat(ts) + timedelta(days=8)
        delta = end_date.replace(tzinfo=None) - max_date
        hour_diff = ceil((delta.days * seconds_in_day + delta.seconds) / 3600)
        for data in generate_datetime(max_date, hour_diff):
            conn.execute(
                text(
                    f"INSERT INTO {dim_date_table} VALUES (:id, :datetime, :date, :month, :quarter, :year)"
                ),
                data,
            )


dag = DAG(
    dag_name,
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
    catchup=False,
    start_date=datetime(2023, 9, 22, 0, 0, 0, 0, tzinfo=pytz.timezone("Asia/Bangkok")),
    schedule="0 7 * * *",
)

ensure_dim_date_populated = PythonOperator(
    python_callable=insert_dim_date, task_id="ensure_dim_date_populated", dag=dag
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

ensure_dim_date_populated >> process_fct_booking >> process_fct_amenities
