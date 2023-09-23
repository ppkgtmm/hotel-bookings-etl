from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pytz
from os import getenv
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from math import ceil

db_host = getenv("DB_HOST_INTERNAL")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")
dim_date_table = getenv("DIM_DATE_TABLE")


def generate_datetime(base_date: datetime, hour_diff: int):
    for hour in range(hour_diff):
        date_time = base_date + timedelta(hours=hour + 1)
        yield dict(
            id=int(date_time.strftime("%Y%m%d%H%M%S")),
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
        end_date = datetime.fromisoformat(ts) + timedelta(days=1)
        delta = end_date.replace(tzinfo=None) - max_date
        hour_diff = ceil((delta.days * seconds_in_day + delta.seconds) / 3600)
        conn.execute(
            f"INSERT INTO {dim_date_table} VALUES (:id, :datetime, :date, :month, :quarter, :year)",
            list(generate_datetime(max_date, hour_diff)),
        )


default_args = dict(owner="airflow", depends_on_past=False)

dag = DAG(
    "process_dims",
    default_args=default_args,
    max_active_runs=1,  # no concurrent runs
    catchup=False,
    start_date=datetime(2023, 9, 22, 0, 0, 0, 0, tzinfo=pytz.timezone("Asia/Bangkok")),
    schedule="0 0 * * *",
)

populate_dim_date = PythonOperator(
    python_callable=insert_dim_date, task_id="populate_dim_date", dag=dag
)
