from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from dotenv import load_dotenv
from os import getenv
from common import decode_data, to_list, connection_string
from db_writer import execute_query

load_dotenv()

bookings_table = getenv("BOOKINGS_TABLE")
stg_booking_table = getenv("STG_BOOKING_TABLE")
del_booking_table = getenv("DEL_BOOKING_TABLE")

upsert_query = (
    "INSERT INTO {} VALUES (:id, :checkin, :checkout, :updated_at)\n"
    "ON DUPLICATE KEY UPDATE SET checkin=:checkin, checkout=:checkout, updated_at=:updated_at"
)


def write_bookings(df: DataFrame, table_name: str):
    if df.count() == 0:
        return
    query = upsert_query.format(table_name)
    execute_query(connection_string, query, to_list(df))


def process_bookings(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, bookings_table)
    deleted_data = (
        data.filter(expr("after IS NULL AND before IS NOT NULL"))
        .select("before.*")
        .withColumn("checkin", expr("date_from_unix_date(checkin)"))
        .withColumn("checkout", expr("date_from_unix_date(checkout)"))
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .select(["id", "checkin", "checkout", "updated_at"])
    )
    write_bookings(deleted_data, del_booking_table)
    upserted_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumn("checkin", expr("date_from_unix_date(checkin)"))
        .withColumn("checkout", expr("date_from_unix_date(checkout)"))
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .select(["id", "checkin", "checkout", "updated_at"])
    )
    write_bookings(upserted_data, stg_booking_table)
