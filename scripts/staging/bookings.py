from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from dotenv import load_dotenv
from os import getenv
from common import decode_data, get_connection_string
from db_writer import execute_query

load_dotenv()

bookings_table = getenv("BOOKINGS_TABLE")
raw_booking_table = getenv("RAW_BOOKING_TABLE")
temp_booking_table = "temp_" + raw_booking_table

upsert_query = (
    "INSERT INTO {} SELECT *, false FROM {} src ON DUPLICATE KEY UPDATE "
    "checkin=src.checkin, checkout=src.checkout, updated_at=src.updated_at"
)

delete_query = (
    "UPDATE {} dest INNER JOIN {} src "
    "ON dest.id = src.id "
    "SET dest.is_deleted = true"
)


def write_bookings(df: DataFrame, delete: bool = False):
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("url", get_connection_string())
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", temp_booking_table)
        .save()
    )
    query = delete_query if delete else upsert_query
    query = query.format(raw_booking_table, temp_booking_table)
    execute_query(get_connection_string(False), query)


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
    write_bookings(deleted_data, True)
    upserted_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumn("checkin", expr("date_from_unix_date(checkin)"))
        .withColumn("checkout", expr("date_from_unix_date(checkout)"))
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .select(["id", "checkin", "checkout", "updated_at"])
    )
    write_bookings(upserted_data)
