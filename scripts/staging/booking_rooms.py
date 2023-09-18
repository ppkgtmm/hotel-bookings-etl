from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from os import getenv
from common import decode_data, get_connection_string
from db_writer import execute_query


booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
temp_booking_room_table = "temp_" + booking_rooms_table

upsert_query = (
    "INSERT INTO {} SELECT *, false, false FROM {} src ON DUPLICATE KEY UPDATE "
    "room=src.room, guest=src.guest, updated_at=src.updated_at, processed=false"
)

delete_query = (
    "UPDATE {} dest INNER JOIN {} src ON dest.id = src.id SET dest.is_deleted = true"
)


def write_booking_rooms(df: DataFrame, delete: bool = False):
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("url", get_connection_string())
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", temp_booking_room_table)
        .save()
    )
    query = delete_query if delete else upsert_query
    query = query.format(raw_booking_room_table, temp_booking_room_table)
    execute_query(get_connection_string(False), query)


def process_booking_rooms(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, booking_rooms_table)
    deleted_data = (
        data.filter(expr("after IS NULL AND before IS NOT NULL"))
        .select("before.*")
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .select(["id", "booking", "room", "guest", "updated_at"])
    )
    write_booking_rooms(deleted_data, True)
    upserted_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .select(["id", "booking", "room", "guest", "updated_at"])
    )
    write_booking_rooms(upserted_data)
