from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from os import getenv
from common import decode_data, get_connection_string
from db_writer import execute_query


booking_addons_table = getenv("BOOKING_ADDONS_TABLE")
raw_booking_addon_table = getenv("RAW_BOOKING_ADDON_TABLE")
temp_booking_addon_table = "temp_" + booking_addons_table

upsert_query = (
    "INSERT INTO {} SELECT *, false, false FROM {} src ON DUPLICATE KEY UPDATE "
    "addon=src.addon, quantity=src.quantity, datetime=src.datetime, updated_at=src.updated_at, processed=false"
)

delete_query = (
    "UPDATE {} dest INNER JOIN {} src ON dest.id = src.id SET dest.is_deleted = true"
)


def write_booking_addons(df: DataFrame, delete: bool = False):
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("url", get_connection_string())
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", temp_booking_addon_table)
        .save()
    )
    query = delete_query if delete else upsert_query
    query = query.format(raw_booking_addon_table, temp_booking_addon_table)
    execute_query(get_connection_string(False), query)


def process_booking_addons(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, booking_addons_table)
    deleted_data = (
        data.filter(expr("after IS NULL AND before IS NOT NULL"))
        .select("before.*")
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .withColumn("datetime", expr("timestamp_millis(datetime)"))
        .select(["id", "booking_room", "addon", "quantity", "datetime", "updated_at"])
    )
    write_booking_addons(deleted_data, True)
    upserted_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .withColumn("datetime", expr("timestamp_millis(datetime)"))
        .select(["id", "booking_room", "addon", "quantity", "datetime", "updated_at"])
    )
    write_booking_addons(upserted_data)
