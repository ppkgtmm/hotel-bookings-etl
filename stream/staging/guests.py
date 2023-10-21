from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from os import getenv
from common import decode_data, get_connection_string
from db_writer import execute_query

guest_table = getenv("GUESTS_TABLE")
raw_guest_table = getenv("RAW_GUEST_TABLE")
temp_guest_table = "temp_" + guest_table

upsert_query = (
    "INSERT INTO {} SELECT *, false FROM {} src ON DUPLICATE KEY UPDATE "
    "state=src.state, country=src.country, updated_at=src.updated_at"
)

delete_query = (
    "UPDATE {} dest INNER JOIN {} src ON dest.id = src.id SET dest.is_deleted = true"
)


def write_guests(df: DataFrame, delete: bool = False):
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("url", get_connection_string())
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", temp_guest_table)
        .save()
    )
    query = delete_query if delete else upsert_query
    query = query.format(raw_guest_table, temp_guest_table)
    execute_query(get_connection_string(False), query)


def process_guests(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, guest_table)
    deleted_data = (
        data.filter(expr("after IS NULL AND before IS NOT NULL"))
        .select("before.*")
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .select(["id", "state", "country", "updated_at"])
    )
    write_guests(deleted_data, True)
    upserted_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .select(["id", "state", "country", "updated_at"])
    )
    write_guests(upserted_data)
