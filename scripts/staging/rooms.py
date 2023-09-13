from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from dotenv import load_dotenv
from os import getenv
from common import decode_data, connection_string

load_dotenv()

rooms_table = getenv("ROOMS_TABLE")
stg_room_table = getenv("STG_ROOM_TABLE")


def process_rooms(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, rooms_table)

    processed_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .select(["id", "type", "updated_at"])
    )
    (
        processed_data.write.format("jdbc")
        .mode("append")
        .option("url", connection_string)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", stg_room_table)
        .save()
    )