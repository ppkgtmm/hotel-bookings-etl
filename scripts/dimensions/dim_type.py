from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from os import getenv
from common import decode_data, get_connection_string


roomtypes_table = getenv("ROOMTYPES_TABLE")
dim_roomtype_table = getenv("DIM_ROOMTYPE_TABLE")


def process_roomtypes(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, roomtypes_table)

    processed_data = (
        data.filter(expr("after IS NOT NULL AND after.deleted_at IS NULL"))
        .select("after.*")
        .withColumnRenamed("id", "_id")
        .withColumn("created_at", expr("timestamp_millis(updated_at)"))
        .select(["_id", "name", "price", "created_at"])
    )
    (
        processed_data.write.format("jdbc")
        .mode("append")
        .option("url", get_connection_string())
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", dim_roomtype_table)
        .save()
    )
