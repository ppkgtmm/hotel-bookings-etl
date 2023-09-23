from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from os import getenv
from common import decode_data, get_connection_string


guests_table = getenv("GUESTS_TABLE")
dim_guest_table = getenv("DIM_GUEST_TABLE")


def process_guests(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, guests_table)

    processed_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumnRenamed("id", "_id")
        .withColumn("created_at", expr("timestamp_millis(updated_at)"))
        .withColumn("dob", expr("date_from_unix_date(dob)"))
        .select(["_id", "email", "dob", "gender", "created_at"])
    )
    (
        processed_data.write.format("jdbc")
        .mode("append")
        .option("url", get_connection_string())
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", dim_guest_table)
        .save()
    )
