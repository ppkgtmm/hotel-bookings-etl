from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from dotenv import load_dotenv
from os import getenv
from common import decode_data, get_connection_string

load_dotenv()

guest_table = getenv("GUESTS_TABLE")
raw_guest_table = getenv("RAW_GUEST_TABLE")


def process_guests(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, guest_table)

    processed_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumn("updated_at", expr("timestamp_millis(updated_at)"))
        .select(["id", "state", "country", "updated_at"])
    )
    (
        processed_data.write.format("jdbc")
        .mode("append")
        .option("url", get_connection_string())
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", raw_guest_table)
        .save()
    )
