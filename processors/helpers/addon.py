from helpers.helper import ProcessingHelper
from sqlalchemy import create_engine, text
from pyspark.sql.types import (
    IntegerType,
    LongType,
    FloatType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import from_json, col, timestamp_seconds


class AddonProcessor(ProcessingHelper):
    def __init__(self) -> None:
        super().__init__()
        self.engine = create_engine(self.connection_string)
        self.conn = self.engine.connect()
        self.schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("price", FloatType()),
                StructField("created_at", LongType()),
                StructField("updated_at", LongType()),
            ]
        )

    def insert_db(self, row: Row):
        payload = row.asDict()
        query = f"""
                    INSERT INTO dim_addon (_id, name, price, created_at)
                    VALUES (:id, :name, :price, :updated_at)
                """
        self.conn.execute(text(query), payload)
        self.conn.commit()

    def process_batch(self, micro_batch_df: DataFrame, batch_id: int):
        data: DataFrame = (
            micro_batch_df.withColumn(
                "message", from_json(col("value").cast(StringType()), self.json_schema)
            )
            .withColumn("payload", from_json("message.payload", self.json_schema))
            .withColumn("data", from_json("payload.after", self.schema))
            .filter("data IS NOT NULL")
            .select(
                [
                    "data.id",
                    "data.name",
                    "data.price",
                    timestamp_seconds(col("data.updated_at") / 1000).alias(
                        "updated_at"
                    ),
                ]
            )
        )
        data.foreach(self.insert_db)

    def tear_down(self):
        self.conn.close()
        self.engine.dispose()
