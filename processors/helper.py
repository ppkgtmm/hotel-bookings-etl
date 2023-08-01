from dotenv import load_dotenv
from os import getenv
from pyspark.sql.types import StringType, MapType
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

load_dotenv()
db_host = getenv("DB_HOST_INTERNAL")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")

connection_string = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
    db_user, db_password, db_host, db_port, db_name
)
engine = create_engine(connection_string)
conn = engine.connect()
addon_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("price", FloatType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
roomtype_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("price", FloatType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
location_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
json_schema = MapType(StringType(), StringType())


def write_addons(row: Row):
    payload = row.asDict()
    query = """
                INSERT INTO dim_addon (_id, name, price, created_at)
                VALUES (:id, :name, :price, :updated_at)
            """
    conn.execute(text(query), payload)
    conn.commit()


def write_roomtypes(row: Row):
    payload = row.asDict()
    query = """
                INSERT INTO dim_roomtype (_id, name, price, created_at)
                VALUES (:id, :name, :price, :updated_at)
            """
    conn.execute(text(query), payload)
    conn.commit()


def write_locations(row: Row):
    payload = row.asDict()
    query = """
                INSERT INTO dim_location (id, state, country)
                VALUES (:id, :state, :country)
                ON DUPLICATE KEY UPDATE
                    state=:state,
                    country=:country
            """
    conn.execute(text(query), payload)
    conn.commit()


def process_addons(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", addon_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.name",
                "data.price",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    data.foreach(write_addons)


def process_roomtypes(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", roomtype_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.name",
                "data.price",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    data.foreach(write_roomtypes)


def process_locations(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", location_schema))
        .filter("data IS NOT NULL")
        .select(["data.id", "data.state", "data.country"])
    )
    data.foreach(write_locations)
