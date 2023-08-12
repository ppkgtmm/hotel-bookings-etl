from pyspark.sql.types import (
    MapType,
    IntegerType,
    LongType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    timestamp_seconds,
    date_add,
    to_date,
    lit,
)
from db_writer import DatabaseWriter

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
room_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("floor", IntegerType()),
        StructField("number", IntegerType()),
        StructField("type", IntegerType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
guest_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("firstname", StringType()),
        StructField("lastname", StringType()),
        StructField("gender", StringType()),
        StructField("email", StringType()),
        StructField("dob", IntegerType()),
        StructField("location", IntegerType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
booking_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("user", IntegerType()),
        StructField("checkin", IntegerType()),
        StructField("checkout", IntegerType()),
        StructField("payment", LongType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
booking_room_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("booking", IntegerType()),
        StructField("room", IntegerType()),
        StructField("guest", IntegerType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
booking_addon_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("booking_room", IntegerType()),
        StructField("addon", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("datetime", LongType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
json_schema = MapType(StringType(), StringType())
db_writer = DatabaseWriter()


def df_to_list(df: DataFrame):
    return [row.asDict() for row in df.collect()]


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
                col("data.id").alias("_id"),
                "data.name",
                "data.price",
                timestamp_seconds(col("data.updated_at") / 1000).alias("created_at"),
            ]
        )
    )
    rows = df_to_list(data)
    db_writer.write_dim_addons(rows)


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
                col("data.id").alias("_id"),
                "data.name",
                "data.price",
                timestamp_seconds(col("data.updated_at") / 1000).alias("created_at"),
            ]
        )
    )
    rows = df_to_list(data)
    db_writer.write_dim_roomtypes(rows)


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

    rows = df_to_list(data)
    db_writer.write_dim_locations(rows)


def process_rooms(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", room_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.type",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    rows = df_to_list(data)
    db_writer.stage_rooms(rows)


def process_guests(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", guest_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.email",
                date_add(
                    to_date(lit("1970-01-01"), "yyyy-MM-dd"), col("data.dob")
                ).alias("dob"),
                "data.gender",
                "data.location",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    rows = df_to_list(data)
    db_writer.stage_guests(rows)
    rows = df_to_list(data.select(["id", "email", "dob", "gender"]))
    db_writer.write_dim_guests(rows)


def transform_bookings(bookings_df: DataFrame, key: str):
    return (
        bookings_df.withColumn("data", from_json(key, booking_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                date_add(
                    to_date(lit("1970-01-01"), "yyyy-MM-dd"), col("data.checkin")
                ).alias("checkin"),
                date_add(
                    to_date(lit("1970-01-01"), "yyyy-MM-dd"), col("data.checkout")
                ).alias("checkout"),
            ]
        )
    )


def process_bookings(micro_batch_df: DataFrame, batch_id: int):
    payload: DataFrame = micro_batch_df.withColumn(
        "message", from_json(col("value").cast(StringType()), json_schema)
    ).withColumn("payload", from_json("message.payload", json_schema))
    before: DataFrame = transform_bookings(payload, "payload.before")
    rows_before = df_to_list(before)
    db_writer.del_bookings(rows_before)
    after: DataFrame = transform_bookings(payload, "payload.after")
    rows_after = df_to_list(after)
    db_writer.stage_bookings(rows_after)


def process_booking_rooms(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", booking_room_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.booking",
                "data.room",
                "data.guest",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    rows = df_to_list(data)
    db_writer.stage_booking_rooms(rows)
    db_writer.write_fct_bookings()


def process_booking_addons(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", booking_addon_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.booking_room",
                "data.addon",
                "data.quantity",
                timestamp_seconds(col("data.datetime") / 1000).alias("datetime"),
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    rows = df_to_list(data)
    db_writer.stage_booking_addons(rows)
    db_writer.write_fct_purchases()
