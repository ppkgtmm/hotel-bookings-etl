from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from dotenv import load_dotenv
from os import getenv

load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")
addons_table = getenv("ADDONS_TABLE")
roomtypes_table = getenv("ROOMTYPES_TABLE")
dim_addon_table = getenv("DIM_ADDON_TABLE")
dim_roomtype_table = getenv("DIM_ROOMTYPE_TABLE")
registry_url = getenv("SCHEMA_REGISTRY_URL")

connection_string = "jdbc:mysql://{}:{}@{}:{}/{}?useSSL=false".format(
    db_user, db_password, db_host, db_port, db_name
)

registry_client = SchemaRegistryClient({"url": registry_url})


def get_avro_schema(topic):
    return registry_client.get_latest_version(topic + "-value").schema.schema_str


def to_list(df: DataFrame):
    return [row.asDict() for row in df.collect()]


def decode_data(df: DataFrame, topic: str):
    return (
        df.withColumn("data", expr("substring(value, 6, length(value) - 5)"))
        .withColumn("decoded", from_avro("data", get_avro_schema(topic)))
        .select("decoded.*")
    )


def process_addons(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, addons_table)

    processed_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumnRenamed("id", "_id")
        .withColumn("created_at", expr("timestamp_millis(updated_at)"))
        .select(["_id", "name", "price", "created_at"])
    )
    (
        processed_data.write.format("jdbc")
        .mode("append")
        .option("url", connection_string)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", dim_addon_table)
        .save()
    )


def process_roomtypes(df: DataFrame, batch_id: int):
    data: DataFrame = decode_data(df, roomtypes_table)

    processed_data = (
        data.filter(expr("after IS NOT NULL"))
        .select("after.*")
        .withColumnRenamed("id", "_id")
        .withColumn("created_at", expr("timestamp_millis(updated_at)"))
        .select(["_id", "name", "price", "created_at"])
    )
    (
        processed_data.write.format("jdbc")
        .mode("append")
        .option("url", connection_string)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", dim_roomtype_table)
        .save()
    )


# def process_locations(micro_batch_df: DataFrame, batch_id: int):
#     data: DataFrame = (
#         micro_batch_df.withColumn(
#             "message", from_json(col("value").cast(StringType()), json_schema)
#         )
#         .withColumn("payload", from_json("message.payload", json_schema))
#         .withColumn("data", from_json("payload.after", location_schema))
#         .filter("data IS NOT NULL")
#         .select(["data.id", "data.state", "data.country"])
#     )

#     rows = df_to_list(data)
#     if rows != []:
#         db_writer.write_dim_locations(rows)


# def process_rooms(micro_batch_df: DataFrame, batch_id: int):
#     data: DataFrame = (
#         micro_batch_df.withColumn(
#             "message", from_json(col("value").cast(StringType()), json_schema)
#         )
#         .withColumn("payload", from_json("message.payload", json_schema))
#         .withColumn("data", from_json("payload.after", room_schema))
#         .filter("data IS NOT NULL")
#         .select(
#             [
#                 "data.id",
#                 "data.type",
#                 timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
#             ]
#         )
#     )
#     rows = df_to_list(data)
#     if rows != []:
#         db_writer.stage_rooms(rows)


# def process_guests(micro_batch_df: DataFrame, batch_id: int):
#     data: DataFrame = (
#         micro_batch_df.withColumn(
#             "message", from_json(col("value").cast(StringType()), json_schema)
#         )
#         .withColumn("payload", from_json("message.payload", json_schema))
#         .withColumn("data", from_json("payload.after", guest_schema))
#         .filter("data IS NOT NULL")
#         .select(
#             [
#                 "data.id",
#                 "data.email",
#                 date_add(
#                     to_date(lit("1970-01-01"), "yyyy-MM-dd"), col("data.dob")
#                 ).alias("dob"),
#                 "data.gender",
#                 "data.location",
#                 timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
#             ]
#         )
#     )
#     rows = df_to_list(data)
#     if rows != []:
#         db_writer.stage_guests(rows)
#         rows = df_to_list(data.select(["id", "email", "dob", "gender"]))
#         db_writer.write_dim_guests(rows)


# def transform_bookings(bookings_df: DataFrame, key: str):
#     return (
#         bookings_df.withColumn("data", from_json(key, booking_schema))
#         .filter("data IS NOT NULL")
#         .select(
#             [
#                 "data.id",
#                 date_add(
#                     to_date(lit("1970-01-01"), "yyyy-MM-dd"), col("data.checkin")
#                 ).alias("checkin"),
#                 date_add(
#                     to_date(lit("1970-01-01"), "yyyy-MM-dd"), col("data.checkout")
#                 ).alias("checkout"),
#             ]
#         )
#     )


# def process_bookings(micro_batch_df: DataFrame, batch_id: int):
#     payload: DataFrame = micro_batch_df.withColumn(
#         "message", from_json(col("value").cast(StringType()), json_schema)
#     ).withColumn("payload", from_json("message.payload", json_schema))
#     before: DataFrame = transform_bookings(payload, "payload.before")
#     rows_before = df_to_list(before)
#     if rows_before != []:
#         db_writer.del_bookings(rows_before)
#     after: DataFrame = transform_bookings(payload, "payload.after")
#     rows_after = df_to_list(after)
#     if rows_after != []:
#         db_writer.stage_bookings(rows_after)


# def transform_booking_rooms(booking_rooms_df: DataFrame, key: str):
#     return (
#         booking_rooms_df.withColumn("data", from_json(key, booking_room_schema))
#         .filter("data IS NOT NULL")
#         .select(
#             [
#                 "data.id",
#                 "data.booking",
#                 "data.room",
#                 "data.guest",
#                 timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
#             ]
#         )
#     )


# def process_booking_rooms(micro_batch_df: DataFrame, batch_id: int):
#     payload: DataFrame = micro_batch_df.withColumn(
#         "message", from_json(col("value").cast(StringType()), json_schema)
#     ).withColumn("payload", from_json("message.payload", json_schema))
#     before: DataFrame = transform_booking_rooms(payload, "payload.before")
#     rows_before = df_to_list(before)
#     if rows_before != []:
#         db_writer.del_booking_rooms(rows_before)
#         db_writer.remove_fct_bookings()
#     after: DataFrame = transform_booking_rooms(payload, "payload.after")
#     rows_after = df_to_list(after)
#     if rows_after != []:
#         db_writer.stage_booking_rooms(rows_after)
#         db_writer.write_fct_bookings()


# def transform_booking_addons(booking_addons_df: DataFrame, key: str):
#     return (
#         booking_addons_df.withColumn("data", from_json(key, booking_addon_schema))
#         .filter("data IS NOT NULL")
#         .select(
#             [
#                 "data.id",
#                 "data.booking_room",
#                 "data.addon",
#                 "data.quantity",
#                 timestamp_seconds(col("data.datetime") / 1000).alias("datetime"),
#                 timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
#             ]
#         )
#     )


# def process_booking_addons(micro_batch_df: DataFrame, batch_id: int):
#     payload: DataFrame = micro_batch_df.withColumn(
#         "message", from_json(col("value").cast(StringType()), json_schema)
#     ).withColumn("payload", from_json("message.payload", json_schema))
#     before: DataFrame = transform_booking_addons(payload, "payload.before")
#     rows_before = df_to_list(before)
#     if rows_before != []:
#         db_writer.del_booking_addons(rows_before)
#         db_writer.remove_fct_purchases()
#     after: DataFrame = transform_booking_addons(payload, "payload.after")
#     rows_after = df_to_list(after)
#     if rows_after != []:
#         db_writer.stage_booking_addons(rows_after)
#         db_writer.write_fct_purchases()


# def tear_down():
#     db_writer.tear_down()
