from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from dotenv import load_dotenv
from os import getenv


# .sparkSession.sql(
#     f"""
#         MERGE INTO {del_booking_table} dst
#         USING deleted_bookings src
#         ON dst.id = src.id
#         WHEN matched then UPDATE set checkin=t.checkin, checkout=t.checkout, updated_at=t.updated_at
#         when not matched then
#     """
# )
# to_list(deleted_data)
# processed_data.createGlobalTempView("upserted_bookings")
# deleted_data.sparkSession.sql(
#     f"""
#         CREATE TABLE IF NOT EXISTS {stg_booking_table}
#         USING JDBC
#         OPTIONS (
#             url '{connection_string}'
#         );

#         INSERT INTO {stg_booking_table}
#         SELECT * FROM upserted_bookings t
#         ON DUPLICATE KEY
#         UPDATE checkin=t.checkin, checkout=t.checkout, updated_at=t.updated_at;
#     """
# )
# to_list(processed_data)


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
