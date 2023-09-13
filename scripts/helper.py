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
