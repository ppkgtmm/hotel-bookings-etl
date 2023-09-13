from pyspark.sql import SparkSession
from dotenv import load_dotenv
from os import getenv

from helper import (
    process_addons,
    process_roomtypes,
    process_rooms,
    process_guests,
    process_bookings,
    # process_booking_rooms,
    # process_booking_addons,
    # tear_down,
)
import traceback

load_dotenv()

max_offsets = 20
# broker = getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")
broker = getenv("KAFKA_BOOTSTRAP_SERVERS")

location_table = getenv("LOCATION_TABLE")
guests_table = getenv("GUESTS_TABLE")
addons_table = getenv("ADDONS_TABLE")
roomtypes_table = getenv("ROOMTYPES_TABLE")
rooms_table = getenv("ROOMS_TABLE")
bookings_table = getenv("BOOKINGS_TABLE")
booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("hotel oltp processor")
        .config("spark.driver.memory", "1g")
        .config(
            "spark.jars",
            "/Users/pinky/Downloads/mysql-connector-java-8.0.13/mysql-connector-java-8.0.13.jar",
        )
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-avro_2.12:3.4.1",
        )  # cr. https://stackoverflow.com/questions/54285151/kafka-structured-streaming-kafkasourceprovider-could-not-be-instantiated
        .getOrCreate()
    )

    addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", addons_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    (
        addons.writeStream.option("checkpointLocation", "/tmp/addons/_checkpoints/")
        .foreachBatch(process_addons)
        .start()
    )

    roomtypes = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", roomtypes_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    (
        roomtypes.writeStream.option(
            "checkpointLocation", "/tmp/roomtypes/_checkpoints/"
        )
        .foreachBatch(process_roomtypes)
        .start()
    )

    rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", rooms_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    (
        rooms.writeStream.option("checkpointLocation", "/tmp/rooms/_checkpoints/")
        .foreachBatch(process_rooms)
        .start()
    )

    guests = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", guests_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    (
        guests.writeStream.option("checkpointLocation", "/tmp/guests/_checkpoints/")
        .foreachBatch(process_guests)
        .start()
    )

    bookings = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", bookings_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    (
        bookings.writeStream.option("checkpointLocation", "/tmp/bookings/_checkpoints/")
        .foreachBatch(process_bookings)
        .start()
    )

    # bookings = (
    #     bookings.withColumn("data", expr("substring(value, 6, length(value) - 5)"))
    #     .withColumn("decoded", from_avro("data", get_schema(bookings_table)))
    #     .filter(expr("coalesce(decoded.before, decoded.after) IS NOT NULL"))
    #     .select(["decoded.before", "decoded.after", "decoded.source.ts_ms"])
    #     .withColumn("id", expr("coalesce(before.id, after.id)"))
    #     .select(["id", "before", "after", expr("timestamp_millis(ts_ms)").alias("ts")])
    #     .withWatermark("ts", "5 minutes")
    # )

    # booking_rooms = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", booking_rooms_table)
    #     .option("startingOffsets", "earliest")
    #     .option("maxOffsetsPerTrigger", max_offsets)
    #     .load()
    # )

    # booking_rooms = (
    #     booking_rooms.withColumn("data", expr("substring(value, 6, length(value) - 5)"))
    #     .withColumn("decoded", from_avro("data", get_schema(booking_rooms_table)))
    #     .filter(expr("coalesce(decoded.before, decoded.after) IS NOT NULL"))
    #     .select(["decoded.before", "decoded.after", "decoded.source.ts_ms"])
    #     .withColumn("id", expr("coalesce(before.id, after.id)"))
    #     .select(["id", "before", "after", expr("timestamp_millis(ts_ms)").alias("ts")])
    #     .withWatermark("ts", "5 minutes")
    # )

    # (
    #     bookings.writeStream.option(
    #         "checkpointLocation", f"/tmp/{bookings_table}/_checkpoints/"
    #     )
    #     .format("console")
    #     .outputMode("append")
    #     .start()
    #     .awaitTermination()
    # )

    # (
    #     booking_rooms.writeStream.option(
    #         "checkpointLocation", f"/tmp/{booking_rooms_table}/_checkpoints/"
    #     )
    #     .format("console")
    #     .outputMode("append")
    #     .start()
    #     .awaitTermination()
    # )

    # .withColumn(
    #     "ts",
    #     expr("timestamp_millis(COALESCE(before.updated_at, after.updated_at))"),
    # )
    # .withWatermark("ts", "5 minutes")
    # bookings_before = (
    #     bookings.where(col("before").isNotNull())
    # .withColumn(
    #     "row_num",
    #     row_number().over(
    #         Window.partitionBy(["window.start", "before.id"]).orderBy("timestamp")
    #     ),
    # )
    # .where(col("row_num") == 1)
    # .select("before")
    # # .withColumn(
    # #     "row_num",
    # #     row_number().over(Window.partitionBy(["before.id"]).orderBy("timestamp")),
    # # )
    # )
    # bookings.createOrReplaceTempView("bookings")
    # bookings = spark.sql(
    #     """
    #     SELECT
    #         COALESCE(before.id, after.id) AS id,

    #     FROM bookings
    #     WHERE COALESCE(before.id, after.id) IS NOT NULL
    #     """
    # )
    # .withColumn("window", window("timestamp", "5 minutes"))
    # (
    #     # bookings.select([expr("COALESCE(before.id, after.id)").alias("id")])
    #     bookings.writeStream.option("checkpointLocation", "/tmp/bookings/_checkpoints/")
    #     .format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("topic", unwrap.format(bookings_table))
    #     .start()
    #     .awaitTermination()
    # )

    # bookings_unwrap = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", unwrap.format(bookings_table))
    #     .option("startingOffsets", "earliest")
    #     .option("maxOffsetsPerTrigger", max_offsets)
    #     .load()
    # )

    # bookings_unwrap = (
    #     bookings_unwrap.withColumn(
    #         "data", expr("SUBSTRING(value, 6, LENGTH(value) - 5)")
    #     )
    #     .withColumn(
    #         "decoded", from_avro("data", get_schema(unwrap.format(bookings_table)))
    #     )
    #     .select(["decoded.id", "decoded.before", "decoded.after", "decoded.ts"])
    # )

    # bookings_after = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", bookings_after_topic)
    #     .option("startingOffsets", "earliest")
    #     .option("maxOffsetsPerTrigger", max_offsets)
    #     .load()
    # )

    # (
    #     bookings_after.writeStream.option(
    #         "checkpointLocation", "/tmp/bookings_after/_checkpoints/"
    #     )
    #     .foreachBatch()
    #     .start()
    # )

    # bookings_after = (
    #     bookings_after.withColumn(
    #         "fixed_value", expr("substring(value, 6, length(value) - 5)")
    #     )
    #     .select(
    #         from_avro(col("fixed_value"), get_schema(bookings_after_topic)).alias(
    #             "decoded"
    #         )
    #     )
    #     .select("decoded.*")
    # )

    # (
    #     bookings_after.writeStream.foreachBatch(
    #         lambda df, batch_id: print(df.collect())
    #     ).start()
    #     # .option(
    #     #     "checkpointLocation", "/tmp/bookings_before/_checkpoints/"
    #     # )
    # )

    # booking_rooms = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", booking_rooms_table)
    #     .option("startingOffsets", "earliest")
    #     # .option("maxOffsetsPerTrigger", 1)
    #     .load()
    # )

    # (
    #     booking_rooms.writeStream.option(
    #         "checkpointLocation", "/tmp/booking_rooms/_checkpoints/"
    #     )
    #     .foreachBatch(process_booking_rooms)
    #     .start()
    # )

    # booking_addons = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", booking_addons_table)
    #     .option("startingOffsets", "earliest")
    #     # .option("maxOffsetsPerTrigger", 1)
    #     .load()
    # )

    # (
    #     booking_addons.writeStream.option(
    #         "checkpointLocation", "/tmp/booking_addons/_checkpoints/"
    #     )
    #     .foreachBatch(process_booking_addons)
    #     .start()
    # )

    try:
        spark.streams.awaitAnyTermination()
    except Exception as e:
        traceback.print_exc()
    # finally:
    #     tear_down()
