from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from os import getenv
from dimensions import *
from staging import *
import traceback


max_offsets = 20
broker = getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")

location_table = getenv("LOCATION_TABLE")
guests_table = getenv("GUESTS_TABLE")
addons_table = getenv("ADDONS_TABLE")
roomtypes_table = getenv("ROOMTYPES_TABLE")
rooms_table = getenv("ROOMS_TABLE")
bookings_table = getenv("BOOKINGS_TABLE")
booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
booking_addons_table = getenv("BOOKING_ADDONS_TABLE")


def process_guests(df: DataFrame, batch_id: int):
    stage_guest(df, batch_id)
    process_dim_guest(df, batch_id)


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

    booking_rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", booking_rooms_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    (
        booking_rooms.writeStream.option(
            "checkpointLocation", "/tmp/booking_rooms/_checkpoints/"
        )
        .foreachBatch(process_booking_rooms)
        .start()
    )

    booking_addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", booking_addons_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    (
        booking_addons.writeStream.option(
            "checkpointLocation", "/tmp/booking_addons/_checkpoints/"
        )
        .foreachBatch(process_booking_addons)
        .start()
    )

    try:
        spark.streams.awaitAnyTermination()
    except Exception as e:
        traceback.print_exc()
