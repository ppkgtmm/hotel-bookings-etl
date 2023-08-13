from pyspark.sql import SparkSession
from dotenv import load_dotenv
from os import getenv
from helper import (
    process_addons,
    process_roomtypes,
    process_locations,
    process_rooms,
    process_guests,
    process_bookings,
    process_booking_rooms,
    process_booking_addons,
)
from db_writer import DatabaseWriter
import traceback

load_dotenv()

max_offsets = 100

broker = getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")
# broker = getenv("KAFKA_BOOTSTRAP_SERVERS")

location_table = getenv("LOCATION_TABLE")
guests_table = getenv("GUESTS_TABLE")
addons_table = getenv("ADDONS_TABLE")
roomtypes_table = getenv("ROOMTYPES_TABLE")
rooms_table = getenv("ROOMS_TABLE")
bookings_table = getenv("BOOKINGS_TABLE")
booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
booking_addons_table = getenv("BOOKING_ADDONS_TABLE")

if __name__ == "__main__":
    db_writer = DatabaseWriter()
    spark = (
        SparkSession.builder.appName("hotel oltp processor")
        .config("spark.driver.memory", "1g")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        )  # cr. https://stackoverflow.com/questions/54285151/kafka-structured-streaming-kafkasourceprovider-could-not-be-instantiated
        .getOrCreate()
    )

    location = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", location_table)
        .option("startingOffsets", "earliest")
        # .option("maxOffsetsPerTrigger", max_offsets * 2)
        .load()
    )

    location.writeStream.option(
        "checkpointLocation", "/tmp/location/_checkpoints/"
    ).foreachBatch(process_locations).start()

    addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", addons_table)
        .option("startingOffsets", "earliest")
        # .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    addons.writeStream.option(
        "checkpointLocation", "/tmp/addons/_checkpoints/"
    ).foreachBatch(process_addons).start()

    roomtypes = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", roomtypes_table)
        .option("startingOffsets", "earliest")
        # .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    roomtypes.writeStream.option(
        "checkpointLocation", "/tmp/roomtypes/_checkpoints/"
    ).foreachBatch(process_roomtypes).start()

    rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", rooms_table)
        .option("startingOffsets", "earliest")
        # .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    rooms.writeStream.option(
        "checkpointLocation", "/tmp/rooms/_checkpoints/"
    ).foreachBatch(process_rooms).start()

    guests = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", guests_table)
        .option("startingOffsets", "earliest")
        # .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    guests.writeStream.option(
        "checkpointLocation", "/tmp/guests/_checkpoints/"
    ).foreachBatch(process_guests).start()

    bookings = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribePattern", bookings_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1)
        .load()
    )

    bookings.writeStream.option(
        "checkpointLocation", "/tmp/bookings/_checkpoints/"
    ).foreachBatch(process_bookings).start()

    booking_rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", booking_rooms_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1)
        .load()
    )

    booking_rooms.writeStream.option(
        "checkpointLocation", "/tmp/booking_rooms/_checkpoints/"
    ).foreachBatch(process_booking_rooms).start()

    booking_addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", booking_addons_table)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1)
        .load()
    )
    booking_addons.writeStream.option(
        "checkpointLocation", "/tmp/booking_addons/_checkpoints/"
    ).foreachBatch(process_booking_addons).start()

    try:
        spark.streams.awaitAnyTermination()
    except Exception as e:
        traceback.print_exc()
    finally:
        db_writer.tear_down()
