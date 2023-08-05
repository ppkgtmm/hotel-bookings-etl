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

MAX_OFFSETS = 100

OLTP_DB = getenv("OLTP_DB")
BROKER = getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")
# BROKER = getenv("KAFKA_BOOTSTRAP_SERVERS")

LOCATION_TABLE = getenv("LOCATION_TABLE")
GUESTS_TABLE = getenv("GUESTS_TABLE")
ADDONS_TABLE = getenv("ADDONS_TABLE")
ROOMTYPES_TABLE = getenv("ROOMTYPES_TABLE")
ROOMS_TABLE = getenv("ROOMS_TABLE")
BOOKINGS_TABLE = getenv("BOOKINGS_TABLE")
BOOKING_ROOMS_TABLE = getenv("BOOKING_ROOMS_TABLE")
BOOKING_ADDONS_TABLE = getenv("BOOKING_ADDONS_TABLE")

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
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", LOCATION_TABLE)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )

    location.writeStream.option(
        "checkpointLocation", "/tmp/location/_checkpoints/"
    ).foreachBatch(process_locations).start()

    addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", ADDONS_TABLE)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )

    addons.writeStream.option(
        "checkpointLocation", "/tmp/addons/_checkpoints/"
    ).foreachBatch(process_addons).start()

    roomtypes = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", ROOMTYPES_TABLE)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )

    roomtypes.writeStream.option(
        "checkpointLocation", "/tmp/roomtypes/_checkpoints/"
    ).foreachBatch(process_roomtypes).start()

    rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", ROOMS_TABLE)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )

    rooms.writeStream.option(
        "checkpointLocation", "/tmp/rooms/_checkpoints/"
    ).foreachBatch(process_rooms).start()

    guests = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", GUESTS_TABLE)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )

    guests.writeStream.option(
        "checkpointLocation", "/tmp/guests/_checkpoints/"
    ).foreachBatch(process_guests).start()

    bookings = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribePattern", BOOKINGS_TABLE)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )

    bookings.writeStream.option(
        "checkpointLocation", "/tmp/bookings/_checkpoints/"
    ).foreachBatch(process_bookings).start()

    booking_rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", BOOKING_ROOMS_TABLE)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1)
        .load()
    )

    booking_rooms.writeStream.option(
        "checkpointLocation", "/tmp/booking_rooms/_checkpoints/"
    ).foreachBatch(process_booking_rooms).start()

    booking_addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", BOOKING_ADDONS_TABLE)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50)
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
