from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from dotenv import load_dotenv
from os import getenv
from helpers import (
    LocationProcessor,
    GuestProcessor,
    AddonProcessor,
    RoomTypeProcessor,
    RoomProcessor,
    BookingProcessor,
    BookingRoomProcessor,
    BookingAddonProcessor,
)

load_dotenv()

oltp_db = getenv("OLTP_DB")
broker = getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local")
        .appName("hotel oltp processor")
        .config("spark.driver.memory", "1g")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        )  # cr. https://stackoverflow.com/questions/54285151/kafka-structured-streaming-kafkasourceprovider-could-not-be-instantiated
        .getOrCreate()
    )

    location = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", "location")
        .option("startingOffsets", "earliest")
        .load()
    )

    location.writeStream.foreach(LocationProcessor()).start()

    guests = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", "guests")
        .option("startingOffsets", "earliest")
        .load()
    )

    guests.writeStream.foreach(GuestProcessor()).start()

    addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", "addons")
        .option("startingOffsets", "earliest")
        .load()
    )

    addons.writeStream.foreach(AddonProcessor()).start()
    roomtypes = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", "roomtypes")
        .option("startingOffsets", "earliest")
        .load()
    )
    roomtypes.writeStream.foreach(RoomTypeProcessor()).start()

    rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", "rooms")
        .option("startingOffsets", "earliest")
        .load()
    )

    rooms.writeStream.foreach(RoomProcessor()).start()

    bookings = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", "bookings")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 20)
        .load()
    )
    bookings.writeStream.foreach(BookingProcessor()).start()

    booking_rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", "booking_rooms")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10)
        .load()
    )
    booking_rooms = booking_rooms.withColumn(
        "current_timestamp", current_timestamp()
    ).withWatermark("current_timestamp", "30 seconds")
    booking_rooms.writeStream.foreach(BookingRoomProcessor()).start()

    booking_addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", "booking_addons")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10)
        .load()
    )
    booking_addons = booking_addons.withColumn(
        "current_timestamp", current_timestamp()
    ).withWatermark("current_timestamp", "2 minutes")
    booking_addons.writeStream.foreach(BookingAddonProcessor()).start()

    spark.streams.awaitAnyTermination()
