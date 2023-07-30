from pyspark.sql import SparkSession
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
        .option("subscribe", f"{oltp_db}.{oltp_db}.location")
        .option("startingOffsets", "earliest")
        .load()
    )

    location.writeStream.foreach(LocationProcessor()).start()

    guests = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", f"{oltp_db}.{oltp_db}.guests")
        .option("startingOffsets", "earliest")
        .load()
    )

    guests.writeStream.foreach(GuestProcessor()).start()

    addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", f"{oltp_db}.{oltp_db}.addons")
        .option("startingOffsets", "earliest")
        .load()
    )

    addons.writeStream.foreach(AddonProcessor()).start()
    roomtypes = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", f"{oltp_db}.{oltp_db}.roomtypes")
        .option("startingOffsets", "earliest")
        .load()
    )
    roomtypes.writeStream.foreach(RoomTypeProcessor()).start()

    rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", f"{oltp_db}.{oltp_db}.rooms")
        .option("startingOffsets", "earliest")
        .load()
    )

    rooms.writeStream.foreach(RoomProcessor()).start()

    bookings = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", f"{oltp_db}.{oltp_db}.bookings")
        .option("startingOffsets", "earliest")
        .load()
    )
    bookings.writeStream.foreach(BookingProcessor()).start()

    booking_rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", f"{oltp_db}.{oltp_db}.booking_rooms")
        .option("startingOffsets", "earliest")
        .load()
    )
    booking_rooms = booking_rooms.withWatermark("timestamp", "5 minutes")
    booking_rooms.writeStream.foreach(BookingRoomProcessor()).start()

    spark.streams.awaitAnyTermination()
