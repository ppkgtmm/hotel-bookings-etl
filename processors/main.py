from pyspark.sql import SparkSession

# from pyspark.sql.functions import when, max
from dotenv import load_dotenv
from os import getenv
from helpers import (
    AddonProcessor,
    BookingProcessor,
    BookingAddonProcessor,
    BookingRoomProcessor,
    RoomProcessor,
    RoomTypeProcessor,
    GuestProcessor,
    LocationProcessor,
)

load_dotenv()

OLTP_DB = getenv("OLTP_DB")
BROKER = getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")
LOCATION_TABLE = OLTP_DB + "-" + getenv("LOCATION_TABLE")
GUESTS_TABLE = OLTP_DB + "-" + getenv("GUESTS_TABLE")
ADDONS_TABLE = OLTP_DB + "-" + getenv("ADDONS_TABLE")
ROOMTYPES_TABLE = OLTP_DB + "-" + getenv("ROOMTYPES_TABLE")
ROOMS_TABLE = OLTP_DB + "-" + getenv("ROOMS_TABLE")
BOOKINGS_TABLE = OLTP_DB + "-" + getenv("BOOKINGS_TABLE")
BOOKING_ROOMS_TABLE = OLTP_DB + "-" + getenv("BOOKING_ROOMS_TABLE")
BOOKING_ADDONS_TABLE = OLTP_DB + "-" + getenv("BOOKING_ADDONS_TABLE")

if __name__ == "__main__":
    # location_topic = f"{oltp_db}.{oltp_db}.location"
    # guests_topic = f"{oltp_db}.{oltp_db}.guests"
    # addons_topic = f"{oltp_db}.{oltp_db}.addons"
    # roomtypes_topic = f"{oltp_db}.{oltp_db}.roomtypes"
    # rooms_topic = f"{oltp_db}.{oltp_db}.rooms"
    # bookings_topic = f"{oltp_db}.{oltp_db}.bookings"
    # booking_rooms_topic = f"{oltp_db}.{oltp_db}.booking_rooms"
    # booking_addons_topic = f"{oltp_db}.{oltp_db}.booking_addons"

    spark = (
        SparkSession.builder.master("local")
        .appName("hotel oltp processor")
        .config("spark.driver.memory", "1g")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        )  # cr. https://stackoverflow.com/questions/54285151/kafka-structured-streaming-kafkasourceprovider-could-not-be-instantiated
        .getOrCreate()
    )

    # oltp_data = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribePattern", f"{oltp_db}\\.{oltp_db}.*")
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )
    # # .withWatermark("timestamp", "5 seconds")
    # (
    #     oltp_data.withColumn(
    #         "processing_order",
    #         when(oltp_data.topic == location_topic, 1)
    #         .when(oltp_data.topic == roomtypes_topic, 2)
    #         .when(oltp_data.topic == rooms_topic, 3)
    #         .when(oltp_data.topic == guests_topic, 4)
    #         .when(oltp_data.topic == addons_topic, 5)
    #         .when(oltp_data.topic == bookings_topic, 6)
    #         .when(oltp_data.topic == booking_rooms_topic, 7)
    #         .when(oltp_data.topic == booking_addons_topic, 8)
    #         .otherwise(-1),
    #     )
    #     .groupBy(["topic", "value"])
    #     .agg(max("processing_order").alias("max_processing_order"))
    #     .orderBy("max_processing_order")
    #     .writeStream.outputMode("complete")
    #     .foreach(Processor())
    #     .start()
    #     .awaitTermination()
    # )

    location = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", LOCATION_TABLE)
        .option("startingOffsets", "earliest")
        .load()
    )

    location.writeStream.foreach(LocationProcessor()).start()

    guests = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", GUESTS_TABLE)
        .option("startingOffsets", "earliest")
        .load()
    )

    guests.writeStream.foreach(GuestProcessor()).start()

    addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", ADDONS_TABLE)
        .option("startingOffsets", "earliest")
        .load()
    )

    addons.writeStream.foreach(AddonProcessor()).start()

    roomtypes = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", ROOMTYPES_TABLE)
        .option("startingOffsets", "earliest")
        .load()
    )
    roomtypes.writeStream.foreach(RoomTypeProcessor()).start()

    rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", ROOMS_TABLE)
        .option("startingOffsets", "earliest")
        .load()
    )

    rooms.writeStream.foreach(RoomProcessor()).start()

    bookings = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribePattern", BOOKINGS_TABLE)
        .option("startingOffsets", "earliest")
        .load()
    )
    bookings.where('topic == "bookings"').writeStream.foreach(
        BookingProcessor()
    ).start()

    booking_rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", BOOKING_ROOMS_TABLE)
        .option("startingOffsets", "earliest")
        .load()
    )
    booking_rooms.writeStream.foreach(BookingRoomProcessor()).start()

    booking_addons = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", BOOKING_ADDONS_TABLE)
        .option("startingOffsets", "earliest")
        .load()
    )
    booking_addons.writeStream.foreach(BookingAddonProcessor()).start()

    spark.streams.awaitAnyTermination()
