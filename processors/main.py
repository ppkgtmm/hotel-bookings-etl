from pyspark.sql import SparkSession
from pyspark.sql.functions import when, max
from dotenv import load_dotenv
from os import getenv
from helpers import Processor

load_dotenv()

oltp_db = getenv("OLTP_DB")
broker = getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")

if __name__ == "__main__":
    location_topic = f"{oltp_db}.{oltp_db}.location"
    guests_topic = f"{oltp_db}.{oltp_db}.guests"
    addons_topic = f"{oltp_db}.{oltp_db}.addons"
    roomtypes_topic = f"{oltp_db}.{oltp_db}.roomtypes"
    rooms_topic = f"{oltp_db}.{oltp_db}.rooms"
    bookings_topic = f"{oltp_db}.{oltp_db}.bookings"
    booking_rooms_topic = f"{oltp_db}.{oltp_db}.booking_rooms"
    booking_addons_topic = f"{oltp_db}.{oltp_db}.booking_addons"

    spark = (
        SparkSession.builder.master("local")
        .appName("hotel oltp processor")
        .config("spark.driver.memory", "1g")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        )  # cr. https://stackoverflow.com/questions/54285151/kafka-structured-streaming-kafkasourceprovider-could-not-be-instantiated
        .getOrCreate()
    )

    oltp_data = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribePattern", f"{oltp_db}\\.{oltp_db}.*")
        .option("startingOffsets", "earliest")
        .load()
    )
    # .withWatermark("timestamp", "5 seconds")
    (
        oltp_data.withColumn(
            "processing_order",
            when(oltp_data.topic == location_topic, 1)
            .when(oltp_data.topic == roomtypes_topic, 2)
            .when(oltp_data.topic == rooms_topic, 3)
            .when(oltp_data.topic == guests_topic, 4)
            .when(oltp_data.topic == addons_topic, 5)
            .when(oltp_data.topic == bookings_topic, 6)
            .when(oltp_data.topic == booking_rooms_topic, 7)
            .when(oltp_data.topic == booking_addons_topic, 8)
            .otherwise(-1),
        )
        .groupBy(["topic", "value"])
        .agg(max("processing_order").alias("max_processing_order"))
        .orderBy("max_processing_order")
        .writeStream.outputMode("complete")
        .foreach(Processor())
        .start()
        .awaitTermination()
    )

    # location = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", "location")
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )

    # location.writeStream.foreach(LocationProcessingHelper()).start()

    # guests = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", "guests")
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )

    # guests.writeStream.foreach(GuestProcessingHelper()).start()

    # addons = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", "addons")
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )

    # addons.writeStream.foreach(AddonProcessingHelper()).start()
    # roomtypes = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", "roomtypes")
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )
    # roomtypes.writeStream.foreach(RoomTypeProcessingHelper()).start()

    # rooms = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribe", "rooms")
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )

    # rooms.writeStream.foreach(RoomProcessingHelper()).start()

    # bookings = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", broker)
    #     .option("subscribePattern", "booking.*")
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )
    # bookings.where('topic == "bookings"').writeStream.foreach(
    #     BookingProcessingHelper()
    # ).start()

    # bookings.where('topic == "booking_rooms"').writeStream.foreach(
    #     BookingRoomProcessingHelper()
    # ).start()

    # bookings.where('topic == "booking_addons"').writeStream.foreach(
    #     BookingAddonProcessingHelper()
    # ).start()

    # # booking_rooms = (
    # #     spark.readStream.format("kafka")
    # #     .option("kafka.bootstrap.servers", broker)
    # #     .option("subscribe", "booking_rooms")
    # #     .option("startingOffsets", "earliest")
    # #     .load()
    # # )
    # # booking_rooms.writeStream.foreach(BookingRoomProcessingHelper()).start()

    # # booking_addons = (
    # #     spark.readStream.format("kafka")
    # #     .option("kafka.bootstrap.servers", broker)
    # #     .option("subscribe", "booking_addons")
    # #     .option("startingOffsets", "earliest")
    # #     .load()
    # # )
    # # booking_addons.writeStream.foreach(BookingAddonProcessingHelper()).start()

    # spark.streams.awaitAnyTermination()
