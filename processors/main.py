from pyspark.sql import SparkSession

from pyspark.sql.functions import current_timestamp, from_json, col, timestamp_seconds
from pyspark.sql.types import (
    IntegerType,
    LongType,
    FloatType,
    StringType,
    StructField,
    StructType,
    MapType,
)
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
# BROKER = getenv("KAFKA_BOOTSTRAP_SERVERS")

LOCATION_TABLE = getenv("LOCATION_TABLE")
GUESTS_TABLE = getenv("GUESTS_TABLE")
ADDONS_TABLE = getenv("ADDONS_TABLE")
ROOMTYPES_TABLE = getenv("ROOMTYPES_TABLE")
ROOMS_TABLE = getenv("ROOMS_TABLE")
BOOKINGS_TABLE = getenv("BOOKINGS_TABLE")
BOOKING_ROOMS_TABLE = getenv("BOOKING_ROOMS_TABLE")
BOOKING_ADDONS_TABLE = getenv("BOOKING_ADDONS_TABLE")


br_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("booking", IntegerType()),
        StructField("room", IntegerType()),
        StructField("guest", IntegerType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)

ba_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("booking_room", IntegerType()),
        StructField("addon", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("datetime", LongType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)

g_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("firstname", StringType()),
        StructField("lastname", StringType()),
        StructField("gender", StringType()),
        StructField("email", StringType()),
        StructField("dob", IntegerType()),
        StructField("location", IntegerType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)

r_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("floor", IntegerType()),
        StructField("number", IntegerType()),
        StructField("type", IntegerType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)

rt_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("price", FloatType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
b_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("user", IntegerType()),
        StructField("checkin", IntegerType()),
        StructField("checkout", IntegerType()),
        StructField("payment", LongType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
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

    # location = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", BROKER)
    #     .option("subscribe", LOCATION_TABLE)
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )

    # location.writeStream.foreach(LocationProcessor()).start()

    # guests = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", BROKER)
    #     .option("subscribe", GUESTS_TABLE)
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )

    # guests.writeStream.foreach(GuestProcessor()).start()

    # addons = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", BROKER)
    #     .option("subscribe", ADDONS_TABLE)
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )

    # addons.writeStream.foreach(AddonProcessor()).start()

    # roomtypes = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", BROKER)
    #     .option("subscribe", ROOMTYPES_TABLE)
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )
    # roomtypes.writeStream.foreach(RoomTypeProcessor()).start()

    # rooms = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", BROKER)
    #     .option("subscribe", ROOMS_TABLE)
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )

    # rooms.writeStream.foreach(RoomProcessor()).start()

    # bookings = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", BROKER)
    #     .option("subscribePattern", BOOKINGS_TABLE)
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )
    # bookings.writeStream.foreach(BookingProcessor()).start()

    booking_rooms = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", BOOKING_ROOMS_TABLE)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10)
        .load()
        .withColumn(
            "message",
            from_json(col("value").cast("string"), MapType(StringType(), StringType())),
        )
        .withColumn(
            "payload",
            from_json(col("message.payload"), MapType(StringType(), StringType())),
        )
        .withColumn("data", from_json(col("payload.after"), br_schema))
        .withColumn("time_s", col("data.updated_at") / 1000)
        .select("data", timestamp_seconds("time_s").alias("time"))
    )

    booking_rooms.withWatermark("time", "5 seconds").writeStream.format(
        "console"
    ).outputMode("append").option("truncate", "false").start().awaitTermination()

    # booking_rooms.writeStream.foreach(BookingRoomProcessor()).start()

    # booking_addons = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", BROKER)
    #     .option("subscribe", BOOKING_ADDONS_TABLE)
    #     .option("startingOffsets", "earliest")
    #     .load()
    # )
    # booking_addons.writeStream.foreach(BookingAddonProcessor()).start()

    # spark.streams.awaitAnyTermination()
