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

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribePattern", f"{oltp_db}\.{oltp_db}\..*")
        .option("startingOffsets", "latest")
        .load()
    )
    # cr. https://stackoverflow.com/questions/67181848/how-to-make-spark-streams-execute-sequentially
    writer = df.writeStream.foreach(LocationProcessor()).processAllAvailable()
    writer = df.writeStream.foreach(GuestProcessor()).processAllAvailable()
    writer = df.writeStream.foreach(AddonProcessor()).processAllAvailable()
    writer = df.writeStream.foreach(RoomTypeProcessor()).processAllAvailable()
    writer = df.writeStream.foreach(RoomProcessor()).processAllAvailable()
    writer = df.writeStream.foreach(BookingProcessor()).processAllAvailable()
    writer = df.writeStream.foreach(BookingRoomProcessor()).processAllAvailable()

    writer.awaitTermination()
