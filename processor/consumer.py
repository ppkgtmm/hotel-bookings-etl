from pyspark.sql import SparkSession
from dotenv import load_dotenv
from os import getenv, environ

load_dotenv()

oltp_db = getenv("OLTP_DB")

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
        .option("kafka.bootstrap.servers", "host.docker.internal:9092")
        .option("subscribe", f"{oltp_db}.{oltp_db}.*")
        .option("startingOffsets", "earliest")
        .load()
    )

    query = (
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream.format("console")
        .start()
    )

    query.awaitTermination()
