from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from os import getenv


driver = "mysql+mysqlconnector://"
jdbc_driver = "jdbc:mysql://"

db_host = getenv("DB_HOST_INTERNAL")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("DWH_DB")

schema_registry_url = getenv("SCHEMA_REGISTRY_URL_INTERNAL")

schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})


def get_connection_string(jdbc: bool = True):
    template = "{}:{}@{}:{}/{}"
    template = template.format(db_user, db_password, db_host, db_port, db_name)
    if jdbc:
        return jdbc_driver + template + "?useSSL=false"
    return driver + template


def get_avro_schema(topic):
    return schema_registry_client.get_latest_version(topic + "-value").schema.schema_str


def to_list(df: DataFrame):
    return [row.asDict() for row in df.collect()]


def decode_data(df: DataFrame, topic: str):
    return (
        df.withColumn("data", expr("substring(value, 6, length(value) - 5)"))
        .withColumn("decoded", from_avro("data", get_avro_schema(topic)))
        .select("decoded.*")
    )
